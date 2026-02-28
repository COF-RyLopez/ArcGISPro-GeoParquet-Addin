using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace DuckDBGeoparquet.Services
{
    public class DuckDbDataService : IDataService
    {
        private readonly DuckDBConnection _connection;
        private readonly IFileHandler _fileHandler;
        private readonly List<LayerCreationInfo> _pendingLayers;
        
        // Constants
        private const string DEFAULT_SRS = "EPSG:4326";
        private const string STRUCT_TYPE = "STRUCT";
        private const string BBOX_COLUMN = "bbox";
        private const string GEOMETRY_COLUMN = "geometry";
        private static readonly string[] THEME_TYPE_SEPARATOR = [" - "];
        
        private ExtentInfo _currentExtent;
        private string _currentParentS3Theme;
        private string _currentActualS3Type;

        public bool EnableGeometryRepair { get; set; } = false;

        public DuckDbDataService(IFileHandler fileHandler)
        {
            _fileHandler = fileHandler;
            _connection = new DuckDBConnection("DataSource=:memory:");
            _pendingLayers = new List<LayerCreationInfo>();
        }

        public async Task InitializeAsync()
        {
            try
            {
                await _connection.OpenAsync();
                
                // Get the add-in folder path (hacky but standard for .NET apps)
                string addInFolder = AppDomain.CurrentDomain.BaseDirectory;
                string extensionsPath = Path.Combine(addInFolder, "Extensions");

                using var command = _connection.CreateCommand();

                try
                {
                    command.CommandText = @"
                        INSTALL spatial;
                        INSTALL httpfs;
                        LOAD spatial;
                        LOAD httpfs;
                    ";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                }
                catch (Exception)
                {
                    // Fallback logic for bundled extensions
                    if (Directory.Exists(extensionsPath))
                    {
                        var extensionFiles = Directory.GetFiles(extensionsPath, "*.duckdb_extension");
                        if (extensionFiles.Length > 0)
                        {
                            string normalizedPath = extensionsPath.Replace('\\', '/');
                            command.CommandText = $@"
                                SET extension_directory='{normalizedPath}';
                                LOAD spatial;
                                LOAD httpfs;
                            ";
                            await command.ExecuteNonQueryAsync(CancellationToken.None);
                        }
                        else
                        {
                            throw new Exception($"No extension files found in {extensionsPath}.");
                        }
                    }
                    else
                    {
                        throw new Exception($"Extensions directory not found at {extensionsPath}.");
                    }
                }

                // Configure DuckDB settings
                command.CommandText = @"
                    SET s3_region='us-west-2';
                    SET enable_http_metadata_cache=true;
                    SET enable_object_cache=true;
                    SET enable_progress_bar=true;
                ";
                await command.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to initialize DuckDB: {ex.Message}", ex);
            }
        }

        public async Task<bool> IngestFileAsync(string s3Path, ExtentInfo extent = null, string actualS3Type = null, IProgress<string> progress = null)
        {
            try
            {
                progress?.Report($"Connecting to S3: {actualS3Type ?? "data"}...");
                _currentExtent = extent;
                _currentActualS3Type = actualS3Type;

                progress?.Report($"Reading schema from S3...");

                using var command = _connection.CreateCommand();
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE temp AS 
                    SELECT * FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1) LIMIT 0;
                ";
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                progress?.Report($"Loading data from S3 (this may take a moment)...");

                string spatialFilter = "";
                if (extent != null)
                {
                    string xMinStr = extent.XMin.ToString("G", CultureInfo.InvariantCulture);
                    string yMinStr = extent.YMin.ToString("G", CultureInfo.InvariantCulture);
                    string xMaxStr = extent.XMax.ToString("G", CultureInfo.InvariantCulture);
                    string yMaxStr = extent.YMax.ToString("G", CultureInfo.InvariantCulture);
                    
                    spatialFilter = $@"
                        WHERE bbox.xmin >= {xMinStr}
                          AND bbox.ymin >= {yMinStr}
                          AND bbox.xmax <= {xMaxStr}
                          AND bbox.ymax <= {yMaxStr}";
                    
                    progress?.Report($"Applying spatial filter for current map extent...");
                }

                string query = $@"
                    CREATE OR REPLACE TABLE current_table AS 
                    SELECT 
                        *, 
                        {(EnableGeometryRepair ? "ST_MakeValid(geometry)" : "geometry")} AS geometry
                    FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                    {spatialFilter}
                ";
                command.CommandText = query;
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                command.CommandText = "SELECT COUNT(*) FROM current_table";
                var count = await command.ExecuteScalarAsync(CancellationToken.None);
                progress?.Report($"Successfully loaded {count:N0} rows from S3");

                if (Convert.ToInt64(count) == 0)
                {
                    progress?.Report("Dataset is empty - no features to process");
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                // In production code, we might want to log this properly
                Console.WriteLine($"Query error: {ex.Message}");
                return false;
            }
        }

        public async Task<DataTable> GetPreviewDataAsync()
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT * FROM current_table LIMIT 1000";
                using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                var previewTable = new DataTable();
                previewTable.Load(reader);
                return previewTable;
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to get preview data: {ex.Message}", ex);
            }
        }

        public async Task CreateFeatureLayerAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot)
        {
            progress?.Report($"Starting layer creation for {layerNameBase}");

            if (string.IsNullOrEmpty(parentS3Theme) || string.IsNullOrEmpty(actualS3Type) || string.IsNullOrEmpty(dataOutputPathRoot))
            {
                progress?.Report("Error: Missing required parameters.");
                return;
            }

            _currentParentS3Theme = parentS3Theme;
            _currentActualS3Type = actualS3Type;

            string themeTypeSpecificFolder = Path.Combine(dataOutputPathRoot, actualS3Type);

            if (!Directory.Exists(dataOutputPathRoot)) Directory.CreateDirectory(dataOutputPathRoot);
            if (!Directory.Exists(themeTypeSpecificFolder)) Directory.CreateDirectory(themeTypeSpecificFolder);

            try
            {
                await ExportByGeometryType(layerNameBase, themeTypeSpecificFolder, progress);
                progress?.Report($"Feature layer creation process completed for {layerNameBase}.");
            }
            catch (Exception ex)
            {
                progress?.Report($"Error creating feature layer {layerNameBase}: {ex.Message}");
                throw;
            }
            finally
            {
                _currentParentS3Theme = null;
                _currentActualS3Type = null;
            }
        }

        private async Task ExportByGeometryType(string layerNameBase, string themeTypeSpecificOutputFolder, IProgress<string> progress = null)
        {
            progress?.Report($"Processing geometry types for {layerNameBase} with optimal layer stacking");

            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT DISTINCT ST_GeometryType(geometry) as geom_type FROM current_table WHERE geometry IS NOT NULL";

            var geometryTypes = new List<string>();
            try
            {
                using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                while (await reader.ReadAsync())
                {
                    var geomType = reader.GetString(0);
                    if (!string.IsNullOrEmpty(geomType)) geometryTypes.Add(geomType);
                }
            }
            catch (Exception ex)
            {
                progress?.Report($"Error reading geometry types: {ex.Message}");
                throw;
            }

            var geometryTypeOrder = new Dictionary<string, int>
            {
                { "POLYGON", 1 }, { "MULTIPOLYGON", 1 },
                { "LINESTRING", 2 }, { "MULTILINESTRING", 2 },
                { "POINT", 3 }, { "MULTIPOINT", 3 }
            };

            geometryTypes = geometryTypes
                .OrderBy(geomType => geometryTypeOrder.TryGetValue(geomType, out int order) ? order : 99)
                .ToList();

            if (geometryTypes.Count == 0)
            {
                progress?.Report("No geometries found in the current dataset.");
                return;
            }

            int typeIndex = 0;
            foreach (var geomType in geometryTypes)
            {
                typeIndex++;
                string descriptiveGeomType = GetDescriptiveGeometryType(geomType);
                string finalLayerName = geometryTypes.Count > 1 ? $"{layerNameBase} ({descriptiveGeomType})" : layerNameBase;
                string outputFileName = $"{SanitizeFileName(finalLayerName)}.parquet";
                string outputPath = Path.Combine(themeTypeSpecificOutputFolder, outputFileName);

                string query = $"SELECT * EXCLUDE geometry, geometry FROM current_table WHERE ST_GeometryType(geometry) = '{geomType}'";

                try
                {
                    (string actualFilePath, double exportedArea) = await ExportToGeoParquet(query, outputPath, finalLayerName, geomType, progress);

                    if (File.Exists(actualFilePath))
                    {
                        var layerInfo = new LayerCreationInfo
                        {
                            FilePath = actualFilePath,
                            LayerName = finalLayerName,
                            GeometryType = geomType,
                            StackingPriority = geometryTypeOrder.TryGetValue(geomType, out int priority) ? priority : 99,
                            ParentTheme = _currentParentS3Theme,
                            ActualType = _currentActualS3Type,
                            Area = exportedArea
                        };
                        _pendingLayers.Add(layerInfo);
                        progress?.Report($"Prepared layer {finalLayerName}");
                    }
                    else
                    {
                        progress?.Report($"Error: Export completed but file not found for {finalLayerName}");
                    }
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error exporting layer for {geomType}: {ex.Message}");
                }
            }
        }

        private async Task<(string FilePath, double Area)> ExportToGeoParquet(string query, string outputPath, string _layerName, string geomType, IProgress<string> progress = null)
        {
            progress?.Report($"Exporting data for {_layerName} to {Path.GetFileName(outputPath)}");

            double avgArea = 0.0;
            if (geomType.Contains("POLYGON", StringComparison.OrdinalIgnoreCase) || geomType.Contains("LINESTRING", StringComparison.OrdinalIgnoreCase))
            {
                using var areaCommand = _connection.CreateCommand();
                string areaQuery = $"SELECT AVG(ST_Area(geometry)) FROM current_table WHERE ST_GeometryType(geometry) = '{geomType}'";
                areaCommand.CommandText = areaQuery;
                var areaResult = await areaCommand.ExecuteScalarAsync(CancellationToken.None);
                if (areaResult != DBNull.Value && areaResult != null)
                {
                    avgArea = Convert.ToDouble(areaResult);
                }
            }

            try
            {
                // Delegate deletion to the injected file handler
                if (File.Exists(outputPath))
                {
                    await _fileHandler.DeleteFileAsync(outputPath);
                    await Task.Delay(250);
                }

                using var command = _connection.CreateCommand();
                string actualOutputPath = outputPath;
                bool useTempFile = File.Exists(outputPath);
                
                if (useTempFile)
                {
                    actualOutputPath = outputPath + $".tmp_{DateTime.Now:yyyyMMdd_HHmmss}";
                }
                
                command.CommandText = $@"
                    COPY ({query}) TO '{actualOutputPath.Replace('\\', '/')}' 
                    WITH (FORMAT 'PARQUET', ROW_GROUP_SIZE 100000, COMPRESSION 'ZSTD');";
                
                await command.ExecuteNonQueryAsync(CancellationToken.None);
                
                if (useTempFile && File.Exists(actualOutputPath))
                {
                    await _fileHandler.DeleteFileAsync(outputPath);
                    if (!File.Exists(outputPath))
                    {
                        File.Move(actualOutputPath, outputPath);
                    }
                    else
                    {
                        outputPath = actualOutputPath; // Fallback to temp file
                    }
                }
                
                return (outputPath, avgArea);
            }
            catch (Exception ex)
            {
                progress?.Report($"Error exporting to GeoParquet for {_layerName}: {ex.Message}");
                throw;
            }
        }

        private static string GetDescriptiveGeometryType(string geomType) => geomType switch
        {
            "POINT" => "points",
            "LINESTRING" => "lines",
            "POLYGON" => "polygons",
            "MULTIPOINT" => "multipoints",
            "MULTILINESTRING" => "multilines",
            "MULTIPOLYGON" => "multipolygons",
            _ => geomType.ToLowerInvariant()
        };

        private static string SanitizeFileName(string name)
        {
            foreach (char c in Path.GetInvalidFileNameChars())
            {
                name = name.Replace(c, '_');
            }
            return name;
        }

        public List<LayerCreationInfo> GetPendingLayers() => _pendingLayers;
        
        public void ClearPendingLayers() => _pendingLayers.Clear();

        public void Dispose()
        {
            _connection?.Dispose();
            _pendingLayers?.Clear();
            GC.SuppressFinalize(this);
        }
    }
}
