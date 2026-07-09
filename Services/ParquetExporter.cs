using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using DuckDBGeoparquet.Models;

namespace DuckDBGeoparquet.Services
{
    public class ParquetExporter
    {
        private readonly DuckDBManager _duckDb;
        private readonly LayerManager _layerManager;
        
        private DuckDBConnection _connection => _duckDb.Connection;
        private bool _isInitialized => _duckDb.IsInitialized;

        public ParquetExporter(DuckDBManager duckDb, LayerManager layerManager)
        {
            _duckDb = duckDb;
            _layerManager = layerManager;
        }

        /// <summary>
        /// Exports a small GeoJSON sample of an Overture S3 dataset for the
        /// preview map — a cheap dry-run of a load. Uses the same bbox
        /// pushdown filter as the real load but skips ST_Intersects and caps
        /// the row count, so it returns in seconds. Only id + geometry are
        /// selected: flat columns keep the GDAL GeoJSON writer happy and the
        /// output small.
        /// </summary>
        public async Task ExportPreviewSampleAsync(string s3Path, string outputGeoJsonPath, ExtentBounds extent = null, int maxFeatures = 2000, CancellationToken cancellationToken = default)
        {
            if (!_isInitialized)
                throw new InvalidOperationException("DuckDB is not initialized yet.");

            string spatialFilter = "";
            if (extent != null)
            {
                spatialFilter = $"WHERE {GeoParquetSql.BuildBboxOverlapPredicate(extent)}";
            }

            string normalizedOutput = outputGeoJsonPath.Replace('\\', '/');
            using var command = _connection.CreateCommand();
            command.CommandText = $@"
                COPY (
                    SELECT id, geometry
                    FROM read_parquet('{s3Path}', hive_partitioning=1)
                    {spatialFilter}
                    LIMIT {maxFeatures}
                ) TO '{normalizedOutput}' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
            ";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        /// <summary>
        /// Exports the current_table to GeoParquet file(s) using DuckDB's Parquet export.
        /// All exported files are placed in a single output folder per session.
        /// </summary>
        public async Task CreateFeatureLayersAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot, string outputSessionSuffix, string compression = "ZSTD", CancellationToken cancellationToken = default)
        {
            System.Diagnostics.Debug.WriteLine($"CreateFeatureLayersAsync called for: {layerNameBase}, ParentS3Theme: {parentS3Theme}, ActualS3Type: {actualS3Type}, DataOutputPathRoot: {dataOutputPathRoot}");
            progress?.Report($"Starting layer creation for {layerNameBase}");
            var exportStopwatch = Stopwatch.StartNew();

            if (string.IsNullOrEmpty(parentS3Theme) || string.IsNullOrEmpty(actualS3Type))
            {
                progress?.Report("Error: Parent theme or actual type not provided. Cannot create feature layer.");
                return;
            }
            if (string.IsNullOrEmpty(dataOutputPathRoot))
            {
                progress?.Report("Error: Data output path root not provided. Cannot create feature layer.");
                return;
            }

            string themeTypeSpecificFolder = Path.Combine(dataOutputPathRoot, actualS3Type);

            if (!Directory.Exists(dataOutputPathRoot))
            {
                Directory.CreateDirectory(dataOutputPathRoot); 
            }
            if (!Directory.Exists(themeTypeSpecificFolder))
            {
                Directory.CreateDirectory(themeTypeSpecificFolder); 
            }

            try
            {
                await ExportByGeometryType(layerNameBase, themeTypeSpecificFolder, parentS3Theme, actualS3Type, outputSessionSuffix, progress, compression, cancellationToken);
                exportStopwatch.Stop();
                System.Diagnostics.Debug.WriteLine(
                    $"[perf][export:{actualS3Type}] geometry-export output={DescribeOutputPathKind(dataOutputPathRoot)} elapsed={FormatElapsed(exportStopwatch.Elapsed)}");
                progress?.Report($"Feature layer creation process completed for {layerNameBase}.");
            }
            catch (Exception ex)
            {
                progress?.Report($"Error creating feature layer {layerNameBase}: {ex.Message}");
                throw;
            }
        }

        private async Task ExportByGeometryType(string layerNameBase, string themeTypeSpecificOutputFolder, string parentS3Theme, string actualS3Type, string outputSessionSuffix, IProgress<string> progress = null, string compression = "ZSTD", CancellationToken cancellationToken = default)
        {
            progress?.Report($"Processing geometry types for {layerNameBase} with optimal layer stacking");

            using var command = _connection.CreateCommand();
            var discoveryStopwatch = Stopwatch.StartNew();
            bool hasCachedGeometryType = await CurrentTableHasColumn(command, GeoParquetSql.InternalGeometryTypeColumn, cancellationToken);
            command.CommandText = hasCachedGeometryType
                ? $"SELECT DISTINCT {GeoParquetSql.InternalGeometryTypeColumn} as geom_type FROM current_table WHERE geometry IS NOT NULL AND {GeoParquetSql.InternalGeometryTypeColumn} IS NOT NULL"
                : "SELECT DISTINCT ST_GeometryType(geometry) as geom_type FROM current_table WHERE geometry IS NOT NULL";

            var geometryTypes = new List<string>();
            try
            {
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync())
                {
                    var geomType = reader.GetString(0);
                    if (!string.IsNullOrEmpty(geomType))
                    {
                        geometryTypes.Add(geomType);
                    }
                }
                discoveryStopwatch.Stop();
                System.Diagnostics.Debug.WriteLine(
                    $"[perf][export:{actualS3Type}] geometry-discovery cached={hasCachedGeometryType} types={geometryTypes.Count} elapsed={FormatElapsed(discoveryStopwatch.Elapsed)}");
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
                progress?.Report("No geometries found in the current dataset. Skipping layer creation.");
                return;
            }

            bool currentTableHasBbox = await CurrentTableHasColumn(command, "bbox", cancellationToken);

            int typeIndex = 0;
            foreach (var geomType in geometryTypes)
            {
                typeIndex++;
                string stackingNote = geometryTypeOrder.TryGetValue(geomType, out int order) ? $" (layer {order} of 3)" : "";
                progress?.Report($"Processing {layerNameBase}: {geomType}{stackingNote} ({typeIndex}/{geometryTypes.Count})");

                string descriptiveGeomType = GetDescriptiveGeometryType(geomType);
                string finalLayerName = geometryTypes.Count > 1 ? $"{layerNameBase} ({descriptiveGeomType})" : layerNameBase;
                string outputFileName = $"{MfcUtility.SanitizeFileName(finalLayerName)}_{outputSessionSuffix}.parquet";
                string finalOutputPath = Path.Combine(themeTypeSpecificOutputFolder, outputFileName);

                try
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(finalOutputPath));

                    string exportSelect = GeoParquetSql.BuildGeometryExportSelect(geomType, currentTableHasBbox, hasCachedGeometryType);

                    string targetPath = await ExportToGeoParquet(exportSelect, finalOutputPath, finalLayerName, progress, compression, cancellationToken);

                    var layerInfo = new LayerCreationInfo
                    {
                        FilePath = targetPath,
                        LayerName = finalLayerName,
                        GeometryType = geomType,
                        StackingPriority = geometryTypeOrder.TryGetValue(geomType, out int priority) ? priority : 99,
                        ParentTheme = parentS3Theme,
                        ActualType = actualS3Type
                    };
                    _layerManager.AddPendingLayer(layerInfo);

                    progress?.Report($"Prepared layer {finalLayerName} for optimal stacking");
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error exporting {geomType}: {ex.Message}");
                }
            }

            progress?.Report($"Finished processing all geometry types for {layerNameBase}.");
        }

        private async Task<string> ExportToGeoParquet(string query, string outputPath, string layerName, IProgress<string> progress = null, string compression = "ZSTD", CancellationToken cancellationToken = default)
        {
            progress?.Report($"Exporting data for {layerName} to {Path.GetFileName(outputPath)}");

            try
            {
                if (File.Exists(outputPath))
                {
                    await _layerManager.RemoveLayersUsingFileAsync(outputPath);
                    
                    await Task.Delay(50);
                    
                    for (int attempt = 1; attempt <= 2; attempt++)
                    {
                        try
                        {
                            File.Delete(outputPath);
                            break;
                        }
                        catch (IOException) when (attempt < 2)
                        {
                            await Task.Delay(50);
                        }
                        catch (IOException) when (attempt == 2)
                        {
                            // Ignore, will use temp file fallback
                        }
                    }
                }

                using var command = _connection.CreateCommand();
                
                string actualOutputPath = outputPath;
                bool useTempFile = File.Exists(outputPath);
                
                if (useTempFile)
                {
                    string directory = Path.GetDirectoryName(outputPath);
                    string fileNameWithoutExt = Path.GetFileNameWithoutExtension(outputPath);
                    string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                    actualOutputPath = Path.Combine(directory, $"{fileNameWithoutExt}_{timestamp}.parquet");
                }
                
                command.CommandText = GeoParquetSql.BuildCopyCommand(query, actualOutputPath, compression);
                var copyStopwatch = Stopwatch.StartNew();
                await command.ExecuteNonQueryAsync(cancellationToken);
                copyStopwatch.Stop();
                System.Diagnostics.Debug.WriteLine(
                    $"[perf][export] copy layer=\"{layerName}\" output={DescribeOutputPathKind(actualOutputPath)} compression={GeoParquetSql.ValidateCompression(compression)} elapsed={FormatElapsed(copyStopwatch.Elapsed)}");
                
                if (useTempFile && File.Exists(actualOutputPath))
                {
                    for (int attempt = 1; attempt <= 3; attempt++)
                    {
                        try
                        {
                            if (File.Exists(outputPath))
                            {
                                File.Delete(outputPath);
                            }
                            File.Move(actualOutputPath, outputPath);
                            break;
                        }
                        catch (IOException) when (attempt < 3)
                        {
                            await Task.Delay(attempt * 100);
                        }
                        catch (IOException) when (attempt == 3)
                        {
                            outputPath = actualOutputPath;
                        }
                    }
                }
                progress?.Report($"Successfully exported data for {layerName}.");
                return outputPath;
            }
            catch (Exception ex)
            {
                progress?.Report($"Error exporting to GeoParquet for {layerName}: {ex.Message}");
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

        private static async Task<bool> CurrentTableHasColumn(DuckDBCommand command, string columnName, CancellationToken cancellationToken)
        {
            command.CommandText = $"SELECT COUNT(*) FROM pragma_table_info('current_table') WHERE lower(name) = lower('{GeoParquetSql.EscapeSqlLiteral(columnName)}')";
            var count = await command.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt32(count) > 0;
        }

        private static string DescribeOutputPathKind(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return "unknown";

            string normalized = path.Replace('/', '\\');
            if (normalized.Contains("\\OneDrive", StringComparison.OrdinalIgnoreCase))
                return "onedrive";
            if (normalized.StartsWith(@"\\", StringComparison.OrdinalIgnoreCase))
                return "network";
            return Path.IsPathRooted(path) ? "local" : "relative";
        }

        private static string FormatElapsed(TimeSpan elapsed)
        {
            return elapsed.TotalSeconds >= 1
                ? $"{elapsed.TotalSeconds:F2}s"
                : $"{elapsed.TotalMilliseconds:F0}ms";
        }
    }
}
