using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using ArcGIS.Core.CIM;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDB.NET.Data;

namespace DuckDBGeoparquet.Services
{
    public class DataProcessor : IDisposable
    {
        private readonly DuckDBConnection _connection;

        // Constants
        private const string DEFAULT_SRS = "EPSG:4326";
        private const string STRUCT_TYPE = "STRUCT";
        private const string BBOX_COLUMN = "bbox";
        private const string GEOMETRY_COLUMN = "geometry";

        // Add static readonly field for the theme-type separator
        private static readonly string[] THEME_TYPE_SEPARATOR = { " - " };

        // Property to hold the output folder for the session.
        private string _outputFolder;

        public DataProcessor()
        {
            _connection = new DuckDBConnection("DataSource=:memory:");
        }

        /// <summary>
        /// Gets (or creates) a single output folder per session.
        /// The folder will be integrated with the MFC structure.
        /// </summary>
        /// <param name="releaseVersion">The release version string.</param>
        /// <returns>The folder path.</returns>
        private string GetOutputFolder(string releaseVersion)
        {
            if (string.IsNullOrEmpty(_outputFolder))
            {
                // Use the MFC folder structure for consistency
                string mfcBasePath = MfcUtility.DefaultMfcBasePath;
                string dataFolder = Path.Combine(mfcBasePath, "Data");
                _outputFolder = Path.Combine(dataFolder, releaseVersion);

                System.Diagnostics.Debug.WriteLine($"Creating output folder structure at {_outputFolder}");

                if (!Directory.Exists(dataFolder))
                {
                    Directory.CreateDirectory(dataFolder);
                    System.Diagnostics.Debug.WriteLine($"Created Data folder: {dataFolder}");
                }

                if (!Directory.Exists(_outputFolder))
                {
                    Directory.CreateDirectory(_outputFolder);
                    System.Diagnostics.Debug.WriteLine($"Created release folder: {_outputFolder}");
                }

                // For MFC compatibility in ArcGIS Pro 3.5, we also need to create the Connection folder
                string connectionFolder = Path.Combine(mfcBasePath, "Connection");
                if (!Directory.Exists(connectionFolder))
                {
                    Directory.CreateDirectory(connectionFolder);
                    System.Diagnostics.Debug.WriteLine($"Created Connection folder: {connectionFolder}");
                }
            }
            return _outputFolder;
        }

        public async Task InitializeDuckDBAsync()
        {
            try
            {
                await _connection.OpenAsync();

                // Get the various potential paths where extensions might be found
                string addInFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                string extensionsPath = Path.Combine(addInFolder, "Extensions");

                using var command = _connection.CreateCommand();

                try
                {
                    // First try using direct INSTALL command - might work if user has admin rights
                    command.CommandText = @"
                        INSTALL spatial;
                        INSTALL httpfs;
                        LOAD spatial;
                        LOAD httpfs;
                    ";

                    try
                    {
                        await command.ExecuteNonQueryAsync(CancellationToken.None);
                        System.Diagnostics.Debug.WriteLine("Successfully installed extensions directly");
                        // If we get here, extensions were successfully installed
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"Direct installation failed: {ex.Message}. Trying bundled extensions...");

                        // Try loading from our bundled extensions path
                        if (Directory.Exists(extensionsPath))
                        {
                            var extensionFiles = Directory.GetFiles(extensionsPath, "*.duckdb_extension");
                            System.Diagnostics.Debug.WriteLine($"Found {extensionFiles.Length} extension files in {extensionsPath}");

                            if (extensionFiles.Length > 0)
                            {
                                // Make sure paths use forward slashes for DuckDB
                                string normalizedPath = extensionsPath.Replace('\\', '/');
                                command.CommandText = $@"
                                    SET extension_directory='{normalizedPath}';
                                    LOAD spatial;
                                    LOAD httpfs;
                                ";
                                await command.ExecuteNonQueryAsync(CancellationToken.None);
                                System.Diagnostics.Debug.WriteLine("Successfully loaded extensions from bundled directory");
                            }
                            else
                            {
                                throw new Exception($"No extension files found in {extensionsPath}. Please follow the instructions in Extensions/README.txt to obtain the required DuckDB extensions.");
                            }
                        }
                        else
                        {
                            throw new Exception($"Extensions directory not found at {extensionsPath}. Please create this directory and add the required DuckDB extensions.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    // If both methods failed, show a more informative error with detailed instructions
                    throw new Exception(
                        $"Failed to load DuckDB extensions: {ex.Message}\n\n" +
                        "To resolve this issue:\n" +
                        "1. Make sure the add-in's Extensions folder contains spatial.duckdb_extension and httpfs.duckdb_extension\n" +
                        "2. The extensions must match your DuckDB version (1.2.0)\n" +
                        "3. Download extensions from https://github.com/duckdb/duckdb/releases/tag/v1.2.0\n" +
                        $"4. Current extension search path: {extensionsPath}", ex);
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

        public async Task<bool> IngestFileAsync(string s3Path, dynamic extent = null)
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE temp AS 
                    SELECT * FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1) LIMIT 0;
                ";
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                command.CommandText = "DESCRIBE temp";
                using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                System.Diagnostics.Debug.WriteLine("Schema from Parquet:");
                while (await reader.ReadAsync())
                {
                    System.Diagnostics.Debug.WriteLine($"Column: {reader["column_name"]}, Type: {reader["column_type"]}");
                }

                string spatialFilter = "";
                if (extent != null)
                {
                    spatialFilter = $@"
                        WHERE bbox.xmin >= {extent.XMin}
                          AND bbox.ymin >= {extent.YMin}
                          AND bbox.xmax <= {extent.XMax}
                          AND bbox.ymax <= {extent.YMax}";
                }

                string query = $@"
                    CREATE OR REPLACE TABLE current_table AS 
                    SELECT *
                    FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                    {spatialFilter}
                ";
                command.CommandText = query;
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                command.CommandText = "SELECT COUNT(*) FROM current_table";
                var count = await command.ExecuteScalarAsync(CancellationToken.None);
                System.Diagnostics.Debug.WriteLine($"Loaded {count} rows");

                command.CommandText = "DESCRIBE current_table";
                using var reader2 = await command.ExecuteReaderAsync(CancellationToken.None);
                System.Diagnostics.Debug.WriteLine("Final table structure:");
                while (await reader2.ReadAsync())
                {
                    System.Diagnostics.Debug.WriteLine($"Column: {reader2["column_name"]}, Type: {reader2["column_type"]}");
                }

                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Query error: {ex.Message}");
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

        private static void DeleteParquetFile(string parquetPath)
        {
            if (File.Exists(parquetPath))
            {
                try
                {
                    File.Delete(parquetPath);
                }
                catch (Exception ex)
                {
                    // Handle any deletion errors (e.g., file is locked).
                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show($"Failed to delete {parquetPath}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Exports the current_table to GeoParquet file(s) using DuckDB's Parquet export.
        /// All exported files are placed in a single output folder per session.
        /// </summary>
        /// <param name="layerNameBase">Base name for the layer.</param>
        /// <param name="releaseVersion">The release version (used to name the output folder).</param>
        /// <param name="progress">Progress reporter.</param>
        public async Task CreateFeatureLayerAsync(string layerNameBase, string releaseVersion, IProgress<string> progress = null)
        {
            string outputFolder = GetOutputFolder(releaseVersion);
            try
            {
                await QueuedTask.Run(async () =>
                {
                    progress?.Report("Initializing...");
                    var map = MapView.Active?.Map
                        ?? throw new Exception("No active map found");

                    // With ArcGIS Pro 3.5's improved GeoParquet support, we can now use the original
                    // complex structure without extensive flattening
                    progress?.Report("Building query...");

                    // Export by geometry type for all datasets
                    await ExportByGeometryType(layerNameBase, releaseVersion, outputFolder, progress);
                    progress?.Report($"All {layerNameBase} features added to map.");
                });
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Detailed error: {ex}");
                throw new Exception($"Failed to create feature layer: {ex.Message}", ex);
            }
        }

        private async Task ExportByGeometryType(string layerNameBase, string releaseVersion, string outputFolder, IProgress<string> progress = null)
        {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT DISTINCT ST_GeometryType(geometry) FROM current_table";
            var geomTypes = new List<string>();

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                geomTypes.Add(reader.GetString(0));
            }

            // Parse the layer name to extract theme and type
            // Format is typically "theme - type" (e.g., "base - infrastructure" or "buildings - building_part")
            string[] parts = layerNameBase.Split(THEME_TYPE_SEPARATOR, StringSplitOptions.None);
            string theme = parts[0].Trim().ToLowerInvariant();
            string type = parts.Length > 1 ? parts[1].Trim().ToLowerInvariant() : theme;

            // Create a dataset folder - this needs to be a direct child of the output folder
            // Following MFC requirements per documentation
            string datasetFolder = Path.Combine(outputFolder, $"{theme}_{type}");
            if (!Directory.Exists(datasetFolder))
            {
                System.Diagnostics.Debug.WriteLine($"Creating dataset folder: {datasetFolder}");
                Directory.CreateDirectory(datasetFolder);
            }

            foreach (var geomType in geomTypes)
            {
                progress?.Report($"Processing {geomType} features for {theme}/{type}...");

                // Build query that preserves all original columns including complex types
                string filteredQuery = $@"
                    SELECT 
                        *,
                        ST_Transform({GEOMETRY_COLUMN}, '{DEFAULT_SRS}', '{DEFAULT_SRS}') AS {GEOMETRY_COLUMN}
                    FROM current_table
                    WHERE ST_GeometryType(geometry) = '{geomType}'";

                // Get the descriptive geometry type (e.g., "points", "lines", "polygons")
                string geometryDescription = GetDescriptiveGeometryType(geomType);

                // Create a filename that includes theme, type and geometry for uniqueness
                string fileName = $"{theme}_{type}_{geometryDescription}.parquet";

                // Place all files in the same dataset folder to maintain schema compatibility
                string parquetPath = Path.Combine(datasetFolder, fileName);

                progress?.Report($"Exporting {fileName}...");
                await ExportToGeoParquet(filteredQuery, parquetPath, fileName);

                if (File.Exists(parquetPath))
                {
                    var fileInfo = new FileInfo(parquetPath);
                    if (fileInfo.Length > 0)
                    {
                        progress?.Report($"Adding {fileName} to map...");
                        await QueuedTask.Run(() =>
                        {
                            var map = MapView.Active?.Map;
                            if (map != null)
                            {
                                // Create the layer and get a reference to it
                                var newLayer = LayerFactory.Instance.CreateLayer(new Uri(parquetPath), map);

                                // Check if this is an address layer and disable binning if needed
                                if (newLayer is FeatureLayer featureLayer && type.Contains("address"))
                                {
                                    try
                                    {
                                        var layerDef = featureLayer.GetDefinition() as CIMFeatureLayer;
                                        if (layerDef?.FeatureReduction != null)
                                        {
                                            layerDef.FeatureReduction.Enabled = false;
                                            featureLayer.SetDefinition(layerDef);
                                            System.Diagnostics.Debug.WriteLine($"Disabled binning for address layer: {featureLayer.Name}");
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        // Just log the error but don't fail the process
                                        System.Diagnostics.Debug.WriteLine($"Failed to disable binning: {ex.Message}");
                                    }
                                }
                            }
                        });
                    }
                }
            }
        }

        private async Task ExportToGeoParquet(string query, string outputPath, string _layerName)
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = BuildGeoParquetCopyCommand(query, outputPath);
                System.Diagnostics.Debug.WriteLine($"Export command for {_layerName}: {command.CommandText}");
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to export GeoParquet for layer {_layerName}", ex);
            }
        }

        private static string BuildGeoParquetCopyCommand(string selectQuery, string outputPath)
        {
            return $@"
                COPY (
                    {selectQuery}
                ) TO '{outputPath.Replace('\\', '/')}' 
                WITH (
                    FORMAT 'PARQUET',
                    ROW_GROUP_SIZE 100000,
                    COMPRESSION 'ZSTD'
                );";
        }

        // Helper method to get a more descriptive geometry type name
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

        public void Dispose()
        {
            _connection?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
