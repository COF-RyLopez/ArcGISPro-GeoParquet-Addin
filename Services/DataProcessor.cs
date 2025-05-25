using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
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

        // Add a class-level field to store the current extent
        private dynamic _currentExtent;

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
                // Store the extent for later use in CreateFeatureLayerAsync
                _currentExtent = extent;

                // The following call to MakeEnvironmentArray was not directly used by a GP tool in this method.
                // Overwrite for deletion is handled in DeleteParquetFileAsync if needed elsewhere.
                // DuckDB handles its own table replacement/creation.
                /* 
                await QueuedTask.Run(() => {
                    try {
                        Geoprocessing.MakeEnvironmentArray(overwriteoutput: true);
                        System.Diagnostics.Debug.WriteLine("Set geoprocessing environment to allow overwriting");
                    }
                    catch (Exception ex) {
                        System.Diagnostics.Debug.WriteLine($"Warning: Could not set geoprocessing environment: {ex.Message}");
                    }
                });
                */

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

        private static async Task RemoveLayersUsingFileAsync(string filePath)
        {
            // Validate the file path
            if (string.IsNullOrEmpty(filePath))
            {
                System.Diagnostics.Debug.WriteLine("RemoveLayersUsingFileAsync: File path is null or empty.");
                return;
            }

            System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Attempting to remove layers associated with file: {filePath}");

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map == null)
                {
                    System.Diagnostics.Debug.WriteLine("RemoveLayersUsingFileAsync: No active map found.");
                    return;
                }

                var membersToRemove = new List<MapMember>();
                var allLayers = map.GetLayersAsFlattenedList().ToList();
                var allTables = map.GetStandaloneTablesAsFlattenedList().ToList();

                System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Found {allLayers.Count} layers and {allTables.Count} standalone tables in the map.");

                string normalizedTargetPath = Path.GetFullPath(filePath).ToLowerInvariant();
                System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Normalized target path: {normalizedTargetPath}");

                // Helper to get the actual file path from a potential dataset path
                Func<string, string> getActualFilePath = (string dataSourcePath) =>
                {
                    if (string.IsNullOrEmpty(dataSourcePath)) return null;
                    string lowerPath = dataSourcePath.ToLowerInvariant();
                    // If path is like "...file.parquet\dataset_name"
                    if (lowerPath.Contains(".parquet\\") || lowerPath.Contains(".parquet/"))
                    {
                        int parquetIndex = lowerPath.IndexOf(".parquet");
                        if (parquetIndex > -1)
                        {
                            return Path.GetFullPath(dataSourcePath.Substring(0, parquetIndex + ".parquet".Length)).ToLowerInvariant();
                        }
                    }
                    return Path.GetFullPath(dataSourcePath).ToLowerInvariant(); // Default to the full path
                };

                // Check layers
                foreach (var layer in allLayers)
                {
                    if (layer is FeatureLayer featureLayer)
                    {
                        bool removedByPath = false;
                        try
                        {
                            using var fc = featureLayer.GetFeatureClass();
                            if (fc != null)
                            {
                                var fcPathUri = fc.GetPath();
                                if (fcPathUri != null && fcPathUri.IsFile)
                                {
                                    string rawLayerDataSourcePath = fcPathUri.LocalPath;
                                    string actualLayerFilePath = getActualFilePath(rawLayerDataSourcePath);
                                    System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Layer '{featureLayer.Name}' path from FeatureClass.GetPath(): {rawLayerDataSourcePath}, Resolved to: {actualLayerFilePath}");
                                    if (actualLayerFilePath == normalizedTargetPath)
                                    {
                                        membersToRemove.Add(featureLayer);
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Added layer '{featureLayer.Name}' for removal based on FeatureClass.GetPath().");
                                        removedByPath = true;
                                    }
                                }

                                if (!removedByPath)
                                {
                                    using var datastore = fc.GetDatastore();
                                    var connector = datastore.GetConnector();
                                    string dsPathViaConnectorRaw = string.Empty;
                                    if (connector is FileSystemConnectionPath fileSystemConnectionPath)
                                    {
                                        dsPathViaConnectorRaw = fileSystemConnectionPath.Path?.LocalPath;
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Layer '{featureLayer.Name}' (Connector Fallback) FileSystemConnectionPath: {dsPathViaConnectorRaw}");
                                    }
                                    else if (connector is ArcGIS.Core.Data.PluginDatastore.PluginDatasourceConnectionPath)
                                    {
                                        var datastoreUri = datastore.GetPath();
                                        dsPathViaConnectorRaw = datastoreUri?.LocalPath;
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Layer '{featureLayer.Name}' (Connector Fallback) PluginDatasourceConnectionPath from Datastore: {dsPathViaConnectorRaw}");
                                    }
                                    else
                                    {
                                        if (connector != null) { System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Layer '{featureLayer.Name}' (Connector Fallback) type: {connector.GetType().Name}"); }
                                        else { System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Layer '{featureLayer.Name}' (Connector Fallback) connector is null."); }
                                        var connString = datastore.GetConnectionString();
                                        if (!string.IsNullOrEmpty(connString) && Uri.TryCreate(connString, UriKind.Absolute, out Uri uriResult) && uriResult.IsFile) { dsPathViaConnectorRaw = uriResult.LocalPath; }
                                        else if (!string.IsNullOrEmpty(connString)) { try { dsPathViaConnectorRaw = Path.GetFullPath(connString); } catch { } }
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Layer '{featureLayer.Name}' (Connector Fallback) path via ConnStr: {dsPathViaConnectorRaw}");
                                    }
                                    string actualDsPathViaConnector = getActualFilePath(dsPathViaConnectorRaw);
                                    if (!string.IsNullOrEmpty(actualDsPathViaConnector) && actualDsPathViaConnector == normalizedTargetPath)
                                    {
                                        membersToRemove.Add(featureLayer);
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Added layer '{featureLayer.Name}' for removal based on Connector Fallback (Resolved Path: {actualDsPathViaConnector}).");
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Error processing layer '{featureLayer.Name}': {ex.Message}");
                        }
                    }
                }

                // Check standalone tables
                foreach (var tableMember in allTables)
                {
                    if (tableMember is StandaloneTable standaloneTable)
                    {
                        bool removedByPath = false;
                        try
                        {
                            using var tbl = standaloneTable.GetTable();
                            if (tbl != null)
                            {
                                var tblPathUri = tbl.GetPath();
                                if (tblPathUri != null && tblPathUri.IsFile)
                                {
                                    string rawTableDataSourcePath = tblPathUri.LocalPath;
                                    string actualTableFilePath = getActualFilePath(rawTableDataSourcePath);
                                    System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Table '{standaloneTable.Name}' path from Table.GetPath(): {rawTableDataSourcePath}, Resolved to: {actualTableFilePath}");
                                    if (actualTableFilePath == normalizedTargetPath)
                                    {
                                        membersToRemove.Add(standaloneTable);
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Added table '{standaloneTable.Name}' for removal based on Table.GetPath().");
                                        removedByPath = true;
                                    }
                                }

                                if (!removedByPath)
                                {
                                    using var datastore = tbl.GetDatastore();
                                    var connector = datastore.GetConnector();
                                    string dsPathViaConnectorRaw = string.Empty;
                                    if (connector is FileSystemConnectionPath fileSystemConnectionPath)
                                    {
                                        dsPathViaConnectorRaw = fileSystemConnectionPath.Path?.LocalPath;
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Table '{standaloneTable.Name}' (Connector Fallback) FileSystemConnectionPath: {dsPathViaConnectorRaw}");
                                    }
                                    else if (connector is ArcGIS.Core.Data.PluginDatastore.PluginDatasourceConnectionPath)
                                    {
                                        var datastoreUri = datastore.GetPath();
                                        dsPathViaConnectorRaw = datastoreUri?.LocalPath;
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Table '{standaloneTable.Name}' (Connector Fallback) PluginDatasourceConnectionPath from Datastore: {dsPathViaConnectorRaw}");
                                    }
                                    else
                                    {
                                        if (connector != null) { System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Table '{standaloneTable.Name}' (Connector Fallback) type: {connector.GetType().Name}."); }
                                        else { System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Table '{standaloneTable.Name}' (Connector Fallback) connector is null."); }
                                        var connString = datastore.GetConnectionString();
                                        if (!string.IsNullOrEmpty(connString) && Uri.TryCreate(connString, UriKind.Absolute, out Uri uriResult) && uriResult.IsFile) { dsPathViaConnectorRaw = uriResult.LocalPath; }
                                        else if (!string.IsNullOrEmpty(connString)) { try { dsPathViaConnectorRaw = Path.GetFullPath(connString); } catch { } }
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Table '{standaloneTable.Name}' (Connector Fallback) path via ConnStr: {dsPathViaConnectorRaw}");
                                    }
                                    string actualDsPathViaConnector = getActualFilePath(dsPathViaConnectorRaw);
                                    if (!string.IsNullOrEmpty(actualDsPathViaConnector) && actualDsPathViaConnector == normalizedTargetPath)
                                    {
                                        membersToRemove.Add(standaloneTable);
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Added table '{standaloneTable.Name}' for removal based on Connector Fallback (Resolved Path: {actualDsPathViaConnector}).");
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Error processing table '{standaloneTable.Name}': {ex.Message}");
                        }
                    }
                }

                if (membersToRemove.Count > 0)
                {
                    var distinctMembersToRemove = membersToRemove.Distinct().ToList();
                    System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Removing {distinctMembersToRemove.Count} distinct layers/tables associated with file: {filePath}");
                    foreach (var member in distinctMembersToRemove)
                    {
                        if (member is Layer layerToRemove)
                        {
                            map.RemoveLayer(layerToRemove);
                            System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Removed layer '{member.Name}'.");
                            (layerToRemove as IDisposable)?.Dispose();
                        }
                        else if (member is StandaloneTable tableToRemove)
                        {
                            map.RemoveStandaloneTable(tableToRemove);
                            System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Removed table '{member.Name}'.");
                            (tableToRemove as IDisposable)?.Dispose();
                        }
                    }
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: No layers or tables found using the file: {filePath}");
                }
            });
        }

        private static async Task DeleteParquetFileAsync(string parquetPath)
        {
            if (!File.Exists(parquetPath))
            {
                System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: File does not exist, no action needed: {parquetPath}");
                return;
            }

            try
            {
                System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Starting deletion process for: {parquetPath}");
                await RemoveLayersUsingFileAsync(parquetPath);

                System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Layers removed. Waiting for file locks to release...");
                await Task.Delay(2500); // Increased delay to 2.5 seconds

                System.Diagnostics.Debug.WriteLine("DeleteParquetFileAsync: Invoking GC.Collect and WaitForPendingFinalizers.");
                GC.Collect();
                GC.WaitForPendingFinalizers();
                await Task.Delay(500); // Short additional delay after GC

                bool deleted = false;
                int gpRetryCount = 2; // Try GP delete a couple of times

                for (int i = 0; i < gpRetryCount && !deleted; i++)
                {
                    if (i > 0)
                    {
                        System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Retrying GP delete, attempt {i + 1} after delay...");
                        await Task.Delay(1500 * i); // Increasing delay for GP retries
                    }
                    System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Attempting GP delete (attempt {i + 1}/{gpRetryCount}): {parquetPath}");

                    await QueuedTask.Run(async () =>
                    {
                        try
                        {
                            var env = Geoprocessing.MakeEnvironmentArray(overwriteoutput: true);
                            var parameters = Geoprocessing.MakeValueArray(parquetPath);
                            var result = await Geoprocessing.ExecuteToolAsync(
                                "management.Delete",
                                parameters,
                                env,
                                flags: ArcGIS.Desktop.Core.Geoprocessing.GPExecuteToolFlags.Default);

                            if (!result.IsFailed)
                            {
                                System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Successfully deleted via geoprocessing: {parquetPath}");
                                deleted = true;
                            }
                            else
                            {
                                System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: GP delete failed (attempt {i + 1}). Messages:");
                                foreach (var msg in result.Messages)
                                {
                                    System.Diagnostics.Debug.WriteLine($"  GP Error: {msg.Text}");
                                }
                            }
                        }
                        catch (Exception gpEx)
                        {
                            System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Error in geoprocessing deletion (attempt {i + 1}): {gpEx.Message}");
                        }
                    });
                }

                if (!deleted && File.Exists(parquetPath))
                {
                    System.Diagnostics.Debug.WriteLine("DeleteParquetFileAsync: GP deletion failed or file still exists. Trying direct file operations as fallback...");
                    int fileDeleteRetryCount = 3;
                    Exception lastFileDeleteException = null;

                    for (int i = 0; i < fileDeleteRetryCount && !deleted; i++)
                    {
                        try
                        {
                            File.Delete(parquetPath);
                            deleted = true;
                            System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Successfully deleted via direct file operation: {parquetPath}");
                        }
                        catch (IOException ex)
                        {
                            lastFileDeleteException = ex;
                            if (i < fileDeleteRetryCount - 1)
                            {
                                int delay = (i + 1) * 1000;
                                System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Direct delete attempt {i + 1} failed (file locked), waiting {delay}ms to retry: {parquetPath}");
                                await Task.Delay(delay);
                            }
                        }
                    }

                    if (!deleted && lastFileDeleteException != null)
                    {
                        System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: All direct file delete attempts failed. Last error: {lastFileDeleteException.Message}");
                        // No longer throwing here, will fall through to the final message box if still not deleted.
                    }
                }

                if (!deleted && File.Exists(parquetPath))
                {
                    string errorMsg = $"Failed to delete {parquetPath}: File is still locked after multiple attempts.\n\nTry closing any ArcGIS Pro projects that might be using this data before proceeding, or manually delete the file.";
                    System.Diagnostics.Debug.WriteLine(errorMsg);
                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(errorMsg, "File Deletion Error",
                        System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Warning);
                }
                else if (!File.Exists(parquetPath))
                {
                    System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Confirmed file successfully deleted: {parquetPath}");
                }
            }
            catch (Exception ex) // Catch unexpected errors in the overall deletion process
            {
                string errorMsg = $"An unexpected error occurred during the deletion of {parquetPath}: {ex.Message}";
                System.Diagnostics.Debug.WriteLine(errorMsg);
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(errorMsg, "File Deletion Error",
                    System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
            }
        }

        // Keep the non-async version for backward compatibility but make it call the async version
        private static void DeleteParquetFile(string parquetPath)
        {
            // Call the async version and wait for it to complete
            Task.Run(async () => await DeleteParquetFileAsync(parquetPath)).Wait();
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

                    // The following call to MakeEnvironmentArray was not directly used by a GP tool in this method's immediate scope.
                    // Overwrite for file deletion is handled correctly within DeleteParquetFileAsync, which is called by ExportByGeometryType.
                    // DuckDB's COPY TO handles its own file overwriting during export.
                    /*
                    try
                    {
                        Geoprocessing.MakeEnvironmentArray(overwriteoutput: true);
                        System.Diagnostics.Debug.WriteLine("Set geoprocessing environment to allow overwriting");
                        progress?.Report("Environment configured to allow file overwriting");
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"Warning: Could not set geoprocessing environment: {ex.Message}");
                    }
                    */

                    // Check if we have an extent filter (for info purposes)
                    if (_currentExtent != null)
                    {
                        progress?.Report($"Creating data filtered to current map extent");
                    }
                    else
                    {
                        progress?.Report("Creating data (no extent filter applied)");
                    }

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

            string[] parts = layerNameBase.Split(THEME_TYPE_SEPARATOR, StringSplitOptions.None);
            string theme = parts[0].Trim().ToLowerInvariant();
            string type = parts.Length > 1 ? parts[1].Trim().ToLowerInvariant() : theme;

            string datasetFolder = Path.Combine(outputFolder, $"{theme}_{type}");

            // Proactive cleanup of layers from the dataset folder
            if (Directory.Exists(datasetFolder))
            {
                System.Diagnostics.Debug.WriteLine($"ExportByGeometryType: Proactively cleaning layers from folder: {datasetFolder}");
                progress?.Report($"Cleaning existing layers for {theme}/{type}...");
                var existingParquetFiles = Directory.GetFiles(datasetFolder, "*.parquet", SearchOption.TopDirectoryOnly);
                if (existingParquetFiles.Length > 0)
                {
                    foreach (var existingFile in existingParquetFiles)
                    {
                        System.Diagnostics.Debug.WriteLine($"ExportByGeometryType: Proactively removing layers for existing file: {existingFile}");
                        await RemoveLayersUsingFileAsync(existingFile); // This runs on MCT
                    }

                    System.Diagnostics.Debug.WriteLine($"ExportByGeometryType: Proactive layer removal complete for {datasetFolder}. Waiting for locks to release.");
                    await Task.Delay(1500); // Increased delay after proactive removal
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    await Task.Delay(750); // Short additional delay after GC
                    System.Diagnostics.Debug.WriteLine($"ExportByGeometryType: Post-GC delay complete for {datasetFolder}.");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"ExportByGeometryType: No existing .parquet files found in {datasetFolder} for proactive cleanup.");
                }
            }

            // Additional delay and GC after all proactive cleanup for the current datasetFolder is complete
            System.Diagnostics.Debug.WriteLine($"ExportByGeometryType: Additional delay and GC after proactive cleanup of {datasetFolder} before processing geometry types.");
            await Task.Delay(2000); // 2-second delay
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await Task.Delay(500); // Short additional delay after GC
            System.Diagnostics.Debug.WriteLine($"ExportByGeometryType: Post-additional GC delay for {datasetFolder}.");

            if (!Directory.Exists(datasetFolder))
            {
                System.Diagnostics.Debug.WriteLine($"Creating dataset folder: {datasetFolder}");
                Directory.CreateDirectory(datasetFolder);
            }

            foreach (var geomType in geomTypes)
            {
                progress?.Report($"Processing {geomType} features for {theme}/{type}...");

                string filteredQuery = $@"
                    SELECT 
                        *,
                        ST_Transform({GEOMETRY_COLUMN}, '{DEFAULT_SRS}', '{DEFAULT_SRS}') AS {GEOMETRY_COLUMN}
                    FROM current_table
                    WHERE ST_GeometryType(geometry) = '{geomType}'";

                string geometryDescription = GetDescriptiveGeometryType(geomType);
                string fileName = $"{theme}_{type}_{geometryDescription}.parquet";

                string finalParquetPath = Path.Combine(datasetFolder, fileName);
                string tempFileName = $"temp_{Guid.NewGuid()}_{fileName}";
                string tempParquetPath = Path.Combine(datasetFolder, tempFileName);

                if (!Directory.Exists(datasetFolder))
                {
                    System.Diagnostics.Debug.WriteLine($"Creating dataset folder: {datasetFolder}");
                    Directory.CreateDirectory(datasetFolder);
                }

                bool exportToTempSuccess = false;
                Exception lastExportToTempException = null;
                progress?.Report($"Exporting data for {fileName} to temporary file...");
                for (int exportAttempt = 0; exportAttempt < 3 && !exportToTempSuccess; exportAttempt++)
                {
                    try
                    {
                        // Use tempFileName for the layer name hint during temp export if needed by ExportToGeoParquet
                        await ExportToGeoParquet(filteredQuery, tempParquetPath, tempFileName);
                        exportToTempSuccess = true;
                        System.Diagnostics.Debug.WriteLine($"Successfully exported to temporary file: {tempParquetPath}");
                    }
                    catch (Exception ex)
                    {
                        lastExportToTempException = ex;
                        System.Diagnostics.Debug.WriteLine($"Temporary export attempt {exportAttempt + 1} for {fileName} failed: {ex.Message}");
                        if (File.Exists(tempParquetPath)) try { File.Delete(tempParquetPath); } catch (Exception iex) { System.Diagnostics.Debug.WriteLine($"Failed to delete incomplete temp file {tempParquetPath}: {iex.Message}"); }
                        await Task.Delay(1000 * (exportAttempt + 1));
                    }
                }

                if (!exportToTempSuccess)
                {
                    progress?.Report($"Failed to create temporary data for {fileName}. Last error: {lastExportToTempException?.Message}");
                    System.Diagnostics.Debug.WriteLine($"All temporary export attempts for {fileName} failed. Last error: {lastExportToTempException?.Message}");
                    continue;
                }

                if (File.Exists(finalParquetPath))
                {
                    progress?.Report($"Attempting to replace existing file: {fileName}...");
                    System.Diagnostics.Debug.WriteLine($"Attempting to delete final target '{finalParquetPath}' before rename.");
                    await DeleteParquetFileAsync(finalParquetPath);
                }

                bool renameSuccess = false;
                if (File.Exists(tempParquetPath))
                {
                    if (!File.Exists(finalParquetPath))
                    {
                        try
                        {
                            File.Move(tempParquetPath, finalParquetPath);
                            renameSuccess = true;
                            System.Diagnostics.Debug.WriteLine($"Successfully moved temporary file to final path: {finalParquetPath}");
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine($"Failed to move temporary file {tempParquetPath} to {finalParquetPath}: {ex.Message}");
                            if (File.Exists(tempParquetPath)) try { File.Delete(tempParquetPath); } catch (Exception iex) { System.Diagnostics.Debug.WriteLine($"Failed to delete orphaned temp file {tempParquetPath} after move failed: {iex.Message}"); }
                        }
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine($"Final target file {finalParquetPath} still exists after delete attempts. Cannot rename temporary file. Cleaning up temp file.");
                        if (File.Exists(tempParquetPath)) try { File.Delete(tempParquetPath); } catch (Exception iex) { System.Diagnostics.Debug.WriteLine($"Failed to delete orphaned temp file {tempParquetPath} because final target still exists: {iex.Message}"); }
                    }
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"Temporary file {tempParquetPath} does not exist, cannot rename. This might indicate an issue with the temp export.");
                }

                if (renameSuccess && File.Exists(finalParquetPath))
                {
                    var fileInfo = new FileInfo(finalParquetPath);
                    if (fileInfo.Length > 0)
                    {
                        progress?.Report($"Adding {fileName} to map...");
                        await QueuedTask.Run(() =>
                        {
                            var map = MapView.Active?.Map;
                            if (map != null)
                            {
                                var newLayer = LayerFactory.Instance.CreateLayer(new Uri(finalParquetPath), map);

                                if (newLayer is FeatureLayer featureLayerForCacheSettings)
                                {
                                    try
                                    {
                                        var layerDef = featureLayerForCacheSettings.GetDefinition() as CIMFeatureLayer;
                                        if (layerDef != null)
                                        {
                                            System.Diagnostics.Debug.WriteLine($"Setting cache options for layer: {featureLayerForCacheSettings.Name}");
                                            layerDef.DisplayCacheType = ArcGIS.Core.CIM.DisplayCacheType.None;
                                            layerDef.FeatureCacheType = ArcGIS.Core.CIM.FeatureCacheType.None;
                                            featureLayerForCacheSettings.SetDefinition(layerDef);
                                            System.Diagnostics.Debug.WriteLine($"Successfully set DisplayCacheType and FeatureCacheType to None for {featureLayerForCacheSettings.Name}.");
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        System.Diagnostics.Debug.WriteLine($"Warning: Failed to set cache options for layer {featureLayerForCacheSettings.Name}: {ex.Message}");
                                    }
                                }

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
                                        System.Diagnostics.Debug.WriteLine($"Failed to disable binning: {ex.Message}");
                                    }
                                }
                            }
                        });
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine($"Final parquet file {finalParquetPath} is empty or does not exist after rename. Layer not added.");
                        progress?.Report($"Error: Final data file for {fileName} is invalid.");
                    }
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"Failed to prepare final file {finalParquetPath}. Layer not added for {fileName}.");
                    progress?.Report($"Failed to update data on map for {fileName}.");
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
