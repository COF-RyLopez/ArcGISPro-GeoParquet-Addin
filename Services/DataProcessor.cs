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
        private static readonly string[] THEME_TYPE_SEPARATOR = [" - "];

        // Add a class-level field to store the current extent
        private dynamic _currentExtent;

        // Fields to store theme context for file path generation
        private string _currentParentS3Theme;
        private string _currentActualS3Type;

        public DataProcessor()
        {
            _connection = new DuckDBConnection("DataSource=:memory:");
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

        public async Task<bool> IngestFileAsync(string s3Path, dynamic extent = null, string actualS3Type = null)
        {
            try
            {
                // Store the extent for later use in CreateFeatureLayerAsync
                _currentExtent = extent;

                // Store the actualS3Type for context
                _currentActualS3Type = actualS3Type;

                // Clear previous parent theme context, it will be set by CreateFeatureLayerAsync if needed
                _currentParentS3Theme = null;

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
                string GetActualFilePath(string dataSourcePath)
                {
                    if (string.IsNullOrEmpty(dataSourcePath)) return null;
                    string lowerPath = dataSourcePath.ToLowerInvariant();
                    // If path is like "...file.parquet\dataset_name"
                    if (lowerPath.Contains(".parquet\\") || lowerPath.Contains(".parquet/"))
                    {
                        int parquetIndex = lowerPath.IndexOf(".parquet");
                        if (parquetIndex > -1)
                        {
                            return Path.GetFullPath(dataSourcePath[..(parquetIndex + ".parquet".Length)]).ToLowerInvariant();
                        }
                    }
                    return Path.GetFullPath(dataSourcePath).ToLowerInvariant(); // Default to the full path
                }
                ;

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
                                    string actualLayerFilePath = GetActualFilePath(rawLayerDataSourcePath);
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
                                    string actualDsPathViaConnector = GetActualFilePath(dsPathViaConnectorRaw);
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
                                    string actualTableFilePath = GetActualFilePath(rawTableDataSourcePath);
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
                                    string actualDsPathViaConnector = GetActualFilePath(dsPathViaConnectorRaw);
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
        /// <param name="parentS3Theme">The S3 parent theme key (e.g., 'buildings').</param>
        /// <param name="actualS3Type">The specific S3 data type (e.g., 'building', 'building_part').</param>
        /// <param name="dataOutputPathRoot">The root directory where data for the current release is stored (e.g., C:\...\ProjectHome\OvertureProAddinData\Data\{ReleaseVersion})</param>
        public async Task CreateFeatureLayerAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot)
        {
            System.Diagnostics.Debug.WriteLine($"CreateFeatureLayerAsync called for: {layerNameBase}, ParentS3Theme: {parentS3Theme}, ActualS3Type: {actualS3Type}, DataOutputPathRoot: {dataOutputPathRoot}");
            progress?.Report($"Starting layer creation for {layerNameBase}");

            if (string.IsNullOrEmpty(parentS3Theme) || string.IsNullOrEmpty(actualS3Type))
            {
                progress?.Report("Error: Parent theme or actual type not provided. Cannot create feature layer.");
                System.Diagnostics.Debug.WriteLine("Error: ParentS3Theme or ActualS3Type is null or empty. Aborting CreateFeatureLayerAsync.");
                return;
            }
            if (string.IsNullOrEmpty(dataOutputPathRoot))
            {
                progress?.Report("Error: Data output path root not provided. Cannot create feature layer.");
                System.Diagnostics.Debug.WriteLine("Error: dataOutputPathRoot is null or empty. Aborting CreateFeatureLayerAsync.");
                return;
            }

            // Store the theme context for this operation (might be used for layer naming or other non-path logic)
            _currentParentS3Theme = parentS3Theme;
            _currentActualS3Type = actualS3Type;

            // Define the specific output folder for this actualS3Type directly under the dataOutputPathRoot
            // Path structure: dataOutputPathRoot / actualS3Type / *.parquet
            string themeTypeSpecificFolder = Path.Combine(dataOutputPathRoot, actualS3Type);

            // Ensure the directory structure exists up to the actualS3Type level
            if (!Directory.Exists(dataOutputPathRoot))
            {
                System.Diagnostics.Debug.WriteLine($"Creating base data output path root: {dataOutputPathRoot}");
                Directory.CreateDirectory(dataOutputPathRoot); // Create the ...\Data\{Release} folder if it doesn't exist
            }
            if (!Directory.Exists(themeTypeSpecificFolder))
            {
                System.Diagnostics.Debug.WriteLine($"Creating theme-specific output folder: {themeTypeSpecificFolder}");
                Directory.CreateDirectory(themeTypeSpecificFolder); // Create the ...\Data\{Release}\actualS3Type folder
            }

            System.Diagnostics.Debug.WriteLine($"Theme-specific output folder set to: {themeTypeSpecificFolder}");

            try
            {
                // Now pass this specific folder to ExportByGeometryType
                await ExportByGeometryType(layerNameBase, themeTypeSpecificFolder, progress);

                progress?.Report($"Feature layer creation process completed for {layerNameBase}.");
                System.Diagnostics.Debug.WriteLine($"Feature layer creation process completed for {layerNameBase}");
            }
            catch (Exception ex)
            {
                progress?.Report($"Error creating feature layer {layerNameBase}: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Error in CreateFeatureLayerAsync for {layerNameBase}: {ex.Message}");
                // Handle or rethrow the exception as appropriate
                throw;
            }
            finally
            {
                // Clear theme context after operation
                _currentParentS3Theme = null;
                _currentActualS3Type = null;
            }
        }

        private async Task ExportByGeometryType(string layerNameBase, string themeTypeSpecificOutputFolder, IProgress<string> progress = null)
        {
            System.Diagnostics.Debug.WriteLine($"ExportByGeometryType for {layerNameBase}, OutputFolder: {themeTypeSpecificOutputFolder}");
            progress?.Report($"Processing geometry types for {layerNameBase}");

            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT DISTINCT ST_GeometryType(geometry) as geom_type FROM current_table WHERE geometry IS NOT NULL";

            var geometryTypes = new List<string>();
            try
            {
                using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                while (await reader.ReadAsync())
                {
                    var geomType = reader.GetString(0);
                    if (!string.IsNullOrEmpty(geomType))
                    {
                        geometryTypes.Add(geomType);
                        System.Diagnostics.Debug.WriteLine($"Found geometry type: {geomType}");
                    }
                }
            }
            catch (Exception ex)
            {
                progress?.Report($"Error reading geometry types: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Error reading geometry types: {ex.Message}");
                throw; // Re-throw to be caught by CreateFeatureLayerAsync
            }

            if (geometryTypes.Count == 0)
            {
                progress?.Report("No geometries found in the current dataset. Skipping layer creation.");
                System.Diagnostics.Debug.WriteLine("No geometry types found. Skipping export.");
                return;
            }

            int typeIndex = 0;
            foreach (var geomType in geometryTypes)
            {
                typeIndex++;
                progress?.Report($"Processing {layerNameBase}: {geomType} ({typeIndex}/{geometryTypes.Count})");
                System.Diagnostics.Debug.WriteLine($"Processing {layerNameBase}: {geomType}");

                string descriptiveGeomType = GetDescriptiveGeometryType(geomType);
                string finalLayerName = geometryTypes.Count > 1 ? $"{layerNameBase} ({descriptiveGeomType})" : layerNameBase;
                string outputFileName = $"{MfcUtility.SanitizeFileName(finalLayerName)}.parquet";

                // Ensure output path uses the theme/type specific folder
                string outputPath = Path.Combine(themeTypeSpecificOutputFolder, outputFileName);
                System.Diagnostics.Debug.WriteLine($"Output path for {geomType}: {outputPath}");

                // Ensure the specific output directory exists (it should from CreateFeatureLayerAsync, but double check)
                string specificDir = Path.GetDirectoryName(outputPath);
                if (!Directory.Exists(specificDir))
                {
                    System.Diagnostics.Debug.WriteLine($"Creating specific directory for Parquet export: {specificDir}");
                    Directory.CreateDirectory(specificDir);
                }

                // Check if the Parquet file already exists
                if (File.Exists(outputPath))
                {
                    progress?.Report($"File {outputFileName} already exists. Deleting before export.");
                    System.Diagnostics.Debug.WriteLine($"File {outputPath} already exists. Deleting.");
                    await DeleteParquetFileAsync(outputPath);
                }

                string query = $"SELECT * EXCLUDE geometry, ST_AsWKB(geometry) as geometry FROM current_table WHERE ST_GeometryType(geometry) = '{geomType}'";

                try
                {
                    await ExportToGeoParquet(query, outputPath, finalLayerName, progress);
                    await AddLayerToMapAsync(outputPath, finalLayerName, progress);
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error exporting or adding layer for {geomType}: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Error during export/add for {geomType} of {layerNameBase}: {ex.Message}");
                    // Decide if we should continue with other geometry types or rethrow
                    // For now, log and continue to attempt other types
                }
            }
            progress?.Report($"Finished processing all geometry types for {layerNameBase}.");
        }

        private async Task ExportToGeoParquet(string query, string outputPath, string _layerName, IProgress<string> progress = null)
        {
            progress?.Report($"Exporting data for {_layerName} to {Path.GetFileName(outputPath)}");
            System.Diagnostics.Debug.WriteLine($"Exporting {_layerName} to {outputPath}");
            System.Diagnostics.Debug.WriteLine($"Export query: {query}");

            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = BuildGeoParquetCopyCommand(query, outputPath);

                await command.ExecuteNonQueryAsync(CancellationToken.None);
                progress?.Report($"Successfully exported data for {_layerName}.");
                System.Diagnostics.Debug.WriteLine($"Successfully exported to {outputPath}");
            }
            catch (Exception ex)
            {
                progress?.Report($"Error exporting to GeoParquet for {_layerName}: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Error in ExportToGeoParquet for {_layerName} to {outputPath}: {ex.Message}");
                throw; // Re-throw to be caught by ExportByGeometryType
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

        private async Task AddLayerToMapAsync(string parquetFilePath, string layerName, IProgress<string> progress = null)
        {
            progress?.Report($"Adding layer {layerName} to map from {Path.GetFileName(parquetFilePath)}");
            System.Diagnostics.Debug.WriteLine($"Attempting to add layer: {layerName} from path: {parquetFilePath}");

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map == null)
                {
                    progress?.Report("Error: No active map found to add layer.");
                    System.Diagnostics.Debug.WriteLine("AddLayerToMapAsync: No active map found.");
                    return;
                }

                if (!File.Exists(parquetFilePath))
                {
                    progress?.Report($"Error: Parquet file not found at {parquetFilePath}");
                    System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Parquet file not found at {parquetFilePath}");
                    return;
                }

                try
                {
                    // Create a URI for the Parquet file
                    Uri dataUri = new(parquetFilePath);

                    // Create the layer using LayerFactory
                    Layer newLayer = LayerFactory.Instance.CreateLayer(dataUri, map, layerName: layerName);

                    if (newLayer == null)
                    {
                        progress?.Report($"Error: Could not create layer for {layerName}.");
                        System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: LayerFactory returned null for {parquetFilePath}");
                        return;
                    }

                    // Attempt to set cache settings to None for performance with potentially large/dynamic datasets
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

                    // Specific handling for address layers to disable feature reduction/binning by default
                    if (newLayer is FeatureLayer featureLayer) // Apply to all FeatureLayers
                    {
                        try
                        {
                            CIMFeatureLayer layerDef = featureLayer.GetDefinition() as CIMFeatureLayer;
                            if (layerDef != null)
                            {
                                // Specifically check for CIMBinningFeatureReduction
                                if (layerDef.FeatureReduction is CIMBinningFeatureReduction binningReduction)
                                {
                                    if (binningReduction.Enabled) // Only disable if it's currently enabled
                                    {
                                        binningReduction.Enabled = false;
                                        featureLayer.SetDefinition(layerDef); // Apply the change
                                        System.Diagnostics.Debug.WriteLine($"Disabled feature binning for layer: {featureLayer.Name}");
                                    }
                                    else
                                    {
                                        System.Diagnostics.Debug.WriteLine($"Feature binning already disabled for layer: {featureLayer.Name}");
                                    }
                                }
                                else if (layerDef.FeatureReduction != null)
                                {
                                    // Log if other types of feature reduction are present but not modified
                                    System.Diagnostics.Debug.WriteLine($"Layer {featureLayer.Name} has feature reduction of type '{layerDef.FeatureReduction.GetType().Name}', not binning. No changes made to feature reduction.");
                                }
                                // If layerDef.FeatureReduction is null, no feature reduction is configured, so nothing to do.
                            }
                        }
                        catch (Exception ex)
                        {
                            // Log any errors during the process
                            System.Diagnostics.Debug.WriteLine($"Warning: Failed to access or modify feature reduction for layer {featureLayer.Name}: {ex.Message}");
                        }
                    }

                    progress?.Report($"Successfully added layer: {newLayer.Name}");
                    System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Successfully added layer {newLayer.Name} to map {map.Name}");
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error adding layer {layerName} to map: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Error adding layer {layerName} from {parquetFilePath}: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Stack trace: {ex.StackTrace}");
                    // Optionally, show a message box to the user if critical
                    // ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show($"Failed to add layer {layerName}:\n{ex.Message}", "Layer Error", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
                }
            });
        }
    }
}
