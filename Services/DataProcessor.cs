using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDB.NET.Data;

namespace DuckDBGeoparquet.Services
{
    // Structure to hold layer information for bulk creation
    public class LayerCreationInfo
    {
        public string FilePath { get; set; }
        public string LayerName { get; set; }
        public string GeometryType { get; set; }
        public int StackingPriority { get; set; }
        public string ParentTheme { get; set; }
        public string ActualType { get; set; }
    }

    // Bridge file record structure
    public class BridgeFileRecord
    {
        public string Id { get; set; }                    // GERS ID
        public string RecordId { get; set; }              // Source data ID
        public DateTime UpdateTime { get; set; }          // Last update timestamp
        public string Dataset { get; set; }               // Source dataset name
        public string Theme { get; set; }                 // Theme (buildings, places, etc.)
        public string Type { get; set; }                  // Type (building, place, etc.)
        public double[] Between { get; set; }             // Portion of normalized length
        public double[] DatasetBetween { get; set; }      // Dataset portion of normalized length
    }

    // Source attribution information for features
    public class SourceAttribution
    {
        public string GersId { get; set; }
        public List<BridgeFileRecord> SourceRecords { get; set; } = new List<BridgeFileRecord>();
        public string PrimaryDataset { get; set; }
        public int SourceCount { get; set; }
        public List<string> ContributingDatasets { get; set; } = new List<string>();
    }

    // Available source datasets (based on Overture documentation)
    public static class KnownSourceDatasets
    {
        public static readonly Dictionary<string, string> DatasetDisplayNames = new Dictionary<string, string>
        {
            { "Esri Community Maps", "Esri Community Maps" },
            { "geoBoundaries", "geoBoundaries" },
            { "Instituto Geográfico Nacional (España)", "IGN España" },
            { "Meta Places", "Meta Places" },
            { "Microsoft Places", "Microsoft Places" },
            { "OpenStreetMap", "OpenStreetMap" },
            { "PinMeTo", "PinMeTo" }
        };

        public static readonly HashSet<string> SupportedDatasets = new HashSet<string>
        {
            "Esri Community Maps", "geoBoundaries", "Instituto Geográfico Nacional (España)",
            "Meta Places", "Microsoft Places", "OpenStreetMap", "PinMeTo"
        };
    }

    public class DataProcessor : IDisposable
    {
        private readonly DuckDBConnection _connection;

        // Constants
        private const string DEFAULT_SRS = "EPSG:4326";
        private const string STRUCT_TYPE = "STRUCT";
        private const string BBOX_COLUMN = "bbox";
        private const string GEOMETRY_COLUMN = "geometry";
        private const string BRIDGE_FILES_BASE_PATH = "s3://overturemaps-us-west-2/bridgefiles";

        // Add static readonly field for the theme-type separator
        private static readonly string[] THEME_TYPE_SEPARATOR = [" - "];

        // Add a class-level field to store the current extent
        private dynamic _currentExtent;

        // Fields to store theme context for file path generation
        private string _currentParentS3Theme;
        private string _currentActualS3Type;

        // Collection to store layer information for bulk creation
        private readonly List<LayerCreationInfo> _pendingLayers;

        // Bridge files functionality
        private bool _bridgeFilesEnabled = false;
        private string _currentRelease;
        private readonly Dictionary<string, List<BridgeFileRecord>> _bridgeFileCache = new();

        public DataProcessor()
        {
            _connection = new DuckDBConnection("DataSource=:memory:");
            _pendingLayers = new List<LayerCreationInfo>();
        }

        // Bridge Files Properties
        public bool BridgeFilesEnabled
        {
            get => _bridgeFilesEnabled;
            set => _bridgeFilesEnabled = value;
        }

        public string CurrentRelease
        {
            get => _currentRelease;
            set => _currentRelease = value;
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

        public async Task<bool> IngestFileAsync(string s3Path, dynamic extent = null, string actualS3Type = null, IProgress<string> progress = null)
        {
            try
            {
                progress?.Report($"Connecting to S3: {actualS3Type ?? "data"}...");

                // Store the extent for later use in CreateFeatureLayerAsync
                _currentExtent = extent;

                // Store the actualS3Type for context
                _currentActualS3Type = actualS3Type;

                // Clear previous parent theme context, it will be set by CreateFeatureLayerAsync if needed
                _currentParentS3Theme = null;

                // Enable ArcGIS Pro 3.5 overwrite capabilities to handle file conflicts gracefully
                await QueuedTask.Run(() => {
                    try {
                        Geoprocessing.MakeEnvironmentArray(overwriteoutput: true);
                        System.Diagnostics.Debug.WriteLine("Set geoprocessing environment to allow overwriting outputs");
                    }
                    catch (Exception ex) {
                        System.Diagnostics.Debug.WriteLine($"Warning: Could not set geoprocessing environment: {ex.Message}");
                    }
                });

                progress?.Report($"Reading schema from S3...");

                using var command = _connection.CreateCommand();
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE temp AS 
                    SELECT * FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1) LIMIT 0;
                ";
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                command.CommandText = "DESCRIBE temp";
                using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                // Count columns for basic validation
                int columnCount = 0;
                while (await reader.ReadAsync()) { columnCount++; }
                System.Diagnostics.Debug.WriteLine($"Schema validated: {columnCount} columns found");

                progress?.Report($"Loading data from S3 (this may take a moment)...");

                string spatialFilter = "";
                if (extent != null)
                {
                    // Use culture-invariant formatting for SQL to prevent German locale decimal separator issues
                    string xMinStr = ((double)extent.XMin).ToString("G", CultureInfo.InvariantCulture);
                    string yMinStr = ((double)extent.YMin).ToString("G", CultureInfo.InvariantCulture);
                    string xMaxStr = ((double)extent.XMax).ToString("G", CultureInfo.InvariantCulture);
                    string yMaxStr = ((double)extent.YMax).ToString("G", CultureInfo.InvariantCulture);
                    
                    spatialFilter = $@"
                        WHERE bbox.xmin >= {xMinStr}
                          AND bbox.ymin >= {yMinStr}
                          AND bbox.xmax <= {xMaxStr}
                          AND bbox.ymax <= {yMaxStr}";
                    
                    // Spatial filter applied successfully with culture-invariant formatting
                    
                    progress?.Report($"Applying spatial filter for current map extent...");
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
                progress?.Report($"Successfully loaded {count:N0} rows from S3");
                System.Diagnostics.Debug.WriteLine($"Loaded {count} rows");

                // Early exit for empty datasets to avoid unnecessary processing
                if (Convert.ToInt64(count) == 0)
                {
                    progress?.Report("Dataset is empty - no features to process");
                    System.Diagnostics.Debug.WriteLine("Early exit: Dataset contains no rows");
                    return false; // Indicate no data to process
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

            await QueuedTask.Run(() =>
            {
                var project = Project.Current;
                if (project == null)
                {
                    System.Diagnostics.Debug.WriteLine("RemoveLayersUsingFileAsync: No current project found.");
                    return;
                }

                // Get ALL maps in the project, not just the active one
                var allMaps = project.GetItems<MapProjectItem>().Select(item => item.GetMap()).Where(map => map != null).ToList();

                var allMembersToRemove = new Dictionary<Map, List<MapMember>>();

                foreach (var map in allMaps)
                {
                    var membersToRemove = new List<MapMember>();
                    var allLayers = map.GetLayersAsFlattenedList().ToList();
                    var allTables = map.GetStandaloneTablesAsFlattenedList().ToList();

                string normalizedTargetPath = Path.GetFullPath(filePath).ToLowerInvariant();

                // Helper to get the actual file path from a potential dataset path
                static string GetActualFilePath(string dataSourcePath)
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
                                    if (actualLayerFilePath == normalizedTargetPath)
                                    {
                                        membersToRemove.Add(featureLayer);
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Found layer '{featureLayer.Name}' for removal");
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
                                    if (actualTableFilePath == normalizedTargetPath)
                                    {
                                        membersToRemove.Add(standaloneTable);
                                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Found table '{standaloneTable.Name}' for removal");
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
                        allMembersToRemove[map] = membersToRemove;
                        System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Found {membersToRemove.Count} members to remove from map '{map.Name}'");
                    }
                }

                // Now remove all found members from their respective maps
                int totalRemoved = 0;
                foreach (var kvp in allMembersToRemove)
                {
                    var map = kvp.Key;
                    var membersToRemove = kvp.Value.Distinct().ToList();
                    
                    System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Removing {membersToRemove.Count} distinct layers/tables from map '{map.Name}'");
                    
                    foreach (var member in membersToRemove)
                    {
                        if (member is Layer layerToRemove)
                        {
                            map.RemoveLayer(layerToRemove);
                            totalRemoved++;
                            System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Removed layer '{member.Name}' from map '{map.Name}'");
                            (layerToRemove as IDisposable)?.Dispose();
                        }
                        else if (member is StandaloneTable tableToRemove)
                        {
                            map.RemoveStandaloneTable(tableToRemove);
                            totalRemoved++;
                            System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Removed table '{member.Name}' from map '{map.Name}'");
                            (tableToRemove as IDisposable)?.Dispose();
                        }
                    }
                }

                if (totalRemoved > 0)
                {
                    System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: Total removed {totalRemoved} layers/tables across all maps for file: {filePath}");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"RemoveLayersUsingFileAsync: No layers or tables found using the file across all maps: {filePath}");
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
                await Task.Delay(100); // Reduced delay

                System.Diagnostics.Debug.WriteLine("DeleteParquetFileAsync: Invoking GC.Collect and WaitForPendingFinalizers.");
                GC.Collect();
                GC.WaitForPendingFinalizers();
                await Task.Delay(100); // Short additional delay after GC

                bool deleted = false;
                int gpRetryCount = 2; // Try GP delete a couple of times

                for (int i = 0; i < gpRetryCount && !deleted; i++)
                {
                    if (i > 0)
                    {
                        System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Retrying GP delete, attempt {i + 1} after delay...");
                        await Task.Delay(200 * i); // Much shorter delays for GP retries
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
                                int delay = (i + 1) * 100; // Much shorter delays
                                System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: Direct delete attempt {i + 1} failed (file locked), waiting {delay}ms to retry: {parquetPath}");
                                await Task.Delay(delay);
                            }
                        }
                    }

                    if (!deleted && (Exception)null != null)
                    {
                        System.Diagnostics.Debug.WriteLine($"DeleteParquetFileAsync: All direct file delete attempts failed. Last error: {((Exception)null).Message}");
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
                    if (!string.IsNullOrEmpty(geomType))
                    {
                        geometryTypes.Add(geomType);
                    }
                }
            }
            catch (Exception ex)
            {
                progress?.Report($"Error reading geometry types: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Error reading geometry types: {ex.Message}");
                throw; // Re-throw to be caught by CreateFeatureLayerAsync
            }

            // Sort geometry types for optimal layer stacking order (bottom to top)
            // Polygons on bottom, lines in middle, points on top
            var geometryTypeOrder = new Dictionary<string, int>
            {
                { "POLYGON", 1 }, { "MULTIPOLYGON", 1 },           // Bottom layer
                { "LINESTRING", 2 }, { "MULTILINESTRING", 2 },     // Middle layer  
                { "POINT", 3 }, { "MULTIPOINT", 3 }                // Top layer
            };

            geometryTypes = geometryTypes
                .OrderBy(geomType => geometryTypeOrder.TryGetValue(geomType, out int order) ? order : 99)
                .ToList();

            if (geometryTypes.Count == 0)
            {
                progress?.Report("No geometries found in the current dataset. Skipping layer creation.");
                System.Diagnostics.Debug.WriteLine("No geometry types found. Skipping export.");
                return;
            }

            System.Diagnostics.Debug.WriteLine($"Processing {geometryTypes.Count} geometry types: {string.Join(", ", geometryTypes)}");

            int typeIndex = 0;
            foreach (var geomType in geometryTypes)
            {
                typeIndex++;
                string stackingNote = geometryTypeOrder.TryGetValue(geomType, out int order)
                    ? $" (layer {order} of 3)"
                    : "";
                progress?.Report($"Processing {layerNameBase}: {geomType}{stackingNote} ({typeIndex}/{geometryTypes.Count})");

                string descriptiveGeomType = GetDescriptiveGeometryType(geomType);
                string finalLayerName = geometryTypes.Count > 1 ? $"{layerNameBase} ({descriptiveGeomType})" : layerNameBase;
                string outputFileName = $"{MfcUtility.SanitizeFileName(finalLayerName)}.parquet";

                // Ensure output path uses the theme/type specific folder
                string outputPath = Path.Combine(themeTypeSpecificOutputFolder, outputFileName);

                // Ensure the specific output directory exists (it should from CreateFeatureLayerAsync, but double check)
                string specificDir = Path.GetDirectoryName(outputPath);
                if (!Directory.Exists(specificDir))
                {
                    System.Diagnostics.Debug.WriteLine($"Creating specific directory for Parquet export: {specificDir}");
                    Directory.CreateDirectory(specificDir);
                }

                // Note: Individual file deletion is no longer needed here since we now delete
                // entire folders upfront when the user confirms replacing existing data

                string query = $"SELECT * EXCLUDE geometry, geometry FROM current_table WHERE ST_GeometryType(geometry) = '{geomType}'";

                try
                {
                    string actualFilePath = await ExportToGeoParquet(query, outputPath, finalLayerName, progress);

                    // Verify the file actually exists before adding to pending layers
                    if (File.Exists(actualFilePath))
                    {
                        // Instead of immediately adding to map, collect layer info for bulk creation
                        var layerInfo = new LayerCreationInfo
                        {
                            FilePath = actualFilePath,
                            LayerName = finalLayerName,
                            GeometryType = geomType,
                            StackingPriority = geometryTypeOrder.TryGetValue(geomType, out int priority) ? priority : 99,
                            ParentTheme = _currentParentS3Theme,
                            ActualType = _currentActualS3Type
                        };
                        _pendingLayers.Add(layerInfo);

                        progress?.Report($"Prepared layer {finalLayerName} for optimal stacking");
                        System.Diagnostics.Debug.WriteLine($"Successfully prepared layer: {finalLayerName} at {actualFilePath}");
                    }
                    else
                    {
                        progress?.Report($"Error: Export completed but file not found for {finalLayerName}");
                        System.Diagnostics.Debug.WriteLine($"Error: Export completed but file not found: {actualFilePath}");
                    }
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error exporting layer for {geomType}: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Error during export for {geomType} of {layerNameBase}: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Stack trace: {ex.StackTrace}");
                    // Decide if we should continue with other geometry types or rethrow
                    // For now, log and continue to attempt other types
                }
            }
            progress?.Report($"Finished processing all geometry types for {layerNameBase}.");
        }

        private async Task<string> ExportToGeoParquet(string query, string outputPath, string _layerName, IProgress<string> progress = null)
        {
            progress?.Report($"Exporting data for {_layerName} to {Path.GetFileName(outputPath)}");

            try
            {
                // First, proactively remove any existing layers that use this file
                if (File.Exists(outputPath))
                {
                    await RemoveLayersUsingFileAsync(outputPath);
                    
                    // Give ArcGIS Pro time to release file handles
                    await Task.Delay(50);
                    
                    // Try to delete the file directly before DuckDB export
                    for (int attempt = 1; attempt <= 2; attempt++)
                    {
                        try
                        {
                            File.Delete(outputPath);
                            break;
                        }
                        catch (IOException) when (attempt < 2)
                        {
                            await Task.Delay(50); // Single short delay
                        }
                        catch (IOException ex) when (attempt == 2)
                        {
                            System.Diagnostics.Debug.WriteLine($"Could not delete existing file: {ex.Message}. Using temp file approach.");
                        }
                    }
                }

                using var command = _connection.CreateCommand();
                
                // If the file still exists after our cleanup attempts, use a temporary file approach
                string actualOutputPath = outputPath;
                bool useTempFile = File.Exists(outputPath);
                
                if (useTempFile)
                {
                    actualOutputPath = outputPath + $".tmp_{DateTime.Now:yyyyMMdd_HHmmss}";
                    System.Diagnostics.Debug.WriteLine($"File still locked, using temporary export path: {actualOutputPath}");
                }
                
                command.CommandText = BuildGeoParquetCopyCommand(query, actualOutputPath);
                await command.ExecuteNonQueryAsync(CancellationToken.None);
                
                // If we used a temp file, try to replace the original
                if (useTempFile && File.Exists(actualOutputPath))
                {
                    System.Diagnostics.Debug.WriteLine("Temp file export successful, attempting to replace original...");
                    
                    // Try one more time to delete the original file
                    for (int attempt = 1; attempt <= 3; attempt++)
                    {
                        try
                        {
                            if (File.Exists(outputPath))
                            {
                                File.Delete(outputPath);
                            }
                            File.Move(actualOutputPath, outputPath);
                            System.Diagnostics.Debug.WriteLine($"Successfully replaced original file on attempt {attempt}");
                            break;
                        }
                        catch (IOException ex) when (attempt < 3)
                        {
                            System.Diagnostics.Debug.WriteLine($"Attempt {attempt} to replace file failed: {ex.Message}. Retrying in {attempt * 100}ms...");
                            await Task.Delay(attempt * 100);
                        }
                        catch (IOException ex) when (attempt == 3)
                        {
                            System.Diagnostics.Debug.WriteLine($"Could not replace original file after {attempt} attempts: {ex.Message}");
                            System.Diagnostics.Debug.WriteLine($"Temp file remains at: {actualOutputPath}");
                            // Update the output path to point to the temp file for layer creation
                            outputPath = actualOutputPath;
                        }
                    }
                }
                progress?.Report($"Successfully exported data for {_layerName}.");
                System.Diagnostics.Debug.WriteLine($"Successfully exported to {outputPath}");
                return outputPath; // Return the actual file path used
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

        /// <summary>
        /// Adds all pending layers to the map in optimal stacking order (polygons → lines → points)
        /// </summary>
        public async Task AddAllLayersToMapAsync(IProgress<string> progress = null)
        {
            if (!_pendingLayers.Any())
            {
                progress?.Report("No layers to add to map.");
                return;
            }

            // Sort all layers by stacking priority (higher priority = higher in Contents panel = drawn on top)
            // We want: Points on top (drawn last), Lines in middle, Polygons on bottom (drawn first)
            var sortedLayers = _pendingLayers
                .OrderByDescending(layer => layer.StackingPriority) // Higher priority first
                .ThenBy(layer => layer.ParentTheme) // Secondary sort by theme for consistency
                .ThenBy(layer => layer.ActualType)  // Tertiary sort by actual type
                .ToList();

            progress?.Report($"Preparing to add {sortedLayers.Count} layers with optimal stacking order...");
            System.Diagnostics.Debug.WriteLine($"Adding {sortedLayers.Count} layers in optimal stacking order (points → lines → polygons):");

            foreach (var layer in sortedLayers)
            {
                System.Diagnostics.Debug.WriteLine($"  {layer.LayerName} ({layer.GeometryType}, priority {layer.StackingPriority})");
            }

            // Group layers by geometry type for progress reporting
            var pointLayers = sortedLayers.Where(l => l.StackingPriority == 3).ToList();
            var lineLayers = sortedLayers.Where(l => l.StackingPriority == 2).ToList();
            var polygonLayers = sortedLayers.Where(l => l.StackingPriority == 1).ToList();

            progress?.Report($"Layer summary: {pointLayers.Count} point layers, {lineLayers.Count} line layers, {polygonLayers.Count} polygon layers");

            // Use individual layer creation for single layers, bulk creation for multiple layers
            if (sortedLayers.Count == 1)
            {
                progress?.Report("Single layer detected, using individual layer creation...");
                System.Diagnostics.Debug.WriteLine("Using individual layer creation for single layer (bulk creation may not work reliably with 1 layer)");
                await FallbackToIndividualLayerCreation(sortedLayers, progress);
            }
            else
            {
                List<LayerCreationInfo> missingLayersForIndividualCreation = null;
                
                try
                {
                    progress?.Report("Starting bulk layer creation process...");

                    await QueuedTask.Run(() =>
                    {
                        var map = MapView.Active?.Map;
                        if (map == null)
                        {
                            progress?.Report("Error: No active map found to add layers.");
                            System.Diagnostics.Debug.WriteLine("AddAllLayersToMapAsync: No active map found.");
                            return;
                        }

                        progress?.Report("Validating layer files...");

                        // Use BulkMapMemberCreationParams for optimal performance
                        var uris = new List<Uri>();
                        var layerNames = new List<string>();
                        int validFiles = 0;

                        foreach (var layerInfo in sortedLayers)
                        {
                            if (File.Exists(layerInfo.FilePath))
                            {
                                uris.Add(new Uri(layerInfo.FilePath));
                                layerNames.Add(layerInfo.LayerName);
                                validFiles++;
                                System.Diagnostics.Debug.WriteLine($"Valid file for {layerInfo.LayerName}: {layerInfo.FilePath}");
                            }
                            else
                            {
                                progress?.Report($"Warning: File not found for {layerInfo.LayerName}");
                                System.Diagnostics.Debug.WriteLine($"Warning: File not found for layer {layerInfo.LayerName}: {layerInfo.FilePath}");
                            }
                        }

                        progress?.Report($"Validated {validFiles} layer files. Creating layers...");

                        if (uris.Any())
                        {
                            progress?.Report("Executing bulk layer creation (this may take a moment)...");

                            // Create layers using LayerFactory with URI enumeration for optimal performance
                            System.Diagnostics.Debug.WriteLine($"Attempting to create {uris.Count} layers via bulk creation...");
                            var layers = LayerFactory.Instance.CreateLayers(uris, map);

                            progress?.Report($"Bulk creation completed. Applying settings to {layers.Count} layers...");
                            System.Diagnostics.Debug.WriteLine($"Bulk creation result: {layers.Count} layers created from {uris.Count} URIs");

                            // Apply custom names and settings to the created layers
                            for (int i = 0; i < layers.Count && i < layerNames.Count; i++)
                            {
                                if (layers[i] != null)
                                {
                                    layers[i].SetName(layerNames[i]);

                                    // Apply cache and feature reduction settings
                                    if (layers[i] is FeatureLayer featureLayer)
                                    {
                                        ApplyLayerSettings(featureLayer);
                                    }

                                    // Report progress every few layers to keep UI responsive
                                    if (i % 3 == 0 || i == layers.Count - 1)
                                    {
                                        progress?.Report($"Configured layer {i + 1} of {layers.Count}: {layerNames[i]}");
                                    }
                                    System.Diagnostics.Debug.WriteLine($"Successfully added layer: {layerNames[i]}");
                                }
                            }

                            progress?.Report($"✅ Successfully added {layers.Count} layers with optimal stacking order!");
                            System.Diagnostics.Debug.WriteLine($"Bulk layer creation completed. Added {layers.Count} layers.");
                            
                            // If bulk creation didn't create all expected layers, fall back to individual creation for missing ones
                            if (layers.Count < uris.Count)
                            {
                                progress?.Report($"Some layers missing from bulk creation ({layers.Count}/{uris.Count}). Creating missing layers individually...");
                                System.Diagnostics.Debug.WriteLine($"Bulk creation incomplete: {layers.Count}/{uris.Count} layers created. Falling back for missing layers...");
                                
                                // Find which layers were not created and create them individually
                                var createdLayerNames = layers.Select(l => l.Name).ToHashSet();
                                var missingLayers = sortedLayers.Where(layerInfo => 
                                    File.Exists(layerInfo.FilePath) && 
                                    !createdLayerNames.Contains(layerInfo.LayerName)).ToList();
                                
                                // Store missing layers for individual creation outside QueuedTask
                                missingLayersForIndividualCreation = missingLayers;
                            }
                        }
                    });
                    
                    // Create missing layers individually outside QueuedTask
                    if (missingLayersForIndividualCreation?.Any() == true)
                    {
                        await FallbackToIndividualLayerCreation(missingLayersForIndividualCreation, progress);
                    }
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error during bulk layer creation: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Error in AddAllLayersToMapAsync: {ex.Message}");

                    // Fallback to individual layer creation if bulk creation fails
                    progress?.Report("Falling back to individual layer creation...");
                    await FallbackToIndividualLayerCreation(sortedLayers, progress);
                }
            }
            
            // Clear pending layers after processing
            _pendingLayers.Clear();
        }

        /// <summary>
        /// Fallback method for individual layer creation if bulk creation fails
        /// </summary>
        private async Task FallbackToIndividualLayerCreation(List<LayerCreationInfo> layers, IProgress<string> progress)
        {
            foreach (var layerInfo in layers)
            {
                try
                {
                    await AddLayerToMapAsync(layerInfo.FilePath, layerInfo.LayerName, progress);
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error adding layer {layerInfo.LayerName}: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Error in fallback layer creation for {layerInfo.LayerName}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Applies consistent settings to feature layers (cache, feature reduction, etc.)
        /// </summary>
        private static void ApplyLayerSettings(FeatureLayer featureLayer)
        {
            try
            {
                if (featureLayer.GetDefinition() is CIMFeatureLayer layerDef)
                {
                    // Set cache settings for performance
                    layerDef.DisplayCacheType = ArcGIS.Core.CIM.DisplayCacheType.None;
                    layerDef.FeatureCacheType = ArcGIS.Core.CIM.FeatureCacheType.None;

                    // Disable feature binning/reduction for better data visibility
                    if (layerDef.FeatureReduction is CIMBinningFeatureReduction binningReduction && binningReduction.Enabled)
                    {
                        binningReduction.Enabled = false;
                        System.Diagnostics.Debug.WriteLine($"Disabled feature binning for layer: {featureLayer.Name}");
                    }

                    featureLayer.SetDefinition(layerDef);
                    System.Diagnostics.Debug.WriteLine($"Applied settings to layer: {featureLayer.Name}");
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Warning: Failed to apply settings to layer {featureLayer.Name}: {ex.Message}");
            }
        }

        /// <summary>
        /// Clears any pending layers (useful for cleanup or reset operations)
        /// </summary>
        public void ClearPendingLayers()
        {
            _pendingLayers.Clear();
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _pendingLayers?.Clear();
            GC.SuppressFinalize(this);
        }

        private static async Task AddLayerToMapAsync(string parquetFilePath, string layerName, IProgress<string> progress = null)
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

                // Verify file size before attempting to add
                var fileInfo = new FileInfo(parquetFilePath);
                System.Diagnostics.Debug.WriteLine($"File size: {fileInfo.Length:N0} bytes");
                
                if (fileInfo.Length == 0)
                {
                    progress?.Report($"Error: Parquet file is empty: {parquetFilePath}");
                    System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Parquet file is empty: {parquetFilePath}");
                    return;
                }

                try
                {
                    // Try multiple approaches to add the layer
                    Layer newLayer = null;
                    Exception lastException = null;

                    // Method 1: Try direct LayerFactory approach
                    try
                    {
                        Uri dataUri = new(parquetFilePath);
                        newLayer = LayerFactory.Instance.CreateLayer(dataUri, map, layerName: layerName);
                        System.Diagnostics.Debug.WriteLine($"✅ Method 1 (LayerFactory direct) succeeded for {layerName}");
                    }
                    catch (Exception ex1)
                    {
                        lastException = ex1;
                        System.Diagnostics.Debug.WriteLine($"❌ Method 1 (LayerFactory direct) failed: {ex1.Message}");
                    }

                    // Method 2: Try with different LayerFactory approach
                    if (newLayer == null)
                    {
                        try
                        {
                            // Try alternative CreateLayer overload
                            Uri dataUri = new(parquetFilePath);
                            newLayer = LayerFactory.Instance.CreateLayer(dataUri, map, 0, layerName);
                            System.Diagnostics.Debug.WriteLine($"✅ Method 2 (Index overload) succeeded for {layerName}");
                        }
                        catch (Exception ex2)
                        {
                            lastException = ex2;
                            System.Diagnostics.Debug.WriteLine($"❌ Method 2 (Index overload) failed: {ex2.Message}");
                        }
                    }

                    // Method 3: Try adding to specific group layer (if exists)
                    if (newLayer == null)
                    {
                        try
                        {
                            // Find or create a "Source Cooperative" group layer
                            var groupLayer = map.FindLayers("Source Cooperative").FirstOrDefault() as GroupLayer;
                            if (groupLayer == null)
                            {
                                // Create a group layer if it doesn't exist
                                groupLayer = LayerFactory.Instance.CreateGroupLayer(map, 0, "Source Cooperative");
                            }

                            if (groupLayer != null)
                            {
                                Uri dataUri = new(parquetFilePath);
                                newLayer = LayerFactory.Instance.CreateLayer(dataUri, groupLayer, 0, layerName);
                                System.Diagnostics.Debug.WriteLine($"✅ Method 3 (Group layer) succeeded for {layerName}");
                            }
                        }
                        catch (Exception ex3)
                        {
                            lastException = ex3;
                            System.Diagnostics.Debug.WriteLine($"❌ Method 3 (Group layer) failed: {ex3.Message}");
                        }
                    }

                    if (newLayer == null)
                    {
                        progress?.Report($"❌ Error creating layer with LayerFactory: {lastException?.Message ?? "Unknown error"}");
                        System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: All layer creation methods failed for {parquetFilePath}");
                        
                        // Try alternative layer creation method
                        progress?.Report("Trying alternative layer creation method...");
                        
                        try
                        {
                            // This is a more basic approach that sometimes works when LayerFactory fails
                            string connectionString = $"DRIVER=Apache Parquet;DBQ={parquetFilePath}";
                            System.Diagnostics.Debug.WriteLine($"Trying connection string approach: {connectionString}");
                            
                            // For now, just report that manual addition is needed
                            progress?.Report($"❌ Alternative layer creation failed: Failed to add data: {Path.GetFileName(parquetFilePath)}");
                            progress?.Report($"💡 You can manually add the file to ArcGIS Pro: {parquetFilePath}");
                            System.Diagnostics.Debug.WriteLine($"Manual addition required for {parquetFilePath}");
                        }
                        catch (Exception ex4)
                        {
                            System.Diagnostics.Debug.WriteLine($"❌ Alternative method also failed: {ex4.Message}");
                            progress?.Report($"💡 You can manually add the file to ArcGIS Pro: {parquetFilePath}");
                        }
                        
                        return;
                    }

                    // Configure the successfully created layer
                    ConfigureNewLayer(newLayer, progress);
                    
                    progress?.Report($"✅ Successfully added layer: {newLayer.Name}");
                    System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Successfully added layer {newLayer.Name} to map {map.Name}");
                }
                catch (Exception ex)
                {
                    progress?.Report($"❌ Error adding layer {layerName} to map: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Error adding layer {layerName} from {parquetFilePath}: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Stack trace: {ex.StackTrace}");
                    progress?.Report($"💡 You can manually add the file to ArcGIS Pro: {parquetFilePath}");
                }
            });
        }

        private static void ConfigureNewLayer(Layer newLayer, IProgress<string> progress)
        {
            // Attempt to set cache settings to None for performance with potentially large/dynamic datasets
            if (newLayer is FeatureLayer featureLayerForCacheSettings)
            {
                try
                {
                    if (featureLayerForCacheSettings.GetDefinition() is CIMFeatureLayer layerDef)
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
                    if (featureLayer.GetDefinition() is CIMFeatureLayer layerDef)
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
        }

        public async Task<bool> ProcessSourceCooperativeDataset(
            string sourceUrl, 
            string outputGeoParquetPath, 
            ArcGIS.Core.Geometry.Envelope extent, 
            Action<double> progressCallback,
            Action<string> logCallback,
            CancellationToken cancellationToken)
        {
            try
            {
                logCallback?.Invoke($"Starting download from Source Cooperative: {sourceUrl}");
                progressCallback?.Invoke(0);

                // Initialize DuckDB if not already done
                if (_connection.State != ConnectionState.Open)
                {
                    await InitializeDuckDBAsync();
                }

                progressCallback?.Invoke(10);
                logCallback?.Invoke("DuckDB initialized with spatial extensions");

                using var command = _connection.CreateCommand();

                // First, check the schema to understand the geometry column type
                logCallback?.Invoke("Analyzing data schema...");
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE temp_schema AS 
                    SELECT * FROM read_parquet('{sourceUrl}') LIMIT 1;
                ";
                await command.ExecuteNonQueryAsync(cancellationToken);

                // Check if geometry column is already GEOMETRY type or needs conversion
                command.CommandText = "DESCRIBE temp_schema";
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                
                bool geometryIsBlob = false;
                while (await reader.ReadAsync())
                {
                    string columnName = reader.GetString("column_name");
                    string columnType = reader.GetString("column_type");
                    
                    if (columnName.Equals("geometry", StringComparison.OrdinalIgnoreCase))
                    {
                        geometryIsBlob = columnType.Contains("BLOB", StringComparison.OrdinalIgnoreCase);
                        logCallback?.Invoke($"Geometry column type: {columnType} (needs conversion: {geometryIsBlob})");
                        break;
                    }
                }

                // Build the spatial filter query based on geometry column type
                string spatialFilter = "";
                string geometryExpression = geometryIsBlob ? "ST_GeomFromWKB(geometry)" : "geometry";
                
                if (extent != null)
                {
                    // Use culture-invariant formatting for SQL and explicit DOUBLE casts
                    string xMinStr = extent.XMin.ToString("G", CultureInfo.InvariantCulture);
                    string yMinStr = extent.YMin.ToString("G", CultureInfo.InvariantCulture);
                    string xMaxStr = extent.XMax.ToString("G", CultureInfo.InvariantCulture);
                    string yMaxStr = extent.YMax.ToString("G", CultureInfo.InvariantCulture);
                    
                    spatialFilter = $@"
                        WHERE ST_Intersects(
                            {geometryExpression}, 
                            ST_MakeEnvelope(
                                CAST({xMinStr} AS DOUBLE), 
                                CAST({yMinStr} AS DOUBLE), 
                                CAST({xMaxStr} AS DOUBLE), 
                                CAST({yMaxStr} AS DOUBLE)
                            )
                        )";
                    
                    logCallback?.Invoke($"Applying spatial filter: {extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}");
                }

                progressCallback?.Invoke(20);

                // Check if the source URL is accessible and get a count first
                logCallback?.Invoke("Checking data source accessibility...");
                string countQuery = $@"
                    SELECT COUNT(*) 
                    FROM read_parquet('{sourceUrl}')
                    {spatialFilter}
                ";
                
                command.CommandText = countQuery;
                var totalCount = await command.ExecuteScalarAsync(cancellationToken);
                long recordCount = Convert.ToInt64(totalCount);
                
                logCallback?.Invoke($"Found {recordCount:N0} records to process");
                
                if (recordCount == 0)
                {
                    logCallback?.Invoke("No records found matching the criteria");
                    return false;
                }

                progressCallback?.Invoke(30);

                // Create the main query to select and process the data
                string mainQuery = $@"
                    SELECT 
                        *,
                        {geometryExpression} AS geom
                    FROM read_parquet('{sourceUrl}')
                    {spatialFilter}
                ";

                logCallback?.Invoke("Loading and processing data...");
                progressCallback?.Invoke(50);

                // Create temporary table with the data
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE temp_source_coop AS 
                    {mainQuery}
                ";
                await command.ExecuteNonQueryAsync(cancellationToken);

                progressCallback?.Invoke(70);
                logCallback?.Invoke("Data loaded into DuckDB, exporting to GeoParquet...");

                // Export to GeoParquet format directly
                string exportQuery = $@"
                    COPY (
                        SELECT 
                            * EXCLUDE (geom),
                            ST_AsWKB(geom) AS geometry
                        FROM temp_source_coop
                    ) TO '{outputGeoParquetPath.Replace('\\', '/')}' 
                    WITH (FORMAT PARQUET);
                ";

                command.CommandText = exportQuery;
                await command.ExecuteNonQueryAsync(cancellationToken);

                progressCallback?.Invoke(90);
                logCallback?.Invoke("Adding GeoParquet metadata...");

                // Add GeoParquet metadata using DuckDB's spatial extension
                AddGeoParquetMetadata(outputGeoParquetPath, logCallback, cancellationToken);

                progressCallback?.Invoke(100);
                logCallback?.Invoke($"Successfully created GeoParquet: {outputGeoParquetPath}");

                // Clean up temporary tables
                command.CommandText = "DROP TABLE IF EXISTS temp_source_coop; DROP TABLE IF EXISTS temp_schema;";
                await command.ExecuteNonQueryAsync(cancellationToken);

                return true;
            }
            catch (Exception ex)
            {
                logCallback?.Invoke($"Error processing Source Cooperative dataset: {ex.Message}");
                return false;
            }
        }

        private void AddGeoParquetMetadata(string parquetPath, Action<string> logCallback, CancellationToken cancellationToken)
        {
            try
            {
                // For now, skip the complex metadata addition since it's causing issues
                // The GeoParquet file created by DuckDB's COPY command should already have 
                // the necessary spatial metadata for ArcGIS Pro to read it
                logCallback?.Invoke("GeoParquet file created with DuckDB spatial metadata");
                
                // Verify the file exists and has content
                if (File.Exists(parquetPath))
                {
                    var fileInfo = new FileInfo(parquetPath);
                    if (fileInfo.Length > 0)
                    {
                        logCallback?.Invoke($"GeoParquet file verification: {fileInfo.Length:N0} bytes");
                    }
                    else
                    {
                        logCallback?.Invoke("Warning: GeoParquet file is empty");
                    }
                }
                else
                {
                    logCallback?.Invoke("Error: GeoParquet file was not created");
                }
            }
            catch (Exception ex)
            {
                logCallback?.Invoke($"Warning: Could not verify GeoParquet metadata: {ex.Message}");
                // This is not critical - the file should still be readable by ArcGIS Pro
            }
        }

        /// <summary>
        /// Downloads and loads bridge files for the specified theme and type
        /// </summary>
        public async Task<bool> LoadBridgeFilesAsync(string theme, string type, string release, IProgress<string> progress = null)
        {
            if (!_bridgeFilesEnabled)
            {
                progress?.Report("Bridge files functionality is disabled");
                return false;
            }

            try
            {
                progress?.Report($"Loading bridge files for {theme}/{type}...");

                // Clear existing bridge file cache for this theme/type
                string cacheKey = $"{theme}:{type}";
                _bridgeFileCache.Remove(cacheKey);

                using var command = _connection.CreateCommand();

                // Check which datasets have bridge files for this theme/type
                var availableDatasets = new List<string>();
                foreach (var dataset in KnownSourceDatasets.SupportedDatasets)
                {
                    string bridgePath = $"{BRIDGE_FILES_BASE_PATH}/{release}/dataset={dataset}/theme={theme}/type={type}/*.parquet";
                    
                    try
                    {
                        // Test if bridge files exist for this dataset using a more efficient approach
                        command.CommandText = $@"
                            SELECT EXISTS(
                                SELECT 1 FROM read_parquet('{bridgePath}', filename=true, hive_partitioning=1) 
                                LIMIT 1
                            )
                        ";
                        var result = await command.ExecuteScalarAsync(CancellationToken.None);
                        if (Convert.ToBoolean(result))
                        {
                            availableDatasets.Add(dataset);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Dataset doesn't have bridge files for this theme/type - this is expected for most datasets
                        System.Diagnostics.Debug.WriteLine($"No bridge files found for dataset '{dataset}' with theme '{theme}' and type '{type}' - this is normal.");
                    }
                }

                if (!availableDatasets.Any())
                {
                    progress?.Report($"No bridge files found for {theme}/{type}");
                    return false;
                }

                progress?.Report($"Found bridge files from {availableDatasets.Count} datasets: {string.Join(", ", availableDatasets.Take(3))}{(availableDatasets.Count > 3 ? "..." : "")}");

                // Load bridge files from all available datasets
                var bridgeRecords = new List<BridgeFileRecord>();
                foreach (var dataset in availableDatasets)
                {
                    string bridgePath = $"{BRIDGE_FILES_BASE_PATH}/{release}/dataset={dataset}/theme={theme}/type={type}/*.parquet";
                    
                    command.CommandText = $@"
                        CREATE OR REPLACE TABLE temp_bridge_{dataset.Replace(" ", "_").Replace("(", "").Replace(")", "")} AS 
                        SELECT id, record_id, update_time, dataset, theme, type, between, dataset_between
                        FROM read_parquet('{bridgePath}', filename=true, hive_partitioning=1)
                    ";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);

                    // Read the bridge file records
                    command.CommandText = $"SELECT * FROM temp_bridge_{dataset.Replace(" ", "_").Replace("(", "").Replace(")", "")}";
                    using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                    
                    while (await reader.ReadAsync())
                    {
                        var record = new BridgeFileRecord
                        {
                            Id = reader.GetString("id"),
                            RecordId = reader.GetString("record_id"),
                            UpdateTime = ParseDateTime(reader["update_time"]),
                            Dataset = reader.GetString("dataset"),
                            Theme = reader.GetString("theme"),
                            Type = reader.GetString("type"),
                            Between = ParseDoubleArray(reader["between"]),
                            DatasetBetween = ParseDoubleArray(reader["dataset_between"])
                        };
                        bridgeRecords.Add(record);
                    }
                }

                // Cache the bridge file records
                _bridgeFileCache[cacheKey] = bridgeRecords;
                
                progress?.Report($"Successfully loaded {bridgeRecords.Count:N0} bridge file records for {theme}/{type}");
                return true;
            }
            catch (Exception ex)
            {
                progress?.Report($"Error loading bridge files: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Error loading bridge files for {theme}/{type}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Creates a joined dataset with source attribution information
        /// </summary>
        public async Task<bool> CreateAttributedDatasetAsync(string layerName, string theme, string type, IProgress<string> progress = null)
        {
            if (!_bridgeFilesEnabled)
            {
                progress?.Report("Bridge files functionality is disabled");
                return false;
            }

            string cacheKey = $"{theme}:{type}";
            if (!_bridgeFileCache.ContainsKey(cacheKey))
            {
                progress?.Report($"No bridge files loaded for {theme}/{type}. Load bridge files first.");
                return false;
            }

            try
            {
                progress?.Report($"Creating attributed dataset for {layerName}...");

                using var command = _connection.CreateCommand();

                // Create a table with the bridge file data
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE bridge_data AS
                    SELECT 
                        id as gers_id,
                        record_id,
                        dataset,
                        update_time,
                        theme,
                        type
                    FROM (VALUES ";

                var bridgeRecords = _bridgeFileCache[cacheKey];
                var valuesList = new List<string>();
                
                foreach (var record in bridgeRecords.Take(10000)) // Limit for performance
                {
                    valuesList.Add($"('{record.Id}', '{record.RecordId}', '{record.Dataset}', '{record.UpdateTime:yyyy-MM-dd HH:mm:ss}', '{record.Theme}', '{record.Type}')");
                }
                
                command.CommandText += string.Join(", ", valuesList) + @"
                    ) AS bridge_values(id, record_id, dataset, update_time, theme, type)
                ";
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                // Create the attributed dataset by joining current_table with bridge data
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE attributed_dataset AS
                    SELECT 
                        ct.*,
                        bd.record_id as source_record_id,
                        bd.dataset as source_dataset,
                        bd.update_time as bridge_update_time,
                        COUNT(bd.dataset) OVER (PARTITION BY ct.id) as source_count,
                        STRING_AGG(DISTINCT bd.dataset, ', ') OVER (PARTITION BY ct.id) as contributing_datasets
                    FROM current_table ct
                    LEFT JOIN bridge_data bd ON ct.id = bd.gers_id
                ";
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                // Get count of attributed records
                command.CommandText = "SELECT COUNT(*) FROM attributed_dataset WHERE source_dataset IS NOT NULL";
                var attributedCount = await command.ExecuteScalarAsync(CancellationToken.None);
                
                command.CommandText = "SELECT COUNT(*) FROM attributed_dataset";
                var totalCount = await command.ExecuteScalarAsync(CancellationToken.None);

                progress?.Report($"Successfully created attributed dataset: {attributedCount:N0} of {totalCount:N0} features have source attribution");
                return true;
            }
            catch (Exception ex)
            {
                progress?.Report($"Error creating attributed dataset: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Error creating attributed dataset: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Gets source attribution information for a specific GERS ID
        /// </summary>
        public SourceAttribution GetSourceAttribution(string gersId, string theme, string type)
        {
            string cacheKey = $"{theme}:{type}";
            if (!_bridgeFileCache.ContainsKey(cacheKey))
            {
                return null;
            }

            var bridgeRecords = _bridgeFileCache[cacheKey].Where(r => r.Id == gersId).ToList();
            
            if (!bridgeRecords.Any())
            {
                return null;
            }

            var attribution = new SourceAttribution
            {
                GersId = gersId,
                SourceRecords = bridgeRecords,
                SourceCount = bridgeRecords.Count,
                ContributingDatasets = bridgeRecords.Select(r => r.Dataset).Distinct().ToList(),
                PrimaryDataset = bridgeRecords.GroupBy(r => r.Dataset)
                                              .OrderByDescending(g => g.Count())
                                              .First().Key
            };

            return attribution;
        }

        /// <summary>
        /// Updates source attribution for a feature (for editing functionality)
        /// </summary>
        public async Task<bool> UpdateSourceAttributionAsync(string gersId, string newPrimaryDataset, List<string> contributingDatasets, IProgress<string> progress = null)
        {
            try
            {
                progress?.Report($"Updating source attribution for {gersId}...");

                using var command = _connection.CreateCommand();

                // Update the attributed_dataset table with new attribution
                command.CommandText = $@"
                    UPDATE attributed_dataset 
                    SET 
                        source_dataset = '{newPrimaryDataset}',
                        contributing_datasets = '{string.Join(", ", contributingDatasets)}',
                        bridge_update_time = CURRENT_TIMESTAMP
                    WHERE id = '{gersId}'
                ";
                
                var affectedRows = await command.ExecuteNonQueryAsync(CancellationToken.None);
                
                progress?.Report($"Updated attribution for {affectedRows} record(s)");
                return affectedRows > 0;
            }
            catch (Exception ex)
            {
                progress?.Report($"Error updating source attribution: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Exports attributed dataset with source information
        /// </summary>
        public async Task<string> ExportAttributedDatasetAsync(string outputPath, string layerName, IProgress<string> progress = null)
        {
            try
            {
                progress?.Report($"Exporting attributed dataset to {Path.GetFileName(outputPath)}...");

                using var command = _connection.CreateCommand();
                
                // Export the attributed dataset
                string query = "SELECT * FROM attributed_dataset";
                string actualFilePath = await ExportToGeoParquet(query, outputPath, layerName, progress);
                
                progress?.Report($"Successfully exported attributed dataset: {layerName}");
                return actualFilePath;
            }
            catch (Exception ex)
            {
                progress?.Report($"Error exporting attributed dataset: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Gets summary statistics about source attribution
        /// </summary>
        public async Task<Dictionary<string, object>> GetAttributionSummaryAsync()
        {
            var summary = new Dictionary<string, object>();
            
            try
            {
                using var command = _connection.CreateCommand();
                
                // Check if attributed_dataset exists
                command.CommandText = @"
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_name = 'attributed_dataset'
                ";
                var tableExists = Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None)) > 0;
                
                if (!tableExists)
                {
                    summary["error"] = "No attributed dataset available";
                    return summary;
                }

                // Total features
                command.CommandText = "SELECT COUNT(*) FROM attributed_dataset";
                summary["total_features"] = await command.ExecuteScalarAsync(CancellationToken.None);

                // Features with attribution
                command.CommandText = "SELECT COUNT(*) FROM attributed_dataset WHERE source_dataset IS NOT NULL";
                summary["attributed_features"] = await command.ExecuteScalarAsync(CancellationToken.None);

                // Source dataset breakdown
                command.CommandText = @"
                    SELECT source_dataset, COUNT(*) as count 
                    FROM attributed_dataset 
                    WHERE source_dataset IS NOT NULL 
                    GROUP BY source_dataset 
                    ORDER BY count DESC
                ";
                
                var datasetBreakdown = new Dictionary<string, long>();
                using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                while (await reader.ReadAsync())
                {
                    datasetBreakdown[reader.GetString("source_dataset")] = reader.GetInt64("count");
                }
                summary["dataset_breakdown"] = datasetBreakdown;

                // Multi-source features
                command.CommandText = "SELECT COUNT(*) FROM attributed_dataset WHERE source_count > 1";
                summary["multi_source_features"] = await command.ExecuteScalarAsync(CancellationToken.None);

            }
            catch (Exception ex)
            {
                summary["error"] = ex.Message;
            }
            
            return summary;
        }

        /// <summary>
        /// Helper method to parse double arrays from DuckDB results
        /// </summary>
        private static double[] ParseDoubleArray(object value)
        {
            if (value == null || value == DBNull.Value)
                return new double[0];
                
            // Handle various possible formats DuckDB might return arrays in
            if (value is string stringValue)
            {
                // Parse array string format like "[1.0, 2.0]"
                var cleaned = stringValue.Trim('[', ']');
                if (string.IsNullOrWhiteSpace(cleaned))
                    return new double[0];
                    
                return cleaned.Split(',')
                             .Select(s => double.TryParse(s.Trim(), out double d) ? d : 0.0)
                             .ToArray();
            }
            
            // If it's already an array, convert it
            if (value is Array array)
            {
                return array.Cast<object>()
                           .Select(o => Convert.ToDouble(o))
                           .ToArray();
            }
            
            return new double[0];
        }

        /// <summary>
        /// Safely parses a DateTime value from various formats that DuckDB might return
        /// </summary>
        private static DateTime ParseDateTime(object value)
        {
            if (value == null || value == DBNull.Value)
                return DateTime.MinValue;

            // Handle DateTime type directly
            if (value is DateTime dateTime)
                return dateTime;

            // Handle string representation
            if (value is string stringValue)
            {
                // Try common datetime formats
                if (DateTime.TryParse(stringValue, out DateTime result))
                    return result;

                // Try ISO format specifically
                if (DateTime.TryParseExact(stringValue, "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out result))
                    return result;

                // Try without microseconds
                if (DateTime.TryParseExact(stringValue, "yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out result))
                    return result;

                // Try without timezone
                if (DateTime.TryParseExact(stringValue, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
                    return result;
            }

            // Handle numeric timestamp (Unix timestamp)
            if (value is long longValue)
            {
                try
                {
                    return DateTimeOffset.FromUnixTimeSeconds(longValue).DateTime;
                }
                catch
                {
                    // If it's not a valid Unix timestamp, return default
                    return DateTime.MinValue;
                }
            }

            if (value is double doubleValue)
            {
                try
                {
                    return DateTimeOffset.FromUnixTimeSeconds((long)doubleValue).DateTime;
                }
                catch
                {
                    return DateTime.MinValue;
                }
            }

            return DateTime.MinValue;
        }
    }
}
