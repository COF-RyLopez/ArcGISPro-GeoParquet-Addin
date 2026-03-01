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
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDB.NET.Data;
using DuckDBGeoparquet.Models;

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

        // Columns to drop per dataset type to reduce S3 transfer and decoding cost (only dropped when present)
        private static readonly Dictionary<string, HashSet<string>> ColumnDropMap = new(StringComparer.OrdinalIgnoreCase)
        {
            { "address", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "address_levels", "sources" } },
            { "building", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "building_part", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "connector", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "division", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "local_type", "hierarchies", "capital_division_ids", "capital_of_divisions" } },
            { "division_area", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "infrastructure", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } },
            { "land", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } },
            { "land_cover", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "land_use", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } },
            { "place", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "addresses", "brand", "emails", "phones", "socials", "sources", "websites" } },
            { "segment", new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
                "access_restrictions", "connectors", "destinations", "level_rules",
                "prohibited_transitions", "road_flags", "road_surface", "routes", "sources",
                "speed_limits", "subclass_rules", "width_rules"
              }
            },
            { "water", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } }
        };

        // Default draw order fallback used when a style does not provide explicit draw ranks.
        // Higher rank draws above lower rank.
        private static readonly Dictionary<string, int> DefaultDrawOrderRanks = new(StringComparer.OrdinalIgnoreCase)
        {
            // Point overlays
            { "place:point", 330 },
            { "division:point", 326 },
            { "address:point", 318 },
            { "connector:point", 314 },
            { "infrastructure:point", 312 },
            { "land:point", 308 },
            { "water:point", 306 },

            // Linear networks and boundaries
            { "segment:line", 304 },
            { "division_boundary:line", 300 },
            { "infrastructure:line", 296 },
            { "water:line", 294 },
            { "land_use:line", 292 },
            { "land:line", 290 },

            // Polygonal layers (buildings above polygons, water at the bottom)
            { "building:polygon", 286 },
            { "building_part:polygon", 284 },
            { "infrastructure:polygon", 278 },
            { "division_area:polygon", 276 },
            { "division:polygon", 274 },
            { "land_use:polygon", 272 },
            { "land_cover:polygon", 268 },
            { "land:polygon", 264 },
            { "bathymetry:polygon", 260 },
            { "water:polygon", 256 },

            // Type-only fallbacks
            { "building", 286 },
            { "building_part", 284 },
            { "segment", 304 },
            { "address", 318 },
            { "place", 330 },
            { "water", 256 },
            { "bathymetry", 260 },
        };

        // Add a class-level field to store the current extent
        private dynamic _currentExtent;

        // Fields to store theme context for file path generation
        private string _currentParentS3Theme;
        private string _currentActualS3Type;

        // Collection to store layer information for bulk creation
        private readonly List<LayerCreationInfo> _pendingLayers;
        private string _outputSessionSuffix = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
        private static readonly bool VerbosePerLayerDebugLogging = false;

        /// <summary>
        /// When set, layers added to the map will receive cartographic symbology
        /// matching this style definition instead of default ArcGIS Pro symbology.
        /// </summary>
        public MapStyleDefinition SelectedMapStyle { get; set; }

        public int LastAddedLayerCount { get; private set; }
        public string LastAppliedStyleName { get; private set; } = "default";

        public DataProcessor()
        {
            _connection = new DuckDBConnection("DataSource=:memory:");
            _pendingLayers = new List<LayerCreationInfo>();
        }

        /// <summary>
        /// Starts a new output session so exported parquet file paths are unique per load.
        /// This prevents ArcGIS from reusing stale cache metadata tied to a previous file path.
        /// </summary>
        public void BeginNewOutputSession()
        {
            _outputSessionSuffix = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
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
                        "2. The extensions must match your DuckDB version\n" +
                        "3. Download extensions from https://github.com/duckdb/duckdb/releases\n" +
                        $"4. Current extension search path: {extensionsPath}", ex);
                }

                // Configure DuckDB settings for optimal performance
                // Execute each SET command separately to avoid potential parsing issues
                try
                {
                    command.CommandText = "SET s3_region='us-west-2';";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                    
                    command.CommandText = "SET enable_http_metadata_cache=true;";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                    
                    command.CommandText = "SET enable_object_cache=true;";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                    
                    command.CommandText = "SET enable_progress_bar=true;";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);

                    // Additional performance tuning
                    command.CommandText = "SET memory_limit='2GB';";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);

                    string tempDir = Path.GetTempPath().Replace('\\', '/');
                    command.CommandText = $"SET temp_directory='{tempDir}';";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);

                    System.Diagnostics.Debug.WriteLine("DuckDB S3 and cache settings configured successfully");
                }
                catch (Exception settingsEx)
                {
                    System.Diagnostics.Debug.WriteLine($"Warning: Could not set some DuckDB settings: {settingsEx.Message}");
                    // Continue - these are optimizations, not required for functionality
                }

                // Enable parallelism according to host CPU
                try
                {
                    int threads = Math.Max(1, Environment.ProcessorCount);
                    command.CommandText = $"SET threads={threads};";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                    System.Diagnostics.Debug.WriteLine($"DuckDB threads set to {threads}");
                }
                catch (Exception threadEx)
                {
                    System.Diagnostics.Debug.WriteLine($"Warning: Could not set DuckDB threads: {threadEx.Message}");
                }

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

                // Create a fresh command for this operation to ensure clean state
                using var command = _connection.CreateCommand();
                
                // Ensure command text is empty before setting new query
                command.CommandText = string.Empty;
                
                string schemaQuery = $@"
                    CREATE OR REPLACE TABLE temp AS 
                    SELECT * FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1) LIMIT 0;
                ";
                command.CommandText = schemaQuery;
                System.Diagnostics.Debug.WriteLine($"Executing schema query for {actualS3Type ?? "data"}...");
                try
                {
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error executing schema query: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Query that failed: {schemaQuery.Substring(0, Math.Min(300, schemaQuery.Length))}");
                    System.Diagnostics.Debug.WriteLine($"Connection state: {_connection.State}");
                    throw;
                }

                command.CommandText = "DESCRIBE temp";
                var columnNames = new List<string>();
                using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                // Collect column names for projection and count for validation
                while (await reader.ReadAsync())
                {
                    var colName = reader.GetString(0);
                    columnNames.Add(colName);
                }
                System.Diagnostics.Debug.WriteLine($"Schema validated: {columnNames.Count} columns found");

                // Build a selective column list to trim heavy structs/arrays when safe
                var projectedColumns = BuildColumnProjection(actualS3Type, columnNames);

                progress?.Report($"Loading data from S3 (this may take a moment)...");

                string spatialFilter = "";
                string extentPolygon = "";
                if (extent != null)
                {
                    // Use culture-invariant formatting for SQL to prevent German locale decimal separator issues
                    string xMinStr = ((double)extent.XMin).ToString("G", CultureInfo.InvariantCulture);
                    string yMinStr = ((double)extent.YMin).ToString("G", CultureInfo.InvariantCulture);
                    string xMaxStr = ((double)extent.XMax).ToString("G", CultureInfo.InvariantCulture);
                    string yMaxStr = ((double)extent.YMax).ToString("G", CultureInfo.InvariantCulture);
                    
                    // Create a polygon from the extent for clipping
                    // Format: POLYGON((xmin ymin, xmax ymin, xmax ymax, xmin ymax, xmin ymin))
                    extentPolygon = $"ST_GeomFromText('POLYGON(({xMinStr} {yMinStr}, {xMaxStr} {yMinStr}, {xMaxStr} {yMaxStr}, {xMinStr} {yMaxStr}, {xMinStr} {yMinStr}))')";

                    // Bbox overlap first (pushdown), then ST_Intersects so only features whose geometry
                    // actually intersects the extent are kept — prevents data extending beyond the frame.
                    spatialFilter = $@"
                        WHERE bbox.xmin <= {xMaxStr}
                          AND bbox.xmax >= {xMinStr}
                          AND bbox.ymin <= {yMaxStr}
                          AND bbox.ymax >= {yMinStr}
                          AND ST_Intersects(geometry, {extentPolygon})";
                    System.Diagnostics.Debug.WriteLine($"[{actualS3Type}] Spatial filter: requested extent=({xMinStr}, {yMinStr}) to ({xMaxStr}, {yMaxStr}), bbox overlap + ST_Intersects");
                    progress?.Report($"Applying spatial filter for current map extent...");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("WARNING: No extent provided - loading all data without spatial filter!");
                }

                // Build query with optional geometry clipping
                string query;
                // Determine which columns to project (if any) while always retaining geometry
                var projectedColumnList = projectedColumns ?? "*";
                bool hasBboxColumn = columnNames.Any(c => c.Equals(BBOX_COLUMN, StringComparison.OrdinalIgnoreCase));
                var projectedColumnsWithoutGeometry = projectedColumns != null
                    ? string.Join(", ", projectedColumns.Split(',').Select(c => c.Trim()).Where(c => !c.Equals("geometry", StringComparison.OrdinalIgnoreCase)))
                    : "* EXCLUDE(geometry)";
                var projectedColumnsWithoutGeometryOrBbox = projectedColumns != null
                    ? string.Join(", ", projectedColumns.Split(',').Select(c => c.Trim()).Where(c =>
                        !c.Equals("geometry", StringComparison.OrdinalIgnoreCase) &&
                        !c.Equals(BBOX_COLUMN, StringComparison.OrdinalIgnoreCase)))
                    : (hasBboxColumn ? "* EXCLUDE(geometry, bbox)" : "* EXCLUDE(geometry)");
                if (string.IsNullOrWhiteSpace(projectedColumnsWithoutGeometry))
                    projectedColumnsWithoutGeometry = "* EXCLUDE(geometry)";
                if (string.IsNullOrWhiteSpace(projectedColumnsWithoutGeometryOrBbox))
                    projectedColumnsWithoutGeometryOrBbox = hasBboxColumn ? "* EXCLUDE(geometry, bbox)" : "* EXCLUDE(geometry)";

                if (extent != null && !string.IsNullOrEmpty(extentPolygon))
                {
                    // Clip geometries to extent - this keeps all intersecting features but clips them to the extent
                    // Use ST_Intersection to clip, and only include features that actually intersect
                    if (hasBboxColumn)
                    {
                        query = $@"
                        CREATE OR REPLACE TABLE current_table AS 
                        WITH clipped AS (
                            SELECT
                                {projectedColumnsWithoutGeometryOrBbox},
                                CASE
                                    WHEN ST_Intersects(geometry, {extentPolygon})
                                    THEN ST_Intersection(geometry, {extentPolygon})
                                    ELSE NULL
                                END AS clipped_geometry
                            FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                            {spatialFilter}
                        )
                        SELECT
                            * EXCLUDE(clipped_geometry),
                            clipped_geometry AS geometry,
                            CASE
                                WHEN clipped_geometry IS NOT NULL THEN struct_pack(
                                    xmin := ST_XMin(clipped_geometry),
                                    ymin := ST_YMin(clipped_geometry),
                                    xmax := ST_XMax(clipped_geometry),
                                    ymax := ST_YMax(clipped_geometry)
                                )
                                ELSE NULL
                            END AS bbox
                        FROM clipped";
                    }
                    else
                    {
                        query = $@"
                        CREATE OR REPLACE TABLE current_table AS 
                        SELECT 
                            {projectedColumnsWithoutGeometry},
                            CASE 
                                WHEN ST_Intersects(geometry, {extentPolygon}) 
                                THEN ST_Intersection(geometry, {extentPolygon})
                                ELSE NULL
                            END as geometry
                        FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                        {spatialFilter}";
                    }
                }
                else
                {
                    query = $@"
                        CREATE OR REPLACE TABLE current_table AS 
                        SELECT {projectedColumnList}
                        FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                        {spatialFilter}";
                }
                
                // Clear command text before setting new query to ensure clean state
                command.CommandText = string.Empty;
                command.CommandText = query;
                
                System.Diagnostics.Debug.WriteLine($"Executing data load query for {actualS3Type ?? "data"}...");
                try
                {
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error executing data load query: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Query that failed: {query.Substring(0, Math.Min(300, query.Length))}");
                    System.Diagnostics.Debug.WriteLine($"Connection state: {_connection.State}");
                    throw;
                }

                command.CommandText = "SELECT COUNT(*) FROM current_table";
                var count = await command.ExecuteScalarAsync(CancellationToken.None);
                progress?.Report($"Successfully loaded {count:N0} rows from S3");
                System.Diagnostics.Debug.WriteLine($"[{actualS3Type}] Loaded {count} rows");

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

        /// <summary>
        /// Infer ISO 3166-1 alpha-2 country code from extent center (WGS84) for filtering address data.
        /// Returns null if extent center is not in a known bounding box. Reduces address load from global to one country.
        /// </summary>
        private static string TryGetCountryCodeFromExtent(dynamic extent)
        {
            if (extent == null) return null;
            double midLon = (double)((extent.XMin + extent.XMax) / 2.0);
            double midLat = (double)((extent.YMin + extent.YMax) / 2.0);
            // Rough bounding boxes (lon min, lat min, lon max, lat max) for countries with large Overture address counts
            if (midLon >= -125 && midLon <= -66 && midLat >= 24 && midLat <= 50) return "US";
            if (midLon >= -141 && midLon <= -52 && midLat >= 41 && midLat <= 84) return "CA";
            if (midLon >= -118 && midLon <= -86 && midLat >= 14 && midLat <= 33) return "MX";
            if (midLon >= -75 && midLon <= -34 && midLat >= -34 && midLat <= 5) return "BR";
            if (midLon >= -5 && midLon <= 10 && midLat >= 41 && midLat <= 52) return "FR";
            if (midLon >= -10 && midLon <= 5 && midLat >= 50 && midLat <= 61) return "GB";
            if (midLon >= 6 && midLon <= 15 && midLat >= 47 && midLat <= 55) return "DE";
            if (midLon >= 10 && midLon <= 19 && midLat >= 36 && midLat <= 47) return "IT";
            if (midLon >= -4 && midLon <= 5 && midLat >= 35 && midLat <= 44) return "ES";
            if (midLon >= 124 && midLon <= 154 && midLat >= -44 && midLat <= -10) return "AU";
            if (midLon >= 165 && midLon <= 180 && midLat >= -48 && midLat <= -34) return "NZ";
            if (midLon >= -180 && midLon <= -165 && midLat >= -48 && midLat <= -34) return "NZ";
            return null;
        }

        /// <summary>
        /// Build a projection list that drops known heavy optional columns for the given dataset type.
        /// Returns null to indicate "SELECT *" when no drops apply.
        /// </summary>
        private static string BuildColumnProjection(string actualS3Type, List<string> columnNames)
        {
            if (string.IsNullOrWhiteSpace(actualS3Type) || columnNames == null || columnNames.Count == 0)
                return null;

            if (!ColumnDropMap.TryGetValue(actualS3Type, out var dropSet) || dropSet.Count == 0)
                return null;

            var projected = columnNames
                .Where(name =>
                    // Never drop geometry or bbox even if listed
                    name.Equals(GEOMETRY_COLUMN, StringComparison.OrdinalIgnoreCase) ||
                    name.StartsWith($"{BBOX_COLUMN}_", StringComparison.OrdinalIgnoreCase) ||
                    !dropSet.Contains(name))
                .ToList();

            // If projection would drop everything (unlikely), fall back to *
            if (!projected.Any())
                return null;

            return string.Join(", ", projected);
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

        /// <summary>
        /// Gets the schema information for a GeoParquet file from S3.
        /// Returns a list of dictionaries with column_name and column_type.
        /// This method follows the Overture Maps schema conventions.
        /// </summary>
        /// <param name="s3Path">S3 path to the GeoParquet file(s)</param>
        /// <returns>List of dictionaries containing column_name and column_type</returns>
        public async Task<List<Dictionary<string, object>>> GetSchemaAsync(string s3Path)
        {
            try
            {
                // Use the same approach as IngestFileAsync for consistency
                using var command = _connection.CreateCommand();
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE temp_schema AS 
                    SELECT * FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1) LIMIT 0;
                ";
                await command.ExecuteNonQueryAsync(CancellationToken.None);

                // Use ExecuteQueryAsync to get schema results
                var results = await ExecuteQueryAsync("DESCRIBE temp_schema");
                
                System.Diagnostics.Debug.WriteLine($"Schema discovery: Found {results.Count} columns for {s3Path}");
                return results;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error getting schema for {s3Path}: {ex.Message}");
                throw new Exception($"Failed to get schema: {ex.Message}", ex);
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
        public async Task CreateFeatureLayerAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot, string compression = "ZSTD")
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
                await ExportByGeometryType(layerNameBase, themeTypeSpecificFolder, progress, compression);

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

        private async Task ExportByGeometryType(string layerNameBase, string themeTypeSpecificOutputFolder, IProgress<string> progress = null, string compression = "ZSTD")
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

            // Determine whether current_table has a bbox column so we can keep bbox synchronized with geometry.
            command.CommandText = "SELECT COUNT(*) FROM pragma_table_info('current_table') WHERE lower(name) = 'bbox'";
            var bboxColCount = Convert.ToInt32(await command.ExecuteScalarAsync(CancellationToken.None));
            bool currentTableHasBbox = bboxColCount > 0;

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
                string outputFileName = $"{MfcUtility.SanitizeFileName(finalLayerName)}_{_outputSessionSuffix}.parquet";
                string finalOutputPath = Path.Combine(themeTypeSpecificOutputFolder, outputFileName);

                try
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(finalOutputPath));

                    string escapedGeomType = geomType.Replace("'", "''");
                    string exportSelect = currentTableHasBbox
                        ? $@"
                            SELECT
                                * EXCLUDE(geometry, bbox),
                                geometry,
                                struct_pack(
                                    xmin := ST_XMin(geometry),
                                    ymin := ST_YMin(geometry),
                                    xmax := ST_XMax(geometry),
                                    ymax := ST_YMax(geometry)
                                ) AS bbox
                            FROM current_table
                            WHERE geometry IS NOT NULL
                              AND ST_GeometryType(geometry) = '{escapedGeomType}'"
                        : $@"
                            SELECT
                                * EXCLUDE(geometry),
                                geometry
                            FROM current_table
                            WHERE geometry IS NOT NULL
                              AND ST_GeometryType(geometry) = '{escapedGeomType}'";

                    string targetPath = await ExportToGeoParquet(exportSelect, finalOutputPath, finalLayerName, progress, compression);

                    var layerInfo = new LayerCreationInfo
                    {
                        FilePath = targetPath,
                        LayerName = finalLayerName,
                        GeometryType = geomType,
                        StackingPriority = geometryTypeOrder.TryGetValue(geomType, out int priority) ? priority : 99,
                        ParentTheme = _currentParentS3Theme,
                        ActualType = _currentActualS3Type
                    };
                    _pendingLayers.Add(layerInfo);

                    progress?.Report($"Prepared layer {finalLayerName} for optimal stacking");
                    System.Diagnostics.Debug.WriteLine($"Prepared layer: {finalLayerName} at {finalOutputPath}");
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error exporting {geomType}: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Error exporting {geomType}: {ex.Message}");
                }
            }

            progress?.Report($"Finished processing all geometry types for {layerNameBase}.");
        }

        private async Task<string> ExportToGeoParquet(string query, string outputPath, string _layerName, IProgress<string> progress = null, string compression = "ZSTD")
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
                    // Create temp file with .parquet extension so ArcGIS Pro can recognize it
                    // Use format: filename_timestamp.parquet instead of filename.parquet.tmp_timestamp
                    string directory = Path.GetDirectoryName(outputPath);
                    string fileNameWithoutExt = Path.GetFileNameWithoutExtension(outputPath);
                    string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                    actualOutputPath = Path.Combine(directory, $"{fileNameWithoutExt}_{timestamp}.parquet");
                    System.Diagnostics.Debug.WriteLine($"File still locked, using temporary export path: {actualOutputPath}");
                }
                
                command.CommandText = BuildGeoParquetCopyCommand(query, actualOutputPath, compression);
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
                            // Temp file has .parquet extension so it can be used as a layer
                            // Update the output path to point to the temp file for layer creation
                            outputPath = actualOutputPath;
                            System.Diagnostics.Debug.WriteLine($"Using temp file as final output: {outputPath}");
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

        private static string BuildGeoParquetCopyCommand(string selectQuery, string outputPath, string compression = "ZSTD")
        {
            // Validate compression type
            string validCompression = compression?.ToUpperInvariant() switch
            {
                "SNAPPY" => "SNAPPY",
                "GZIP" => "GZIP",
                "ZSTD" => "ZSTD",
                _ => "ZSTD" // Default to ZSTD if invalid
            };

            // Optimized row group size: 128MB target (approximately 100k-200k rows depending on data)
            // Larger row groups improve compression and read performance, but increase memory usage
            // 100000 is a good balance for most GeoParquet datasets
            // For very large datasets, consider increasing to 200000-500000
            int rowGroupSize = 100000;

            return $@"
                COPY (
                    {selectQuery}
                ) TO '{outputPath.Replace('\\', '/')}' 
                WITH (
                    FORMAT 'PARQUET',
                    ROW_GROUP_SIZE {rowGroupSize},
                    COMPRESSION '{validCompression}'
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
                LastAddedLayerCount = 0;
                progress?.Report("No layers to add to map.");
                return;
            }

            var sortedLayers = SortLayersByGeometryPriority(_pendingLayers, SelectedMapStyle);

            progress?.Report($"Preparing to add {sortedLayers.Count} layers with optimal stacking order...");
            string styleName = SelectedMapStyle?.DisplayName ?? "default";
            LastAddedLayerCount = sortedLayers.Count;
            LastAppliedStyleName = styleName;
            System.Diagnostics.Debug.WriteLine($"Adding {sortedLayers.Count} layers using style-aware stacking order ({styleName}):");
            System.Diagnostics.Debug.WriteLine(
                $"Draw order rank source: style map count={SelectedMapStyle?.DrawOrderRanks?.Count ?? 0}, fallback map count={DefaultDrawOrderRanks.Count}");

            if (VerbosePerLayerDebugLogging)
            {
                foreach (var layer in sortedLayers)
                {
                    int drawRank = GetLayerDrawingRank(layer, SelectedMapStyle);
                    string resolvedType = ResolveLayerType(layer);
                    System.Diagnostics.Debug.WriteLine(
                        $"  {layer.LayerName} ({layer.GeometryType}, type={resolvedType ?? "unknown"}, draw rank {drawRank}, base priority {layer.StackingPriority})");
                }
            }

            int pointCount = sortedLayers.Count(l => l.StackingPriority == 3);
            int lineCount = sortedLayers.Count(l => l.StackingPriority == 2);
            int polygonCount = sortedLayers.Count(l => l.StackingPriority == 1);
            progress?.Report($"Layer summary: {pointCount} point layers, {lineCount} line layers, {polygonCount} polygon layers");

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

                        missingLayersForIndividualCreation = AddLayerBatch(map, sortedLayers, progress);
                    });

                    if (missingLayersForIndividualCreation?.Any() == true)
                    {
                        await FallbackToIndividualLayerCreation(missingLayersForIndividualCreation, progress);
                    }
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error during bulk layer creation: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Error in AddAllLayersToMapAsync: {ex.Message}");

                    progress?.Report("Falling back to individual layer creation...");
                    await FallbackToIndividualLayerCreation(sortedLayers, progress);
                }
            }

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map != null)
                {
                    EnforceLayerDrawOrder(map, sortedLayers, progress);
                }
            });

            _pendingLayers.Clear();
        }

        private static List<LayerCreationInfo> SortLayersByGeometryPriority(List<LayerCreationInfo> layers, MapStyleDefinition style)
        {
            return layers
                .OrderByDescending(layer => GetLayerDrawingRank(layer, style))
                .ThenBy(layer => layer.ParentTheme)
                .ThenBy(layer => layer.ActualType)
                .ThenBy(layer => layer.LayerName)
                .ToList();
        }

        private static int GetLayerDrawingRank(LayerCreationInfo layerInfo, MapStyleDefinition style)
        {
            if (layerInfo == null)
                return 0;

            string actualType = ResolveLayerType(layerInfo);
            string geometryGroup = GetGeometryGroup(layerInfo.GeometryType);
            var styleOrder = style?.DrawOrderRanks;
            string exactKey = string.IsNullOrWhiteSpace(actualType) ? null : $"{actualType}:{geometryGroup}";

            if (!string.IsNullOrWhiteSpace(exactKey))
            {
                if (styleOrder != null && styleOrder.TryGetValue(exactKey, out int exactStyleRank))
                    return exactStyleRank;

                if (DefaultDrawOrderRanks.TryGetValue(exactKey, out int exactFallbackRank))
                    return exactFallbackRank;
            }

            if (!string.IsNullOrWhiteSpace(actualType))
            {
                if (styleOrder != null && styleOrder.TryGetValue(actualType, out int typeStyleRank))
                    return typeStyleRank;

                if (DefaultDrawOrderRanks.TryGetValue(actualType, out int typeFallbackRank))
                    return typeFallbackRank;
            }

            // Final geometry-based fallback with explicit separation to avoid same-rank tie churn.
            return geometryGroup switch
            {
                "point" => 300,
                "line" => 200,
                _ => 100
            };
        }

        private static string ResolveLayerType(LayerCreationInfo layerInfo)
        {
            if (layerInfo == null)
                return null;

            if (!string.IsNullOrWhiteSpace(layerInfo.ActualType))
                return layerInfo.ActualType.Trim().ToLowerInvariant();

            if (!string.IsNullOrWhiteSpace(layerInfo.ParentTheme))
                return layerInfo.ParentTheme.Trim().ToLowerInvariant();

            string layerName = layerInfo.LayerName?.ToLowerInvariant() ?? string.Empty;
            if (layerName.Contains("building part"))
                return "building_part";
            if (layerName.Contains("building"))
                return "building";
            if (layerName.Contains("land use"))
                return "land_use";
            if (layerName.Contains("land cover"))
                return "land_cover";
            if (layerName.Contains("division boundary"))
                return "division_boundary";
            if (layerName.Contains("division area"))
                return "division_area";
            if (layerName.Contains("infrastructure"))
                return "infrastructure";
            if (layerName.Contains("bathymetry"))
                return "bathymetry";
            if (layerName.Contains("water"))
                return "water";
            if (layerName.Contains("land"))
                return "land";
            if (layerName.Contains("segment"))
                return "segment";
            if (layerName.Contains("connector"))
                return "connector";
            if (layerName.Contains("address"))
                return "address";
            if (layerName.Contains("place"))
                return "place";
            if (layerName.Contains("division"))
                return "division";

            return null;
        }

        private static string GetGeometryGroup(string geometryType)
        {
            if (string.IsNullOrWhiteSpace(geometryType))
                return "polygon";

            if (geometryType.Equals("POINT", StringComparison.OrdinalIgnoreCase) ||
                geometryType.Equals("MULTIPOINT", StringComparison.OrdinalIgnoreCase))
                return "point";

            if (geometryType.Equals("LINESTRING", StringComparison.OrdinalIgnoreCase) ||
                geometryType.Equals("MULTILINESTRING", StringComparison.OrdinalIgnoreCase))
                return "line";

            return "polygon";
        }

        private List<LayerCreationInfo> AddLayerBatch(Map map, List<LayerCreationInfo> sortedLayers, IProgress<string> progress)
        {
            progress?.Report("Validating layer files...");

            var uris = new List<Uri>();
            var layerNames = new List<string>();
            var validLayerInfos = new List<LayerCreationInfo>();
            int validFiles = 0;

            foreach (var layerInfo in sortedLayers)
            {
                if (File.Exists(layerInfo.FilePath))
                {
                    uris.Add(new Uri(layerInfo.FilePath));
                    layerNames.Add(layerInfo.LayerName);
                    validLayerInfos.Add(layerInfo);
                    validFiles++;
                    if (VerbosePerLayerDebugLogging)
                    {
                        System.Diagnostics.Debug.WriteLine($"Valid file for {layerInfo.LayerName}: {layerInfo.FilePath}");
                    }
                }
                else
                {
                    progress?.Report($"Warning: File not found for {layerInfo.LayerName}");
                    System.Diagnostics.Debug.WriteLine($"Warning: File not found for layer {layerInfo.LayerName}: {layerInfo.FilePath}");
                }
            }

            progress?.Report($"Validated {validFiles} layer files. Creating layers...");

            if (!uris.Any())
                return null;

            // Defensive cleanup: remove any existing layers/tables referencing the same parquet files
            // before bulk creation. This avoids ArcGIS reusing stale datasource state/extents.
            int removedExisting = RemoveExistingMembersForTargetParquetFiles(map, validLayerInfos);
            if (removedExisting > 0)
            {
                progress?.Report($"Removed {removedExisting} existing map member(s) that used the same output files.");
            }

            progress?.Report("Executing bulk layer creation (this may take a moment)...");

            System.Diagnostics.Debug.WriteLine($"Attempting to create {uris.Count} layers via bulk creation...");
            var layers = LayerFactory.Instance.CreateLayers(uris, map);

            progress?.Report($"Bulk creation completed. Applying settings to {layers.Count} layers...");
            System.Diagnostics.Debug.WriteLine($"Bulk creation result: {layers.Count} layers created from {uris.Count} URIs");

            for (int i = 0; i < layers.Count && i < layerNames.Count; i++)
            {
                if (layers[i] != null)
                {
                    layers[i].SetName(layerNames[i]);

                    if (layers[i] is FeatureLayer featureLayer)
                    {
                        CIMRenderer renderer = null;
                        var layerInfo = i < validLayerInfos.Count ? validLayerInfos[i] : null;
                        if (SelectedMapStyle != null && layerInfo != null)
                        {
                            renderer = CartographyService.CreateRendererForLayer(layerInfo, SelectedMapStyle);
                        }
                        ApplyLayerSettings(featureLayer, renderer, layerInfo, SelectedMapStyle);

                        if (renderer != null)
                        {
                            if (VerbosePerLayerDebugLogging)
                            {
                                System.Diagnostics.Debug.WriteLine($"CartographyService: Applied '{SelectedMapStyle.DisplayName}' style to layer '{featureLayer.Name}' (type={layerInfo.ActualType}, geom={layerInfo.GeometryType})");
                            }
                        }
                    }

                    if (i % 3 == 0 || i == layers.Count - 1)
                    {
                        progress?.Report($"Configured layer {i + 1} of {layers.Count}: {layerNames[i]}");
                    }
                    if (VerbosePerLayerDebugLogging)
                    {
                        System.Diagnostics.Debug.WriteLine($"Successfully added layer: {layerNames[i]}");
                    }
                }
            }

            progress?.Report($"✅ Successfully added {layers.Count} layers with optimal stacking order!");
            System.Diagnostics.Debug.WriteLine($"Bulk layer creation completed. Added {layers.Count} layers.");

            if (layers.Count < uris.Count)
            {
                progress?.Report($"Some layers missing from bulk creation ({layers.Count}/{uris.Count}). Creating missing layers individually...");
                System.Diagnostics.Debug.WriteLine($"Bulk creation incomplete: {layers.Count}/{uris.Count} layers created. Falling back for missing layers...");

                var createdLayerNames = layers.Select(l => l.Name).ToHashSet();
                return sortedLayers.Where(layerInfo =>
                    File.Exists(layerInfo.FilePath) &&
                    !createdLayerNames.Contains(layerInfo.LayerName)).ToList();
            }

            return null;
        }

        /// <summary>
        /// Enforces deterministic top-to-bottom draw order for the newly loaded Overture layers.
        /// This corrects any ordering drift introduced by bulk layer creation.
        /// Must be called on the MCT.
        /// </summary>
        private static void EnforceLayerDrawOrder(Map map, List<LayerCreationInfo> desiredOrder, IProgress<string> progress)
        {
            if (map == null || desiredOrder == null || desiredOrder.Count == 0)
                return;

            // Desired order is top -> bottom.
            var desiredNames = desiredOrder
                .Select(l => l?.LayerName)
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .ToList();

            if (desiredNames.Count == 0)
                return;

            var liveLayerMap = map.Layers
                .Where(l => l != null)
                .GroupBy(l => l.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

            var presentLayers = desiredNames
                .Where(name => liveLayerMap.ContainsKey(name))
                .Select(name => liveLayerMap[name])
                .ToList();

            if (presentLayers.Count <= 1)
                return;

            int targetStartIndex = presentLayers
                .Select(l => map.Layers.IndexOf(l))
                .Where(i => i >= 0)
                .DefaultIfEmpty(0)
                .Min();

            // Move in reverse so final visual order is desired top -> bottom.
            for (int i = desiredNames.Count - 1; i >= 0; i--)
            {
                string layerName = desiredNames[i];
                if (!liveLayerMap.TryGetValue(layerName, out Layer layerToMove) || layerToMove == null)
                    continue;

                map.MoveLayer(layerToMove, targetStartIndex);
            }

            System.Diagnostics.Debug.WriteLine($"Enforced deterministic draw order for {presentLayers.Count} loaded layer(s) starting at index {targetStartIndex}.");
            progress?.Report("Applied deterministic layer draw order");
        }

        private static int RemoveExistingMembersForTargetParquetFiles(Map map, IEnumerable<LayerCreationInfo> targetLayers)
        {
            if (map == null || targetLayers == null)
                return 0;

            var targetLayerList = targetLayers.Where(t => t != null).ToList();
            if (!targetLayerList.Any())
                return 0;

            var targetFilePaths = new HashSet<string>(
                targetLayerList
                    .Select(t => NormalizeToParquetFilePath(t.FilePath))
                    .Where(p => !string.IsNullOrWhiteSpace(p)),
                StringComparer.OrdinalIgnoreCase);

            var targetLayerNames = new HashSet<string>(
                targetLayerList
                    .Select(t => t.LayerName)
                    .Where(n => !string.IsNullOrWhiteSpace(n)),
                StringComparer.OrdinalIgnoreCase);

            var membersToRemove = new List<MapMember>();

            foreach (var layer in map.GetLayersAsFlattenedList().OfType<FeatureLayer>())
            {
                bool removeByPath = false;
                bool pathCheckFailed = false;

                try
                {
                    using var fc = layer.GetFeatureClass();
                    var pathUri = fc?.GetPath();
                    var normalizedLayerPath = NormalizeToParquetFilePath(pathUri != null
                        ? (pathUri.IsFile ? pathUri.LocalPath : pathUri.OriginalString)
                        : null);

                    if (!string.IsNullOrWhiteSpace(normalizedLayerPath) && targetFilePaths.Contains(normalizedLayerPath))
                        removeByPath = true;
                }
                catch (Exception ex)
                {
                    pathCheckFailed = true;
                    System.Diagnostics.Debug.WriteLine($"RemoveExistingMembersForTargetParquetFiles: Failed to inspect layer path for '{layer.Name}': {ex.Message}");
                }

                if (removeByPath || (pathCheckFailed && targetLayerNames.Contains(layer.Name)))
                {
                    membersToRemove.Add(layer);
                }
            }

            foreach (var table in map.GetStandaloneTablesAsFlattenedList().OfType<StandaloneTable>())
            {
                bool removeByPath = false;
                bool pathCheckFailed = false;

                try
                {
                    using var tbl = table.GetTable();
                    var pathUri = tbl?.GetPath();
                    var normalizedTablePath = NormalizeToParquetFilePath(pathUri != null
                        ? (pathUri.IsFile ? pathUri.LocalPath : pathUri.OriginalString)
                        : null);

                    if (!string.IsNullOrWhiteSpace(normalizedTablePath) && targetFilePaths.Contains(normalizedTablePath))
                        removeByPath = true;
                }
                catch (Exception ex)
                {
                    pathCheckFailed = true;
                    System.Diagnostics.Debug.WriteLine($"RemoveExistingMembersForTargetParquetFiles: Failed to inspect table path for '{table.Name}': {ex.Message}");
                }

                if (removeByPath || (pathCheckFailed && targetLayerNames.Contains(table.Name)))
                {
                    membersToRemove.Add(table);
                }
            }

            int removedCount = 0;
            foreach (var member in membersToRemove.Distinct())
            {
                try
                {
                    if (member is Layer layer)
                    {
                        map.RemoveLayer(layer);
                        (layer as IDisposable)?.Dispose();
                        removedCount++;
                        System.Diagnostics.Debug.WriteLine($"Removed existing layer before re-create: {member.Name}");
                    }
                    else if (member is StandaloneTable table)
                    {
                        map.RemoveStandaloneTable(table);
                        (table as IDisposable)?.Dispose();
                        removedCount++;
                        System.Diagnostics.Debug.WriteLine($"Removed existing standalone table before re-create: {member.Name}");
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"RemoveExistingMembersForTargetParquetFiles: Failed removing '{member.Name}': {ex.Message}");
                }
            }

            if (removedCount > 0)
            {
                System.Diagnostics.Debug.WriteLine($"Removed {removedCount} existing map member(s) that matched target parquet paths.");
            }

            return removedCount;
        }

        private static string NormalizeToParquetFilePath(string rawPath)
        {
            if (string.IsNullOrWhiteSpace(rawPath))
                return null;

            try
            {
                string fullPath = Path.GetFullPath(rawPath);
                string lowerPath = fullPath.ToLowerInvariant();
                int parquetIndex = lowerPath.IndexOf(".parquet", StringComparison.OrdinalIgnoreCase);
                if (parquetIndex >= 0)
                {
                    return fullPath[..(parquetIndex + ".parquet".Length)].ToLowerInvariant();
                }

                return fullPath.ToLowerInvariant();
            }
            catch
            {
                return null;
            }
        }

        private async Task AddLayerWithFallback(Map map, LayerCreationInfo layerInfo, IProgress<string> progress)
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

        private async Task AddLayersWithFallbackBatch(List<LayerCreationInfo> layers, IProgress<string> progress)
        {
            if (layers == null || layers.Count == 0)
                return;

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map == null)
                {
                    progress?.Report("Error: No active map found to add layer(s).");
                    System.Diagnostics.Debug.WriteLine("AddLayersWithFallbackBatch: No active map found.");
                    return;
                }

                foreach (var layerInfo in layers)
                {
                    if (!File.Exists(layerInfo.FilePath))
                    {
                        progress?.Report($"Error: Parquet file not found at {layerInfo.FilePath}");
                        System.Diagnostics.Debug.WriteLine($"AddLayersWithFallbackBatch: Parquet file not found at {layerInfo.FilePath}");
                        continue;
                    }

                    try
                    {
                        Uri dataUri = new(layerInfo.FilePath);
                        Layer newLayer = LayerFactory.Instance.CreateLayer(dataUri, map, layerName: layerInfo.LayerName);
                        if (newLayer == null)
                        {
                            progress?.Report($"Error: Could not create layer for {layerInfo.LayerName}.");
                            System.Diagnostics.Debug.WriteLine($"AddLayersWithFallbackBatch: LayerFactory returned null for {layerInfo.FilePath}");
                            continue;
                        }

                        if (newLayer is FeatureLayer featureLayer)
                        {
                            CIMRenderer renderer = null;
                            if (SelectedMapStyle != null)
                            {
                                renderer = CartographyService.CreateRendererForLayer(layerInfo, SelectedMapStyle);
                            }
                            ApplyLayerSettings(featureLayer, renderer, layerInfo, SelectedMapStyle);

                            if (renderer != null)
                            {
                                if (VerbosePerLayerDebugLogging)
                                {
                                    System.Diagnostics.Debug.WriteLine($"CartographyService: Applied '{SelectedMapStyle.DisplayName}' style to layer '{featureLayer.Name}' (type={layerInfo.ActualType}, geom={layerInfo.GeometryType})");
                                }
                            }
                        }

                        progress?.Report($"Successfully added layer: {newLayer?.Name ?? layerInfo.LayerName}");
                        System.Diagnostics.Debug.WriteLine($"AddLayersWithFallbackBatch: Added layer {newLayer?.Name ?? layerInfo.LayerName}");
                    }
                    catch (Exception ex)
                    {
                        progress?.Report($"Error adding layer {layerInfo.LayerName}: {ex.Message}");
                        System.Diagnostics.Debug.WriteLine($"AddLayersWithFallbackBatch: Error adding layer {layerInfo.LayerName}: {ex.Message}");
                    }
                }
            });
        }

        /// <summary>
        /// Ensure the target file path is available for writing; tries to delete if it exists, with retries.
        /// If still locked, returns a unique alternate path.
        /// </summary>
        private static async Task<string> EnsureTargetFileAvailableAsync(string targetPath)
        {
            const int maxAttempts = 3;
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    if (File.Exists(targetPath))
                    {
                        File.Delete(targetPath);
                    }
                    return targetPath;
                }
                catch (IOException ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Attempt {attempt} to delete {targetPath} failed: {ex.Message}.");
                    if (attempt < maxAttempts)
                    {
                        System.Diagnostics.Debug.WriteLine("Retrying...");
                        await Task.Delay(attempt * 200);
                    }
                }
            }

            // All attempts failed; return a unique alternative path so the caller can still write
            string altPath = Path.Combine(
                Path.GetDirectoryName(targetPath) ?? string.Empty,
                $"{Path.GetFileNameWithoutExtension(targetPath)}_{Guid.NewGuid():N}{Path.GetExtension(targetPath)}");

            System.Diagnostics.Debug.WriteLine($"Target file locked after retries. Using alternate path: {altPath}");
            return altPath;
        }

        private async Task FallbackToIndividualLayerCreation(List<LayerCreationInfo> layers, IProgress<string> progress)
        {
            await AddLayersWithFallbackBatch(layers, progress);
        }

        /// <summary>
        /// Applies consistent settings to feature layers (cache, feature reduction, etc.)
        /// Optionally sets a CIM renderer and label classes in the same definition update.
        /// Note: In Pro 3.5, accessing layer definitions for Parquet files may trigger domain lookups that fail
        /// </summary>
        private static void ApplyLayerSettings(FeatureLayer featureLayer, CIMRenderer renderer = null, LayerCreationInfo layerInfo = null, MapStyleDefinition mapStyle = null)
        {
            try
            {
                if (featureLayer.GetDefinition() is CIMFeatureLayer layerDef)
                {
                    layerDef.DisplayCacheType = ArcGIS.Core.CIM.DisplayCacheType.None;
                    layerDef.FeatureCacheType = ArcGIS.Core.CIM.FeatureCacheType.None;

                    if (layerDef.FeatureReduction is CIMBinningFeatureReduction binningReduction && binningReduction.Enabled)
                    {
                        bool isPointLayer =
                            layerInfo != null &&
                            (string.Equals(layerInfo.GeometryType, "POINT", StringComparison.OrdinalIgnoreCase) ||
                             string.Equals(layerInfo.GeometryType, "MULTIPOINT", StringComparison.OrdinalIgnoreCase));

                        if (!isPointLayer)
                        {
                            binningReduction.Enabled = false;
                            if (VerbosePerLayerDebugLogging)
                            {
                                System.Diagnostics.Debug.WriteLine($"Disabled feature binning for layer: {featureLayer.Name}");
                            }
                        }
                        else
                        {
                            if (VerbosePerLayerDebugLogging)
                            {
                                System.Diagnostics.Debug.WriteLine($"Kept feature binning enabled for dense point layer: {featureLayer.Name}");
                            }
                        }
                    }

                    if (renderer != null)
                    {
                        layerDef.Renderer = renderer;
                    }

                    bool labelsAdded = false;
                    if (layerInfo != null)
                    {
                        labelsAdded = CartographyService.ApplyLabelClasses(layerDef, layerInfo.ActualType, layerInfo.GeometryType, mapStyle);
                    }

                    featureLayer.SetDefinition(layerDef);
                    if (renderer != null)
                    {
                        try
                        {
                            featureLayer.SetRenderer(renderer);
                        }
                        catch (Exception rendererEx)
                        {
                            System.Diagnostics.Debug.WriteLine($"Warning: SetRenderer failed for {featureLayer.Name}: {rendererEx.Message}");
                        }
                    }
                    if (labelsAdded)
                        featureLayer.SetLabelVisibility(true);

                    if (layerInfo != null)
                    {
                        CartographyService.ApplyVisibilityDefaults(featureLayer, layerInfo.ActualType, layerInfo.GeometryType);
                    }

                    if (VerbosePerLayerDebugLogging)
                    {
                        System.Diagnostics.Debug.WriteLine($"Applied settings{(renderer != null ? " + renderer" : "")}{(layerInfo != null ? " + labels" : "")} to layer: {featureLayer.Name}");
                    }
                }
            }
            catch (ArgumentException ex) when (ex.Message.Contains("domain") || ex.Message.Contains("Domain") || ex.Message.Contains("not supported"))
            {
                System.Diagnostics.Debug.WriteLine($"Info: Skipping layer settings for {featureLayer.Name} (domain access not supported for Parquet in Pro 3.5): {ex.Message}");
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

        /// <summary>
        /// Executes a SQL query against the current DuckDB connection and returns results
        /// </summary>
        public async Task<List<Dictionary<string, object>>> ExecuteQueryAsync(string sqlQuery)
        {
            var results = new List<Dictionary<string, object>>();

            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = sqlQuery;
                using var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var fieldName = reader.GetName(i);
                        var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        row[fieldName] = value;
                    }
                    results.Add(row);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error executing query: {ex.Message}");
                throw;
            }

            return results;
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

                Layer newLayer = null;
                try
                {
                    // Create a URI for the Parquet file
                    Uri dataUri = new(parquetFilePath);

                    // Create the layer using LayerFactory
                    // Note: In Pro 3.5, this may succeed but subsequent property access may fail
                    newLayer = LayerFactory.Instance.CreateLayer(dataUri, map, layerName: layerName);

                    if (newLayer == null)
                    {
                        progress?.Report($"Error: Could not create layer for {layerName}.");
                        System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: LayerFactory returned null for {parquetFilePath}");
                        return;
                    }

                    // Attempt to set cache settings to None for performance with potentially large/dynamic datasets
                    // Note: In Pro 3.5, accessing layer definitions for Parquet files may trigger domain lookups
                    // that fail, so we wrap this in comprehensive error handling
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
                        catch (ArgumentException ex) when (ex.Message.Contains("domain") || ex.Message.Contains("Domain"))
                        {
                            // Pro 3.5 may try to access domains which Parquet files don't support
                            // This is a known limitation - continue without setting cache options
                            System.Diagnostics.Debug.WriteLine($"Info: Skipping cache settings for {featureLayerForCacheSettings.Name} (domain access not supported for Parquet in Pro 3.5): {ex.Message}");
                        }
                        catch (Exception ex)
                        {
                            // Log but continue - layer is still usable without these settings
                            System.Diagnostics.Debug.WriteLine($"Warning: Failed to set cache options for layer {featureLayerForCacheSettings.Name}: {ex.Message}");
                        }
                    }

                    // Specific handling to disable feature reduction/binning by default
                    // Note: In Pro 3.5, accessing layer definitions for Parquet files may trigger domain lookups
                    if (newLayer is FeatureLayer featureLayer) // Apply to all FeatureLayers
                    {
                        try
                        {
                            if (featureLayer.GetDefinition() is CIMFeatureLayer layerDef)
                            {
                                // Specifically check for CIMBinningFeatureReduction
                                if (layerDef.FeatureReduction is CIMBinningFeatureReduction binningReduction)
                                {
                                    bool isPointLayer = featureLayer.ShapeType == esriGeometryType.esriGeometryPoint
                                        || featureLayer.ShapeType == esriGeometryType.esriGeometryMultipoint;

                                    if (binningReduction.Enabled) // Only disable if it's currently enabled
                                    {
                                        if (!isPointLayer)
                                        {
                                            binningReduction.Enabled = false;
                                            featureLayer.SetDefinition(layerDef); // Apply the change
                                            System.Diagnostics.Debug.WriteLine($"Disabled feature binning for layer: {featureLayer.Name}");
                                        }
                                        else
                                        {
                                            System.Diagnostics.Debug.WriteLine($"Kept feature binning enabled for dense point layer: {featureLayer.Name}");
                                        }
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
                        catch (ArgumentException ex) when (ex.Message.Contains("domain") || ex.Message.Contains("Domain") || ex.Message.Contains("not supported"))
                        {
                            // Pro 3.5 may try to access domains or unsupported operations for Parquet files
                            // This is a known limitation - continue without modifying feature reduction
                            System.Diagnostics.Debug.WriteLine($"Info: Skipping feature reduction settings for {featureLayer.Name} (domain/operation not supported for Parquet in Pro 3.5): {ex.Message}");
                        }
                        catch (Exception ex)
                        {
                            // Log any errors during the process but continue
                            System.Diagnostics.Debug.WriteLine($"Warning: Failed to access or modify feature reduction for layer {featureLayer.Name}: {ex.Message}");
                        }
                    }

                    // If we got here, layer was created successfully
                    // Even if property access failed, the layer is usable
                    progress?.Report($"Successfully added layer: {newLayer?.Name ?? layerName}");
                    System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Successfully added layer {newLayer?.Name ?? layerName} to map {map.Name}");
                }
                catch (ArgumentException ex) when (ex.Message.Contains("domain") || ex.Message.Contains("Domain") || ex.Message.Contains("not supported"))
                {
                    // Pro 3.5 limitation: Parquet files don't support domains
                    // If layer was created, it's still usable even if property access failed
                    if (newLayer != null)
                    {
                        progress?.Report($"Layer {layerName} added (some properties unavailable in Pro 3.5)");
                        System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Layer {layerName} added with limitations (domain access not supported): {ex.Message}");
                    }
                    else
                    {
                        // Layer creation itself failed
                        progress?.Report($"Error: Could not create layer {layerName} (Parquet limitation in Pro 3.5): {ex.Message}");
                        System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Failed to create layer {layerName}: {ex.Message}");
                        throw; // Re-throw if layer creation failed
                    }
                }
                catch (Exception ex)
                {
                    // If layer was created, report success with warning
                    if (newLayer != null)
                    {
                        progress?.Report($"Layer {layerName} added (with warnings: {ex.Message})");
                        System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Layer {layerName} added but encountered error: {ex.Message}");
                    }
                    else
                    {
                        // Layer creation failed completely
                        progress?.Report($"Error adding layer {layerName} to map: {ex.Message}");
                        System.Diagnostics.Debug.WriteLine($"AddLayerToMapAsync: Error adding layer {layerName} from {parquetFilePath}: {ex.Message}");
                        System.Diagnostics.Debug.WriteLine($"Stack trace: {ex.StackTrace}");
                        throw; // Re-throw if layer creation failed
                    }
                }
            });
        }
    }
}
