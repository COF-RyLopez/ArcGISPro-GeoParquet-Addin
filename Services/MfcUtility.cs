using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Utility class for creating and managing Multifile Feature Connections (MFC)
    /// for Overture Maps data
    /// </summary>
    public class MfcUtility
    {
        private const string GEOMETRY_COLUMN = "geometry"; // Added class constant

        // C# Models for MFC JSON Structure
        public class MfcConnectionProps
        {
            [JsonPropertyName("path")]
            public string Path { get; set; }
        }

        public class MfcConnectionInfo // Renamed from MfcConnection to avoid conflict
        {
            [JsonPropertyName("type")]
            public string Type { get; set; } = "filesystem";

            [JsonPropertyName("properties")]
            public MfcConnectionProps Properties { get; set; }
        }

        public class MfcDatasetProperties
        {
            [JsonPropertyName("fileformat")]
            public string FileFormat { get; set; } = "parquet";
        }

        public class MfcField
        {
            [JsonPropertyName("name")]
            public string Name { get; set; }

            [JsonPropertyName("type")]
            public string Type { get; set; }

            // Make Visible nullable. It will only be serialized if it has a value.
            // We'll typically only set this to false for the main geometry field.
            [JsonPropertyName("visible")]
            [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
            public bool? Visible { get; set; }

            // Add SourceType, to be serialized only if it has a value.
            [JsonPropertyName("sourceType")]
            [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
            public string SourceType { get; set; }

            // Constructor to simplify creation
            public MfcField(string name, string type, bool? visible = null, string sourceType = null)
            {
                Name = name;
                Type = type;
                Visible = visible;
                SourceType = sourceType;
            }
        }

        public class MfcGeometryField
        {
            [JsonPropertyName("name")]
            public string Name { get; set; }

            [JsonPropertyName("formats")]
            public List<string> Formats { get; set; }
        }

        public class MfcSpatialReference
        {
            [JsonPropertyName("wkid")]
            public int Wkid { get; set; }
        }

        public class MfcGeometry
        {
            [JsonPropertyName("geometryType")]
            public string GeometryType { get; set; }

            [JsonPropertyName("spatialReference")]
            public MfcSpatialReference SpatialReference { get; set; }

            [JsonPropertyName("fields")]
            public List<MfcGeometryField> Fields { get; set; }
        }

        public class MfcDataset
        {
            [JsonPropertyName("name")]
            public string Name { get; set; }

            [JsonPropertyName("alias")]
            public string Alias { get; set; }

            [JsonPropertyName("properties")]
            public MfcDatasetProperties Properties { get; set; } = new MfcDatasetProperties();

            [JsonPropertyName("fields")]
            public List<MfcField> FieldsList { get; set; } // Renamed to avoid conflict with MfcGeometry.Fields

            [JsonPropertyName("geometry")]
            public MfcGeometry Geometry { get; set; }
        }

        public class MfcRoot
        {
            [JsonPropertyName("connection")]
            public MfcConnectionInfo Connection { get; set; } // Use renamed MfcConnectionInfo

            [JsonPropertyName("datasets")]
            public List<MfcDataset> Datasets { get; set; } = new List<MfcDataset>();
        }

        // Helper for sanitizing file names if needed (currently used by DataProcessor)
        public static string SanitizeFileName(string fileName)
        {
            // Basic sanitization, can be expanded
            return string.Join("_", fileName.Split(Path.GetInvalidFileNameChars()));
        }

        // Define field exclusion and renaming maps
        private static readonly Dictionary<string, HashSet<string>> FieldExclusionMap = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
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

        private static readonly Dictionary<string, Dictionary<string, string>> FieldRenameMap = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase)
        {
            { "division", new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    { "perspectives", "perspectives_mode" },
                    { "norms", "norms_driving_side" }
                }
            }
            // Add other dataset types and their renames as needed
            // For "place" and "brand_wikidata" / "brand_names_primary":
            // We are currently excluding the 'brand' struct. If flattened versions exist in Parquet, 
            // they should be picked up automatically. If not, they can't be created by the MFC.
        };

        private static readonly Dictionary<string, List<string>> DatasetFieldOrder = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase)
        {
            {
                "address", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "country", "postcode", "street", "number", "unit", "postal_city",
                    "version", "filename", "theme", "type", "geometry"
                }
            },
            {
                "building", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "level", "subtype", "class", "height", "names_primary",
                    "has_parts", "is_underground", "num_floors", "num_floors_underground",
                    "min_height", "min_floor", "facade_color", "facade_material",
                    "roof_material", "roof_shape", "roof_direction", "roof_orientation",
                    "roof_color", "roof_height", "filename", "theme", "type", "geometry"
                }
            },
            {
                "building_part", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "level", "height", "names_primary", "is_underground",
                    "num_floors", "num_floors_underground", "min_height", "min_floor",
                    "facade_color", "facade_material", "roof_material", "roof_shape",
                    "roof_direction", "roof_orientation", "roof_color", "roof_height",
                    "building_id", "filename", "theme", "type", "geometry"
                }
            },
            {
                "connector", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "filename", "theme", "type", "geometry"
                }
            },
            {
                "division", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "country", "version", "cartography_prominence", "cartography_min_zoom",
                    "cartography_max_zoom", "cartography_sort_key", "subtype", "class",
                    "names_primary", "wikidata", "region", "perspectives_mode",
                    "parent_division_id", "norms_driving_side", "population",
                    "filename", "theme", "type", "geometry"
                }
            },
            {
                "division_area", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "country", "version", "subtype", "class", "names_primary",
                    "is_land", "is_territorial", "region", "division_id",
                    "filename", "theme", "type", "geometry"
                }
            },
            {
                "infrastructure", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "level", "subtype", "class", "height", "surface",
                    "names_primary", "wikidata", "filename", "theme", "type", "geometry"
                }
            },
            {
                "land", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "level", "subtype", "class", "surface", "names_primary",
                    "wikidata", "elevation", "filename", "theme", "type", "geometry"
                }
            },
            {
                "land_cover", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "cartography_prominence", "cartography_min_zoom",
                    "cartography_max_zoom", "cartography_sort_key", "subtype",
                    "filename", "theme", "type", "geometry"
                }
            },
            {
                "land_use", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "level", "subtype", "class", "surface", "names_primary",
                    "wikidata", "filename", "theme", "type", "geometry"
                }
            },
            {
                "place", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "names_primary", "categories_primary", "confidence",
                    "brand_wikidata", "brand_names_primary", "filename", "theme", "type", "geometry"
                }
            },
            {
                "segment", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "subtype", "class", "names_primary", "subclass",
                    "filename", "theme", "type", "geometry"
                }
            },
            {
                "water", new List<string> {
                    "id", "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax",
                    "version", "level", "subtype", "class", "names_primary", "wikidata",
                    "is_salt", "is_intermittent", "filename", "theme", "type", "geometry"
                }
            }
        };

        public static async Task<bool> GenerateMfcFileAsync(string sourceDataFolder, string outputMfcFilePath, string addinExecutingPath, Action<string> logAction = null)
        {
            logAction ??= Console.WriteLine; // Default logger

            try
            {
                logAction($"Starting MFC generation. Source: {sourceDataFolder}, Output: {outputMfcFilePath}");

                var mfcRoot = new MfcRoot
                {
                    Connection = new MfcConnectionInfo
                    {
                        Properties = new MfcConnectionProps
                        {
                            Path = sourceDataFolder.Replace('/', '\\')
                        }
                    }
                };

                var datasetDirectories = Directory.GetDirectories(sourceDataFolder);
                if (!datasetDirectories.Any())
                {
                    logAction($"No dataset subfolders found in {sourceDataFolder}. Cannot generate MFC.");
                    return false;
                }

                string extensionsPath = Path.Combine(addinExecutingPath, "Extensions");
                string normalizedExtensionsPath = extensionsPath.Replace('\\', '/');

                using (var duckDBConnection = new DuckDBConnection("DataSource=:memory:"))
                {
                    await duckDBConnection.OpenAsync();
                    using (var setupCmd = duckDBConnection.CreateCommand())
                    {
                        bool spatialLoaded = false;
                        // 1. Prioritize Bundled Extension
                        try
                        {
                            setupCmd.CommandText = $"SET extension_directory='{normalizedExtensionsPath}'; LOAD spatial;";
                            await setupCmd.ExecuteNonQueryAsync();
                            logAction("DuckDB spatial extension loaded successfully from local add-in directory.");
                            spatialLoaded = true;
                        }
                        catch (Exception extEx)
                        {
                            logAction($"Info: Could not load DuckDB spatial extension from local directory '{normalizedExtensionsPath}'. Error: {extEx.Message}. Will try other methods.");
                        }

                        // 2. Attempt simple LOAD spatial (if Pro 3.5 makes it available globally to .NET DuckDB)
                        if (!spatialLoaded)
                        {
                            try
                            {
                                setupCmd.CommandText = "LOAD spatial;";
                                await setupCmd.ExecuteNonQueryAsync();
                                logAction("DuckDB spatial extension loaded successfully using simple 'LOAD spatial'. (Potentially from ArcGIS Pro default environment)");
                                spatialLoaded = true;
                            }
                            catch (Exception loadEx)
                            {
                                logAction($"Info: Simple 'LOAD spatial' failed. Error: {loadEx.Message}. Will try FORCE INSTALL as last resort.");
                            }
                        }

                        // 3. Try Force Install and Load (if others fail)
                        if (!spatialLoaded)
                        {
                            try
                            {
                                setupCmd.CommandText = "FORCE INSTALL spatial; LOAD spatial;";
                                await setupCmd.ExecuteNonQueryAsync();
                                logAction("DuckDB spatial extension FORCE INSTALLED and LOADED successfully.");

                                // Diagnostic check for spatial functions
                                using (var checkCmd = duckDBConnection.CreateCommand())
                                {
                                    checkCmd.CommandText = "SELECT function_name FROM duckdb_functions() WHERE function_name ILIKE 'st_srid' OR function_name ILIKE 'st_geometrytype' ORDER BY function_name;";
                                    logAction($"Executing diagnostic query: {checkCmd.CommandText}");
                                    using (var reader = await checkCmd.ExecuteReaderAsync())
                                    {
                                        bool foundSrid = false;
                                        bool foundGeomType = false;
                                        while (await reader.ReadAsync())
                                        {
                                            string funcName = reader.GetString(0);
                                            logAction($"Found function via diagnostic query: {funcName}");
                                            if (funcName.ToLowerInvariant() == "st_srid") foundSrid = true;
                                            if (funcName.ToLowerInvariant() == "st_geometrytype") foundGeomType = true;
                                        }
                                        if (foundSrid && foundGeomType)
                                        {
                                            logAction("Diagnostic check: ST_SRID and ST_GeometryType ARE listed in duckdb_functions().");
                                        }
                                        else if (foundSrid)
                                        {
                                            logAction("Diagnostic check: ST_SRID IS listed, but ST_GeometryType IS NOT.");
                                        }
                                        else if (foundGeomType)
                                        {
                                            logAction("Diagnostic check: ST_GeometryType IS listed, but ST_SRID IS NOT.");
                                        }
                                        else
                                        {
                                            logAction("Diagnostic check: NEITHER ST_SRID NOR ST_GeometryType are listed in duckdb_functions(). This is the core issue.");
                                        }
                                    }
                                }
                                spatialLoaded = true;
                            }
                            catch (Exception forceEx)
                            {
                                logAction($"CRITICAL ERROR: All attempts to load DuckDB spatial extension failed (bundled, simple load, force install/load). Error during FORCE INSTALL/LOAD: {forceEx.Message}. MFC generation will likely fail or produce incorrect geometry types. Please ensure 'spatial.duckdb_extension' is in '{normalizedExtensionsPath}' or that network access allows DuckDB to download it.");
                                spatialLoaded = false; // Explicitly false
                            }
                        }

                        if (!spatialLoaded)
                        {
                            logAction("CRITICAL ERROR: Spatial extension could not be loaded after all attempts. Cannot proceed with MFC generation.");
                            return false;
                        }
                    }

                    foreach (var dirPath in datasetDirectories)
                    {
                        string datasetName = new DirectoryInfo(dirPath).Name;
                        logAction($"Processing dataset: {datasetName}");

                        var parquetFiles = Directory.GetFiles(dirPath, "*.parquet")
                                            .OrderBy(f => f)
                                            .ToList();

                        if (!parquetFiles.Any())
                        {
                            logAction($"No .parquet files found in {dirPath} for dataset {datasetName}. Skipping.");
                            continue;
                        }

                        string firstParquetFileForSchema = parquetFiles.First().Replace('\\', '/');
                        logAction($"Using sample file for general schema: {firstParquetFileForSchema}");

                        var dataset = new MfcDataset
                        {
                            Name = datasetName,
                            Alias = datasetName,
                            FieldsList = new List<MfcField>()
                        };

                        bool geometryColumnExistsInSchema = false;
                        try
                        {
                            var rawColumns = await DiscoverSchemaAsync(duckDBConnection, firstParquetFileForSchema, logAction);
                            dataset.FieldsList = BuildFieldList(rawColumns, datasetName, GEOMETRY_COLUMN, logAction, out geometryColumnExistsInSchema);
                        }
                        catch (Exception ex)
                        {
                            logAction($"Error describing schema for {firstParquetFileForSchema} in dataset {datasetName}: {ex.Message}");
                            continue;
                        }

                        string detectedGeometryType = null;
                        string detectedWkid = "4326"; // Default SRID

                        if (geometryColumnExistsInSchema)
                        {
                            detectedGeometryType = await DetectGeometryTypeAsync(duckDBConnection, datasetName, parquetFiles, logAction);
                        }

                        if (geometryColumnExistsInSchema && !string.IsNullOrEmpty(detectedGeometryType))
                        {
                            dataset.Geometry = new MfcGeometry
                            {
                                GeometryType = MapDuckDbGeomTypeToEsriGeomType(detectedGeometryType.ToUpperInvariant(), logAction),
                                SpatialReference = new MfcSpatialReference { Wkid = int.Parse(detectedWkid) },
                                Fields = new List<MfcGeometryField> { new MfcGeometryField { Name = GEOMETRY_COLUMN, Formats = new List<string> { "WKB" } } }
                            };
                            logAction($"MFC Generation: Added geometry definition for '{datasetName}' with type '{dataset.Geometry.GeometryType}' and SRID '{detectedWkid}'.");
                        }
                        else if (geometryColumnExistsInSchema) // Geometry column was in schema, but type detection failed
                        {
                            logAction($"Warning: Dataset '{datasetName}' has a '{GEOMETRY_COLUMN}' field, but its type could not be robustly determined. Using default 'esriGeometryAny' and SRID '{detectedWkid}'.");
                            dataset.Geometry = new MfcGeometry
                            {
                                GeometryType = "esriGeometryAny",
                                SpatialReference = new MfcSpatialReference { Wkid = int.Parse(detectedWkid) },
                                Fields = new List<MfcGeometryField> { new MfcGeometryField { Name = GEOMETRY_COLUMN, Formats = new List<string> { "WKB" } } }
                            };
                        }
                        // If no geometry column in schema, dataset.Geometry remains null, and will be omitted by JsonSerializerOptions if DefaultIgnoreCondition is WhenWritingNull.

                        mfcRoot.Datasets.Add(dataset);
                    }
                }

                await WriteMfcFile(outputMfcFilePath, mfcRoot, logAction);
                return true;
            }
            catch (Exception ex)
            {
                logAction($"Error generating MFC file: {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        private static async Task<List<(string Name, string Type)>> DiscoverSchemaAsync(
            DuckDBConnection connection, string parquetFilePath, Action<string> logAction)
        {
            var columns = new List<(string Name, string Type)>();
            using (var schemaCmd = connection.CreateCommand())
            {
                schemaCmd.CommandText = $"DESCRIBE SELECT * FROM read_parquet('{parquetFilePath.Replace("'", "''")}') LIMIT 0;";
                using (var reader = await schemaCmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        string colName = reader.GetString(0);
                        string colType = reader.GetString(1).ToUpper();
                        columns.Add((colName, colType));
                    }
                }
            }
            return columns;
        }

        private static List<MfcField> BuildFieldList(
            List<(string Name, string Type)> schemaColumns, string datasetName,
            string geometryColumnName, Action<string> logAction,
            out bool geometryColumnExists)
        {
            geometryColumnExists = false;
            var columns = new List<MfcField>();
            var addedFieldNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var knownBooleanFields = new HashSet<string> {
                "has_parts", "is_underground", "is_land", "is_territorial", "is_salt", "is_intermittent"
            };

            foreach (var (rawName, duckDbType) in schemaColumns)
            {
                string columnName = rawName;

                logAction($"MFC Generation: Dataset '{datasetName}' - Schema Column: '{columnName}', DuckDB Type: '{duckDbType}'");

                if (columnName.StartsWith("__duckdb_internal")) continue;

                if (FieldExclusionMap.TryGetValue(datasetName, out var exclusions) && exclusions.Contains(columnName))
                {
                    logAction($"MFC Generation: Excluding field '{columnName}' for dataset '{datasetName}' as per exclusion rules.");
                    continue;
                }

                if (FieldRenameMap.TryGetValue(datasetName, out var renames) && renames.TryGetValue(columnName, out var newName))
                {
                    logAction($"MFC Generation: Renaming field '{columnName}' to '{newName}' for dataset '{datasetName}'.");
                    columnName = newName;
                }

                string mfcType;
                string sourceType = null;

                if (columnName.ToLower() == geometryColumnName.ToLower())
                {
                    geometryColumnExists = true;
                    if (addedFieldNames.Add(columnName))
                    {
                        columns.Add(new MfcField(columnName, "Binary"));
                    }
                    logAction($"MFC Generation: Found '{geometryColumnName}' field for '{datasetName}'. Will be added to main field list.");
                    continue;
                }

                if (columnName.ToLower() == "bbox_xmin" || columnName.ToLower() == "bbox_xmax" ||
                    columnName.ToLower() == "bbox_ymin" || columnName.ToLower() == "bbox_ymax")
                {
                    if (addedFieldNames.Add(columnName))
                    {
                        columns.Add(new MfcField(columnName, "Float32"));
                    }
                    continue;
                }

                if (columnName.ToLower() == "bbox")
                {
                    logAction($"MFC Generation: Encountered 'bbox' struct for dataset '{datasetName}'. It will be skipped in main field list. Flattened versions will be ensured.");
                    continue;
                }

                if ((columnName.ToLower() == "names" || columnName.ToLower() == "categories") && duckDbType.StartsWith("STRUCT"))
                {
                    string primaryFieldName = $"{columnName}_primary";
                    if (addedFieldNames.Add(primaryFieldName))
                    {
                        columns.Add(new MfcField(primaryFieldName, "String"));
                    }
                    continue;
                }

                if (columnName.ToLower() == "cartography" && duckDbType.StartsWith("STRUCT"))
                {
                    var cartoSubFields = new Dictionary<string, string>
                    {
                        { "prominence", "Int32" }, { "min_zoom", "Int32" },
                        { "max_zoom", "Int32" }, { "sort_key", "Int32" }
                    };
                    foreach (var subField in cartoSubFields)
                    {
                        string fullSubFieldName = $"cartography_{subField.Key}";
                        if (addedFieldNames.Add(fullSubFieldName))
                        {
                            columns.Add(new MfcField(fullSubFieldName, subField.Value));
                        }
                    }
                    continue;
                }

                mfcType = ConvertDuckDbTypeToMfcType(duckDbType, columnName, logAction);
                if (knownBooleanFields.Contains(columnName.ToLower()))
                {
                    mfcType = "String";
                    sourceType = "Boolean";
                }

                if (addedFieldNames.Add(columnName))
                {
                    columns.Add(new MfcField(columnName, mfcType, null, sourceType));
                }
            }

            string[] requiredBboxFields = { "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax" };
            foreach (var bboxField in requiredBboxFields)
            {
                if (addedFieldNames.Add(bboxField))
                {
                    columns.Add(new MfcField(bboxField, "Float32"));
                    logAction($"MFC Generation: Ensured '{bboxField}' (Float32) is added to dataset '{datasetName}'.");
                }
            }

            if (datasetName.Equals("place", StringComparison.OrdinalIgnoreCase))
            {
                if (addedFieldNames.Add("brand_wikidata"))
                {
                    columns.Add(new MfcField("brand_wikidata", "String"));
                    logAction($"MFC Generation: Ensured 'brand_wikidata' (String) is added to dataset 'place'.");
                }
                if (addedFieldNames.Add("brand_names_primary"))
                {
                    columns.Add(new MfcField("brand_names_primary", "String"));
                    logAction($"MFC Generation: Ensured 'brand_names_primary' (String) is added to dataset 'place'.");
                }
            }

            if (DatasetFieldOrder.TryGetValue(datasetName, out var specificOrder))
            {
                var finalOrderedFieldsList = new List<MfcField>();
                var availableFields = columns.ToDictionary(f => f.Name, f => f, StringComparer.OrdinalIgnoreCase);

                foreach (var fieldNameInOrder in specificOrder)
                {
                    if (availableFields.TryGetValue(fieldNameInOrder, out var field))
                    {
                        finalOrderedFieldsList.Add(field);
                        availableFields.Remove(fieldNameInOrder);
                    }
                    else
                    {
                        logAction($"Warning: Field '{fieldNameInOrder}' specified in order for dataset '{datasetName}' was not found in available fields. It might be excluded, not present in Parquet, or not yet handled (e.g. complex structs).");
                    }
                }

                var stillAvailableFields = availableFields.Values.ToList();
                MfcField geomFieldFromAvailable = stillAvailableFields.FirstOrDefault(f => f.Name.Equals(geometryColumnName, StringComparison.OrdinalIgnoreCase));

                foreach (var field in stillAvailableFields
                    .Where(f => !f.Name.Equals(geometryColumnName, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase))
                {
                    finalOrderedFieldsList.Add(field);
                    logAction($"MFC Generation: Adding field '{field.Name}' to dataset '{datasetName}' (was in Parquet but not in specific order).");
                }

                if (geomFieldFromAvailable != null && !specificOrder.Contains(geometryColumnName, StringComparer.OrdinalIgnoreCase))
                {
                    finalOrderedFieldsList.Add(geomFieldFromAvailable);
                    logAction($"MFC Generation: Adding field '{geometryColumnName}' to dataset '{datasetName}' (was in Parquet but not in specific order, placed last among extras).");
                }

                return finalOrderedFieldsList;
            }
            else
            {
                var defaultOrderedFields = new List<MfcField>();
                MfcField idField = columns.FirstOrDefault(f => f.Name.Equals("id", StringComparison.OrdinalIgnoreCase));
                if (idField != null)
                {
                    defaultOrderedFields.Add(idField);
                    columns.Remove(idField);
                }

                var bboxMfcFieldsSource = columns.Where(f => f.Name.StartsWith("bbox_", StringComparison.OrdinalIgnoreCase)).ToList();
                var bboxOrderedFields = new List<MfcField>();
                foreach (string bboxName in new[] { "bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax" })
                {
                    MfcField field = bboxMfcFieldsSource.FirstOrDefault(f => f.Name.Equals(bboxName, StringComparison.OrdinalIgnoreCase));
                    if (field != null)
                    {
                        bboxOrderedFields.Add(field);
                        columns.Remove(field);
                    }
                }
                defaultOrderedFields.AddRange(bboxOrderedFields);

                MfcField geomField = columns.FirstOrDefault(f => f.Name.Equals(geometryColumnName, StringComparison.OrdinalIgnoreCase));
                if (geomField != null) columns.Remove(geomField);

                var otherFields = columns.OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase).ToList();
                defaultOrderedFields.AddRange(otherFields);

                if (geomField != null) defaultOrderedFields.Add(geomField);

                return defaultOrderedFields;
            }
        }

        private static async Task<string> DetectGeometryTypeAsync(
            DuckDBConnection connection, string datasetName, List<string> parquetFiles, Action<string> logAction)
        {
            string detectedGeometryType = null;
            const int MaxFilesToCheck = 3;

            logAction($"Starting geometry type/SRID detection for dataset '{datasetName}'...");
            int fileIndex = 0;
            foreach (var parquetFile in parquetFiles)
            {
                fileIndex++;
                if (fileIndex > MaxFilesToCheck)
                {
                    logAction($"    Stopping geometry detection after {MaxFilesToCheck} files for dataset '{datasetName}' to avoid long scans.");
                    break;
                }

                string currentFileForGeomCheck = parquetFile.Replace('\\', '/');
                logAction($"  Checking file: {currentFileForGeomCheck}");
                try
                {
                    using (var geomCmd = connection.CreateCommand())
                    {
                        string query = $"SELECT ST_GeometryType({GEOMETRY_COLUMN}) FROM read_parquet('{currentFileForGeomCheck.Replace("'", "''")}') WHERE {GEOMETRY_COLUMN} IS NOT NULL LIMIT 1;";
                        geomCmd.CommandText = query;
                        logAction($"    Executing query: {query}");

                        using (var geomReader = await geomCmd.ExecuteReaderAsync())
                        {
                            if (await geomReader.ReadAsync())
                            {
                                logAction("    Successfully read a row for geometry info.");
                                object rawGeomTypeObj = geomReader.GetValue(0);

                                logAction($"    Raw ST_GeometryType: {(rawGeomTypeObj == DBNull.Value ? "DBNull" : rawGeomTypeObj?.ToString())}");

                                if (rawGeomTypeObj != DBNull.Value && rawGeomTypeObj != null)
                                {
                                    detectedGeometryType = rawGeomTypeObj.ToString();
                                    logAction($"MFC Generation: Detected geometry type '{detectedGeometryType}' for dataset '{datasetName}' using file '{currentFileForGeomCheck}'. SRID assumed as 4326.");
                                    break;
                                }
                                else
                                {
                                    logAction("    ST_GeometryType was DBNull or null. Trying next file.");
                                }
                            }
                            else
                            {
                                logAction("    No rows returned for geometry info from this file. Trying next file.");
                            }
                        }
                    }
                }
                catch (Exception geomEx)
                {
                    logAction($"    Warning: Error executing geometry detection query on '{currentFileForGeomCheck}' for dataset '{datasetName}': {geomEx.Message}. Trying next file if available.");
                }
                if (!string.IsNullOrEmpty(detectedGeometryType)) break;
            }

            if (string.IsNullOrEmpty(detectedGeometryType))
            {
                logAction($"Warning: Could not detect a specific geometry type for dataset '{datasetName}' after checking all files. Defaulting geometry definition.");
            }

            return detectedGeometryType;
        }

        private static async Task WriteMfcFile(string mfcFilePath, MfcRoot mfcRoot, Action<string> logAction)
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };
            string jsonString = JsonSerializer.Serialize(mfcRoot, options);

            await File.WriteAllTextAsync(mfcFilePath, jsonString);
            logAction($"MFC file successfully generated at {mfcFilePath}");
        }

        private static string ConvertDuckDbTypeToMfcType(string duckDbType, string columnNameForContext, Action<string> logAction)
        {
            if (duckDbType.StartsWith("DECIMAL")) return "Float64";
            if (duckDbType.StartsWith("VARCHAR") || duckDbType.Contains("CHAR") || duckDbType == "TEXT") return "String";

            switch (duckDbType)
            {
                case "BOOLEAN":
                    logAction($"Converting DuckDB BOOLEAN type for column '{columnNameForContext}' to String. Consider adding to knownBooleanFields for SourceType.");
                    return "String";
                case "TINYINT": return "Int8";
                case "SMALLINT": return "Int16";
                case "INTEGER": return "Int32";
                case "BIGINT": return "Int64";
                case "HUGEINT": return "String";
                case "FLOAT4":
                case "REAL":
                case "FLOAT":
                    return "Float32";
                case "FLOAT8":
                case "DOUBLE PRECISION":
                case "DOUBLE":
                    return "Float64";
                case "DATE": return "Date";
                case "TIMESTAMP": return "String";
                case "TIMESTAMPTZ": return "String";
                case "TIME": return "String";
                case "INTERVAL": return "String";
                case "BLOB": return "Binary";
                case "BYTEA": return "Binary";
                default:
                    if (duckDbType.StartsWith("STRUCT") || duckDbType.StartsWith("LIST") || duckDbType.StartsWith("ARRAY") || duckDbType.StartsWith("MAP"))
                    {
                        logAction($"Warning: Converting complex DuckDB type '{duckDbType}' for column '{columnNameForContext}' to String. Data may be stringified.");
                        return "String";
                    }
                    logAction($"Warning: Unknown DuckDB type for column '{columnNameForContext}': '{duckDbType}'. Defaulting to String.");
                    return "String";
            }
        }

        private static string MapDuckDbGeomTypeToEsriGeomType(string duckDbGeomType, Action<string> logAction)
        {
            switch (duckDbGeomType)
            {
                case "POINT":
                case "MULTIPOINT":
                    return "esriGeometryPoint";
                case "LINESTRING":
                case "MULTILINESTRING":
                    return "esriGeometryPolyline";
                case "POLYGON":
                case "MULTIPOLYGON":
                    return "esriGeometryPolygon";
                default:
                    logAction($"Unmapped DuckDB geometry type: {duckDbGeomType}. Defaulting to esriGeometryPoint.");
                    return "esriGeometryPoint";
            }
        }

        // Removed the old CreateMfcAsync stub and RefreshMfcAsync method
    }
}
