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

            // Only serialize 'visible' if it's false, as true is default implication by Esri's behavior
            // However, spec says "All other fields are set to true by default."
            // For clarity and to match user's example, let's explicitly serialize it.
            // But, geometry field should be false.
            [JsonPropertyName("visible")]
            public bool Visible { get; set; } = true;
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

        public static string SanitizeFileName(string fileName)
        {
            // Basic sanitization, can be expanded
            return string.Join("_", fileName.Split(Path.GetInvalidFileNameChars()));
        }

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
                            // Ensure paths are stored with backslashes for Windows, which Pro expects for MFC paths.
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

                // Initialize DuckDB
                // Connection string for an in-memory database
                // Adding "duckdb_extensions" path for out-of-the-box experience on dev machine.
                // string duckDbExtensionsFolder = Path.Combine(addinExecutingPath, "Extensions");
                // string duckDbInitCommands = $"SET extension_directory='{duckDbExtensionsFolder.Replace('\\', '/')}'; LOAD spatial;";

                // For bundled extensions, it's safer to handle potential path issues.
                string extensionsPath = Path.Combine(addinExecutingPath, "Extensions");
                string normalizedExtensionsPath = extensionsPath.Replace('\\', '/');


                using (var duckDBConnection = new DuckDBConnection("DataSource=:memory:"))
                {
                    await duckDBConnection.OpenAsync();
                    using (var cmd = duckDBConnection.CreateCommand())
                    {
                        // Try to load extensions
                        try
                        {
                            cmd.CommandText = $"SET extension_directory='{normalizedExtensionsPath}'; LOAD spatial;";
                            await cmd.ExecuteNonQueryAsync();
                            logAction("DuckDB spatial extension loaded.");
                        }
                        catch (Exception extEx)
                        {
                            logAction($"Warning: Could not load DuckDB spatial extension from '{normalizedExtensionsPath}'. MFC generation might fail if Parquet files require it for inspection. Error: {extEx.Message}");
                            // Attempting INSTALL as a fallback, though it might require internet/permissions
                            try
                            {
                                cmd.CommandText = "INSTALL spatial; LOAD spatial;";
                                await cmd.ExecuteNonQueryAsync();
                                logAction("DuckDB spatial extension installed and loaded as fallback.");
                            }
                            catch (Exception installEx)
                            {
                                logAction($"Error: Failed to load/install DuckDB spatial extension. Error: {installEx.Message}. Please ensure 'spatial.duckdb_extension' is in '{extensionsPath}'.");
                                return false;
                            }
                        }
                    }

                    foreach (var dirPath in datasetDirectories)
                    {
                        string datasetName = new DirectoryInfo(dirPath).Name;
                        logAction($"Processing dataset: {datasetName}");

                        var parquetFiles = Directory.GetFiles(dirPath, "*.parquet");
                        if (!parquetFiles.Any())
                        {
                            logAction($"No .parquet files found in {dirPath} for dataset {datasetName}. Skipping.");
                            continue;
                        }

                        string sampleParquetFile = parquetFiles.First().Replace('\\', '/'); // DuckDB prefers forward slashes
                        logAction($"Using sample file for schema: {sampleParquetFile}");

                        var dataset = new MfcDataset
                        {
                            Name = datasetName,
                            Alias = datasetName, // Or make it more friendly if needed
                            FieldsList = new List<MfcField>()
                        };

                        // Get Schema
                        try
                        {
                            using (var cmd = duckDBConnection.CreateCommand())
                            {
                                // Ensure the Parquet path is correctly formatted for DuckDB
                                cmd.CommandText = $"DESCRIBE SELECT * FROM read_parquet('{sampleParquetFile.Replace("'", "''")}') LIMIT 0;";
                                using (var reader = await cmd.ExecuteReaderAsync())
                                {
                                    while (await reader.ReadAsync())
                                    {
                                        string colName = reader.GetString(0);
                                        string duckDbType = reader.GetString(1).ToUpperInvariant();

                                        string mfcType;
                                        if (colName.Equals("geometry", StringComparison.OrdinalIgnoreCase))
                                        {
                                            mfcType = "Binary"; // Explicitly set geometry to Binary
                                        }
                                        else
                                        {
                                            mfcType = MapDuckDbTypeToMfcType(duckDbType, logAction);
                                        }

                                        dataset.FieldsList.Add(new MfcField
                                        {
                                            Name = colName,
                                            Type = mfcType,
                                            Visible = !colName.Equals("geometry", StringComparison.OrdinalIgnoreCase) // Geometry field not visible
                                        });
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            logAction($"Error describing schema for {sampleParquetFile}: {ex.Message}");
                            continue; // Skip this dataset
                        }


                        // Get Geometry Type
                        string esriGeomType = "esriGeometryPoint"; // Default
                        bool geomFound = false;
                        if (dataset.FieldsList.Any(f => f.Name.Equals("geometry", StringComparison.OrdinalIgnoreCase)))
                        {
                            try
                            {
                                using (var cmd = duckDBConnection.CreateCommand())
                                {
                                    cmd.CommandText = $"SELECT DISTINCT ST_GeometryType(geometry) FROM read_parquet('{sampleParquetFile.Replace("'", "''")}') WHERE geometry IS NOT NULL LIMIT 1;";
                                    var duckGeomType = (await cmd.ExecuteScalarAsync())?.ToString();
                                    if (!string.IsNullOrEmpty(duckGeomType))
                                    {
                                        esriGeomType = MapDuckDbGeomTypeToEsriGeomType(duckGeomType.ToUpperInvariant(), logAction);
                                        geomFound = true;
                                    }
                                    else
                                    {
                                        logAction($"No geometry type found for {sampleParquetFile} via ST_GeometryType. Defaulting to Point.");
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                logAction($"Error getting geometry type for {sampleParquetFile}. Defaulting to Point. Error: {ex.Message}");
                            }
                        }
                        else
                        {
                            logAction($"No 'geometry' column found for {sampleParquetFile}. Cannot determine geometry type.");
                        }


                        if (geomFound || dataset.FieldsList.Any(f => f.Name.Equals("geometry", StringComparison.OrdinalIgnoreCase))) // Proceed if geometry column exists even if type not detected
                        {
                            dataset.Geometry = new MfcGeometry
                            {
                                GeometryType = esriGeomType,
                                SpatialReference = new MfcSpatialReference { Wkid = 4326 }, // Assume WGS84
                                Fields = new List<MfcGeometryField>
                                {
                                    new MfcGeometryField { Name = "geometry", Formats = new List<string> { "WKB" } }
                                }
                            };
                        }
                        else
                        {
                            logAction($"Dataset {datasetName} has no geometry column or geometry type could not be determined. MFC will not have geometry section for it.");
                        }
                        mfcRoot.Datasets.Add(dataset);
                    }
                }

                var options = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull // Or WhenWritingDefault if prefer
                };
                string jsonString = JsonSerializer.Serialize(mfcRoot, options);

                await File.WriteAllTextAsync(outputMfcFilePath, jsonString);
                logAction($"MFC file successfully generated at {outputMfcFilePath}");
                return true;
            }
            catch (Exception ex)
            {
                logAction($"Error generating MFC file: {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        private static string MapDuckDbTypeToMfcType(string duckDbType, Action<string> logAction)
        {
            // Basic mapping, can be expanded
            // Based on spec: Int8, Int16, Int32, Int64, Float32, Float64, String, Binary, Date
            // DuckDB types: BIGINT, BOOLEAN, BLOB, DATE, DOUBLE, INTEGER, FLOAT, VARCHAR, TIMESTAMP, etc.
            if (duckDbType.StartsWith("DECIMAL")) return "Float64"; // Or String if precision is critical
            if (duckDbType.StartsWith("VARCHAR") || duckDbType.Contains("CHAR") || duckDbType == "TEXT") return "String";

            switch (duckDbType)
            {
                case "BOOLEAN": return "String"; // MFC has no direct boolean field, represent as string "true"/"false" or 0/1 if preferred and mapped to Int
                case "TINYINT": return "Int8"; // ArcGIS Pro shows as Short
                case "SMALLINT": return "Int16"; // ArcGIS Pro shows as Short
                case "INTEGER": return "Int32"; // ArcGIS Pro shows as Long
                case "BIGINT": return "Int64"; // ArcGIS Pro shows as Double - this is per spec.
                case "HUGEINT": return "String"; // Too large for standard types
                case "FLOAT": return "Float32";
                case "REAL": return "Float32";
                case "DOUBLE": return "Float64";
                case "DATE": return "Date";
                case "TIMESTAMP": return "String"; // Or Int64 if epoch. MFC spec supports formatting for 'Date' type, but timestamp might be complex.
                case "TIMESTAMPTZ": return "String";
                case "TIME": return "String";
                case "INTERVAL": return "String";
                case "BLOB": return "Binary";
                case "BYTEA": return "Binary";
                // STRUCT, LIST, MAP - would need specific handling, for now map to string or skip
                default:
                    logAction($"Unmapped DuckDB type: {duckDbType}. Defaulting to String.");
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