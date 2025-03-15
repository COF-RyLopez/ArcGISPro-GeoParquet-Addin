using System;
using System.Data;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Framework.Dialogs;
using System.Windows;

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
        
        // Property to hold the output folder for the session.
        private string _outputFolder;

        public DataProcessor()
        {
            _connection = new DuckDBConnection("DataSource=:memory:");
        }

        /// <summary>
        /// Gets (or creates) a single output folder per session.
        /// The folder will be named "OvertureMapsData_<release>".
        /// </summary>
        /// <param name="releaseVersion">The release version string.</param>
        /// <returns>The folder path.</returns>
        private string GetOutputFolder(string releaseVersion)
        {
            if (string.IsNullOrEmpty(_outputFolder))
            {
                // For example, create the folder under the project's HomeFolderPath.
                _outputFolder = Path.Combine(Project.Current.HomeFolderPath, $"OvertureMapsData_{releaseVersion}");
                if (!Directory.Exists(_outputFolder))
                {
                    Directory.CreateDirectory(_outputFolder);
                }
            }
            return _outputFolder;
        }

        public async Task InitializeDuckDBAsync()
        {
            try
            {
                await _connection.OpenAsync();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSTALL spatial;
                        INSTALL httpfs;
                        LOAD spatial;
                        LOAD httpfs;
                        SET s3_region='us-west-2';
                        SET enable_http_metadata_cache=true;
                        SET enable_object_cache=true;
                        SET enable_progress_bar=true;
                    ";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                }
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
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = $@"
                        CREATE OR REPLACE TABLE temp AS 
                        SELECT * FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1) LIMIT 0;
                    ";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);

                    command.CommandText = "DESCRIBE temp";
                    using (var reader = await command.ExecuteReaderAsync(CancellationToken.None))
                    {
                        System.Diagnostics.Debug.WriteLine("Schema from Parquet:");
                        while (await reader.ReadAsync())
                        {
                            System.Diagnostics.Debug.WriteLine($"Column: {reader["column_name"]}, Type: {reader["column_type"]}");
                        }
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
                    using (var reader = await command.ExecuteReaderAsync(CancellationToken.None))
                    {
                        System.Diagnostics.Debug.WriteLine("Final table structure:");
                        while (await reader.ReadAsync())
                        {
                            System.Diagnostics.Debug.WriteLine($"Column: {reader["column_name"]}, Type: {reader["column_type"]}");
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Query error: {ex.Message}");
                throw new Exception($"Failed to ingest file: {ex.Message}", ex);
            }
        }

        public async Task<DataTable> GetPreviewDataAsync()
        {
            try
            {
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "SELECT * FROM current_table LIMIT 1000";
                    using (var reader = await command.ExecuteReaderAsync(CancellationToken.None))
                    {
                        DataTable previewTable = new DataTable();
                        previewTable.Load(reader);
                        return previewTable;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to get preview data: {ex.Message}", ex);
            }
        }


        private void DeleteShapefile(string shpPath)
        {
            // Get the base file path without extension.
            string basePath = Path.Combine(Path.GetDirectoryName(shpPath), Path.GetFileNameWithoutExtension(shpPath));
            // Shapefile typically has these extensions.
            string[] extensions = { ".shp", ".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx" };
            foreach (var ext in extensions)
            {
                string file = basePath + ext;
                if (File.Exists(file))
                {
                    try
                    {
                        File.Delete(file);
                    }
                    catch (Exception ex)
                    {
                        // Handle any deletion errors (e.g., file is locked).
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show($"Failed to delete {file}: {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Exports the current_table to shapefile(s) using DuckDB's GDAL export.
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
                    var map = MapView.Active?.Map;
                    if (map == null)
                        throw new Exception("No active map found");

                    // Build the dynamic SELECT clause from the current_table.
                    List<string> selectColumns = new List<string>();
                    using (var command = _connection.CreateCommand())
                    {
                        command.CommandText = "DESCRIBE current_table";
                        using (var reader = await command.ExecuteReaderAsync(CancellationToken.None))
                        {
                            while (await reader.ReadAsync())
                            {
                                string colName = reader["column_name"].ToString();
                                string colType = reader["column_type"].ToString();
                                if (colName.Equals("geometry", StringComparison.OrdinalIgnoreCase))
                                {
                                    continue;
                                }
                                string safeName = colName.Length > 10 ? colName.Substring(0, 10) : colName;

                                // Flatten STRUCT columns based on the schema reference
                                if (colType.StartsWith("STRUCT", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (colName == "bbox")
                                    {
                                        selectColumns.Add($"bbox.xmin AS bbox_xmin");
                                        selectColumns.Add($"bbox.xmax AS bbox_xmax");
                                        selectColumns.Add($"bbox.ymin AS bbox_ymin");
                                        selectColumns.Add($"bbox.ymax AS bbox_ymax");
                                    }
                                    else if (colName == "sources")
                                    {
                                        selectColumns.Add($"to_json(sources) AS sources");
                                    }
                                    else if (colName == "names")
                                    {
                                        selectColumns.Add($"names.primary AS names_primary");
                                        selectColumns.Add($"to_json(names.common) AS names_common");
                                        selectColumns.Add($"to_json(names.rules) AS names_rules");
                                    }
                                    else if (colName == "categories")
                                    {
                                        selectColumns.Add($"categories.primary AS categories_primary");
                                        selectColumns.Add($"to_json(categories.alternate) AS categories_alternate");
                                    }
                                    else if (colName == "brand")
                                    {
                                        selectColumns.Add($"brand.wikidata AS brand_wikidata");
                                        selectColumns.Add($"to_json(brand.names) AS brand_names");
                                    }
                                    else if (colName == "addresses")
                                    {
                                        selectColumns.Add($"to_json(addresses) AS addresses");
                                    }
                                }
                                else if (colType.StartsWith("MAP", StringComparison.OrdinalIgnoreCase) ||
                                         colType.Contains("["))
                                {
                                    selectColumns.Add($"to_json({colName}) AS {safeName}");
                                }
                                else
                                {
                                    selectColumns.Add($"{colName} AS {safeName}");
                                }
                            }
                        }
                    }

                    // Always export by geometry type for all datasets
                    await ExportByGeometryType(selectColumns, layerNameBase, releaseVersion, outputFolder);
                    progress?.Report($"All {layerNameBase} features added to map.");
                });
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Detailed error: {ex}");
                throw new Exception($"Failed to create feature layer: {ex.Message}", ex);
            }
        }

        private async Task HandleStructColumn(string colName, string colType, List<string> selectColumns)
        {
            if (!colType.StartsWith(STRUCT_TYPE, StringComparison.OrdinalIgnoreCase))
                return;

            if (colName == BBOX_COLUMN)
            {
                selectColumns.Add($"{colName}.minx as bbox_minx");
                selectColumns.Add($"{colName}.miny as bbox_miny");
                selectColumns.Add($"{colName}.maxx as bbox_maxx");
                selectColumns.Add($"{colName}.maxy as bbox_maxy");
                return;
            }

            string safeName = colName.Length > 10 ? colName.Substring(0, 10) : colName;
            using var command = _connection.CreateCommand();
            command.CommandText = $"DESCRIBE {colName}";
            using var reader = await command.ExecuteReaderAsync();
            
            while (await reader.ReadAsync())
            {
                string fieldName = reader.GetString(0);
                string fieldType = reader.GetString(1);
                string safeFieldName = fieldName.Length > 10 ? fieldName.Substring(0, 10) : fieldName;
                selectColumns.Add($"{colName}.{fieldName} as {safeName}_{safeFieldName}");
            }
        }

        private async Task ExportByGeometryType(List<string> selectColumns, string layerNameBase, string releaseVersion, string outputFolder)
        {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT DISTINCT ST_GeometryType(geometry) FROM current_table";
            var geomTypes = new List<string>();
            
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    geomTypes.Add(reader.GetString(0));
                }
            }

            foreach (var geomType in geomTypes)
            {
                // Determine the appropriate shapefile type based on the geometry type
                string shapefileType = "POINT"; // Default
                if (geomType.Contains("POLYGON"))
                {
                    shapefileType = "POLYGON";
                }
                else if (geomType.Contains("LINESTRING") || geomType.Contains("LINE"))
                {
                    shapefileType = "ARC";
                }

                System.Diagnostics.Debug.WriteLine($"Using shapefile type {shapefileType} for geometry type {geomType}");
                
                string filteredQuery = BuildSelectQuery(
                    string.Join(",\n    ", selectColumns), 
                    BuildGeometryClause(), 
                    $"ST_GeometryType(geometry) = '{geomType}'");

                string layerName = $"Overture {layerNameBase} - {releaseVersion} - {geomType}";
                string shpPath = Path.Combine(outputFolder, $"{layerName.Replace(" ", "_")}.shp");
                
                // Pass the shapefile type to BuildCopyCommand for proper format
                await ExportToShapefile(filteredQuery, shpPath, layerName, shapefileType);
                
                if (File.Exists(shpPath))
                {
                    var fileInfo = new FileInfo(shpPath);
                    if (fileInfo.Length > 0)
                    {
                        await QueuedTask.Run(() =>
                        {
                            var map = MapView.Active?.Map;
                            if (map != null)
                            {
                                LayerFactory.Instance.CreateLayer(new Uri(shpPath), map);
                            }
                        });
                    }
                }
            }
        }

        private async Task ExportToShapefile(string query, string outputPath, string layerName, string shapefileType = null)
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = BuildCopyCommand(query, outputPath, shapefileType);
                System.Diagnostics.Debug.WriteLine($"Export command for {layerName}: {command.CommandText}");
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to export shapefile for layer {layerName}", ex);
            }
        }
        
        private string BuildCopyCommand(string selectQuery, string outputPath, string shapefileType = null)
        {
            // If shapefile type is not specified, try to detect it automatically
            if (string.IsNullOrEmpty(shapefileType))
            {
                // First determine the geometry type
                shapefileType = "POINT"; // Default to POINT
                
                try 
                {
                    using var cmd = _connection.CreateCommand();
                    
                    // Get a better view of the actual geometry types
                    cmd.CommandText = "SELECT DISTINCT ST_GeometryType(geometry) FROM current_table LIMIT 10";
                    using var reader = cmd.ExecuteReader();
                    
                    // Check if there are polygon or linestring types
                    bool hasPolygon = false;
                    bool hasLine = false;
                    
                    while (reader.Read())
                    {
                        string currentType = reader.GetString(0).ToUpperInvariant();
                        System.Diagnostics.Debug.WriteLine($"Detected geometry type: {currentType}");
                        
                        if (currentType.Contains("POLYGON"))
                        {
                            hasPolygon = true;
                        }
                        else if (currentType.Contains("LINESTRING") || currentType.Contains("LINE"))
                        {
                            hasLine = true;
                        }
                    }
                    
                    // Prioritize polygon over line over point
                    if (hasPolygon)
                    {
                        shapefileType = "POLYGON";
                    }
                    else if (hasLine)
                    {
                        shapefileType = "ARC";
                    }
                }
                catch (Exception ex)
                {
                    // Log but don't throw
                    System.Diagnostics.Debug.WriteLine($"Error detecting geometry type: {ex.Message}");
                }
            }

            System.Diagnostics.Debug.WriteLine($"Using shapefile geometry type: {shapefileType}");
            
            // Add the appropriate Layer Creation Option
            string shapefileOption = $", LAYER_CREATION_OPTIONS 'SHPT={shapefileType}'";
            
            return $@"
                COPY (
                    {selectQuery}
                ) TO '{outputPath.Replace('\\', '/')}' 
                WITH (
                    FORMAT GDAL,
                    DRIVER 'ESRI Shapefile',
                    SRS '{DEFAULT_SRS}'{shapefileOption}
                );";
        }

        private string BuildSelectQuery(string selectClause, string geometryClause, string? whereClause = null)
        {
            var query = $@"
                SELECT 
                    {selectClause},
                    {geometryClause}
                FROM current_table";
            
            if (!string.IsNullOrEmpty(whereClause))
            {
                query += $@"
                WHERE {whereClause}";
            }
            
            return query;
        }

        private string BuildGeometryClause()
        {
            return $"ST_Transform({GEOMETRY_COLUMN}, '{DEFAULT_SRS}', '{DEFAULT_SRS}') AS {GEOMETRY_COLUMN}";
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
