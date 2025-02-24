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
using ArcGIS.Desktop.Core.Geoprocessing;

namespace DuckDBGeoparquet.Services
{
    public class DataProcessor : IDisposable
    {
        private const string S3_REGION = "us-west-2";
        private DuckDBConnection _connection;

        public async Task InitializeDuckDBAsync()
        {
            try
            {
                // Create a new in-memory DuckDB instance.
                _connection = new DuckDBConnection("DataSource=:memory:");
                await _connection.OpenAsync();

                // Install and load required extensions.
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
                    // Check if the path exists by attempting to read a zero-row sample.
                    command.CommandText = $@"
                        CREATE OR REPLACE TABLE temp AS 
                        SELECT * FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1) LIMIT 0;
                    ";
                    await command.ExecuteNonQueryAsync(CancellationToken.None);

                    // Log schema from the sample table.
                    command.CommandText = "DESCRIBE temp";
                    using (var reader = await command.ExecuteReaderAsync(CancellationToken.None))
                    {
                        System.Diagnostics.Debug.WriteLine("Schema from Parquet:");
                        while (await reader.ReadAsync())
                        {
                            System.Diagnostics.Debug.WriteLine($"Column: {reader["column_name"]}, Type: {reader["column_type"]}");
                        }
                    }

                    // Apply spatial filter if extent is provided.
                    string spatialFilter = "";
                    if (extent != null)
                    {
                        spatialFilter = $@"
                            WHERE bbox.xmin >= {extent.XMin}
                              AND bbox.ymin >= {extent.YMin}
                              AND bbox.xmax <= {extent.XMax}
                              AND bbox.ymax <= {extent.YMax}";
                        System.Diagnostics.Debug.WriteLine($"Applying spatial filter: {spatialFilter}");
                    }

                    // Create the actual table with the spatial filter applied.
                    string query = $@"
                        CREATE OR REPLACE TABLE current_table AS 
                        SELECT *
                        FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                        {spatialFilter}
                    ";
                    System.Diagnostics.Debug.WriteLine($"Executing query: {query}");
                    command.CommandText = query;
                    await command.ExecuteNonQueryAsync(CancellationToken.None);

                    // Log row count.
                    command.CommandText = "SELECT COUNT(*) FROM current_table";
                    var count = await command.ExecuteScalarAsync(CancellationToken.None);
                    System.Diagnostics.Debug.WriteLine($"Loaded {count} rows");

                    // Log final table structure.
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

        /// <summary>
        /// Exports the current_table to shapefile(s) using DuckDB's GDAL export.
        /// If the theme (e.g., "infrastructure") has mixed geometries, the method queries for distinct geometry types
        /// and loops through them to write each type to a separate shapefile.
        /// For other themes, it exports directly.
        /// Field selection is built dynamically—unsupported types (STRUCT, MAP, or any type containing '[') are omitted.
        /// </summary>
        public async Task CreateFeatureLayerAsync(string layerNameBase, string releaseVersion, IProgress<string> progress = null)
        {
            string tempDir = "";
            try
            {
                await QueuedTask.Run(async () =>
                {
                    progress?.Report("Initializing...");
                    var map = MapView.Active?.Map;
                    if (map == null)
                        throw new Exception("No active map found");

                    // Set up a temporary directory.
                    tempDir = Path.Combine(Project.Current.HomeFolderPath, $"temp_shp_{Guid.NewGuid()}");
                    Directory.CreateDirectory(tempDir);

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
                                // Skip the geometry column and unsupported types.
                                if (colName.Equals("geometry", StringComparison.OrdinalIgnoreCase) ||
                                    colType.StartsWith("STRUCT", StringComparison.OrdinalIgnoreCase) ||
                                    colType.IndexOf("MAP(", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                    colType.Contains("["))
                                {
                                    continue;
                                }
                                // Shapefiles have field name limitations (usually 10 characters).
                                string safeName = colName.Length > 10 ? colName.Substring(0, 10) : colName;
                                selectColumns.Add($"{colName} AS {safeName}");
                            }
                        }
                    }

                    // Always include the geometry column (transformed).
                    string geometryClause = "ST_Transform(geometry, 'EPSG:4326', 'EPSG:4326') AS geometry";
                    string selectClause = string.Join(",\n    ", selectColumns);
                    string fullSelect = $@"
                        SELECT 
                            {selectClause},
                            {geometryClause}
                        FROM current_table
                    ";

                    // Check if the layerNameBase indicates a theme with mixed geometries.
                    if (layerNameBase.IndexOf("infrastructure", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        // Query distinct geometry types.
                        List<string> geomTypes = new List<string>();
                        using (var command = _connection.CreateCommand())
                        {
                            command.CommandText = "SELECT DISTINCT ST_GeometryType(geometry) AS geom_type FROM current_table";
                            using (var reader = await command.ExecuteReaderAsync(CancellationToken.None))
                            {
                                while (await reader.ReadAsync())
                                {
                                    geomTypes.Add(reader["geom_type"].ToString());
                                }
                            }
                        }

                        // Loop through each geometry type and export separately.
                        foreach (var geomType in geomTypes)
                        {
                            // Build a query that filters for this geometry type.
                            string filteredQuery = $@"
                                SELECT 
                                    {selectClause},
                                    {geometryClause}
                                FROM current_table
                                WHERE ST_GeometryType(geometry) = '{geomType}'
                            ";

                            // Build a layer name that includes the theme, release version, and geometry type.
                            string layerName = $"Overture {layerNameBase} - {releaseVersion} - {geomType}";
                            // For shapefile export, output path must end with .shp.
                            string shpPath = Path.Combine(tempDir, $"{layerName.Replace(" ", "_")}.shp");

                            progress?.Report($"Exporting {layerName} to shapefile...");

                            using (var command = _connection.CreateCommand())
                            {
                                command.CommandText = $@"
                                    COPY (
                                        {filteredQuery}
                                    ) TO '{shpPath.Replace('\\', '/')}' 
                                    WITH (
                                        FORMAT GDAL,
                                        DRIVER 'ESRI Shapefile',
                                        SRS 'EPSG:4326'
                                    );
                                ";
                                System.Diagnostics.Debug.WriteLine($"Export command for {layerName}: {command.CommandText}");
                                await command.ExecuteNonQueryAsync();
                            }

                            // Verify the shapefile was created.
                            if (!File.Exists(shpPath))
                                throw new Exception($"Failed to create shapefile at {shpPath}");
                            var fileInfo = new FileInfo(shpPath);
                            if (fileInfo.Length == 0)
                                throw new Exception("Generated shapefile is empty: " + shpPath);
                            progress?.Report($"{layerName} exported successfully.");

                            // Add the shapefile to the active map.
                            await QueuedTask.Run(() =>
                            {
                                LayerFactory.Instance.CreateLayer(new Uri(shpPath), map);
                            });
                            progress?.Report($"{layerName} added to map.");
                        }
                    }
                    else
                    {
                        // For themes with a single geometry type, export directly.
                        string layerName = $"Overture {layerNameBase} - {releaseVersion}";
                        string shpPath = Path.Combine(tempDir, $"{layerName.Replace(" ", "_")}.shp");

                        progress?.Report($"Exporting {layerName} to shapefile...");

                        using (var command = _connection.CreateCommand())
                        {
                            command.CommandText = $@"
                                COPY (
                                    {fullSelect}
                                ) TO '{shpPath.Replace('\\', '/')}' 
                                WITH (
                                    FORMAT GDAL,
                                    DRIVER 'ESRI Shapefile',
                                    SRS 'EPSG:4326'
                                );
                            ";
                            System.Diagnostics.Debug.WriteLine($"Export command: {command.CommandText}");
                            await command.ExecuteNonQueryAsync();
                        }

                        if (!File.Exists(shpPath))
                            throw new Exception($"Failed to create shapefile at {shpPath}");
                        var fileInfo = new FileInfo(shpPath);
                        if (fileInfo.Length == 0)
                            throw new Exception("Generated shapefile is empty: " + shpPath);
                        progress?.Report($"{layerName} exported successfully.");

                        await QueuedTask.Run(() =>
                        {
                            LayerFactory.Instance.CreateLayer(new Uri(shpPath), map);
                        });
                        progress?.Report($"{layerName} added to map.");
                    }
                });
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Detailed error: {ex}");
                progress?.Report($"Error: {ex.Message}");
                throw new Exception($"Failed to create feature layer: {ex.Message}", ex);
            }
            finally
            {
                try
                {
                    if (!string.IsNullOrEmpty(tempDir) && Directory.Exists(tempDir))
                    {
                        // Optionally, delete temporary files after verification.
                        // Directory.Delete(tempDir, true);
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Warning - Failed to clean up temp files: {ex.Message}");
                }
            }
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
