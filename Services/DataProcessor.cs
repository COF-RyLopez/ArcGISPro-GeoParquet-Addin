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
        private const string S3_REGION = "us-west-2";
        private DuckDBConnection _connection;
        // Property to hold the output folder for the session.
        private string _outputFolder;

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
                _connection = new DuckDBConnection("DataSource=:memory:");
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

                    string geometryClause = "ST_Transform(geometry, 'EPSG:4326', 'EPSG:4326') AS geometry";
                    string selectClause = string.Join(",\n    ", selectColumns);
                    string fullSelect = $@"
                SELECT 
                    {selectClause},
                    {geometryClause}
                FROM current_table
            ";

                    if (layerNameBase.IndexOf("infrastructure", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
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

                        foreach (var geomType in geomTypes)
                        {
                            string filteredQuery = $@"
                        SELECT 
                            {selectClause},
                            {geometryClause}
                        FROM current_table
                        WHERE ST_GeometryType(geometry) = '{geomType}'
                    ";

                            string layerName = $"Overture {layerNameBase} - {releaseVersion} - {geomType}";
                            string shpPath = Path.Combine(outputFolder, $"{layerName.Replace(" ", "_")}.shp");

                            progress?.Report($"Exporting {layerName} to shapefile...");

                            // Before running your COPY command, check if the shapefile exists.
                            if (File.Exists(shpPath))
                            {
                                // Prompt the user using the native Pro MessageBox.
                                var result = ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                                    "A shapefile with the same name already exists. " +
                                    "Would you like to overwrite it (Yes), append new data (No), or cancel the export (Cancel)?",
                                    "File Exists",
                                    MessageBoxButton.YesNoCancel,
                                    MessageBoxImage.Question);

                                if (result == MessageBoxResult.Yes)
                                {
                                    // Overwrite: Delete the existing shapefile and its associated files.
                                    DeleteShapefile(shpPath);
                                }
                                else if (result == MessageBoxResult.No)
                                {
                                    // Append: Here you would adapt your logic to open the existing shapefile
                                    // and add new rows to it. For simplicity, you can continue using the same shpPath.
                                    // (Note: appending to a shapefile may require using an InsertCursor on the existing table.)
                                }
                                else
                                {
                                    // Cancel export.
                                    progress?.Report("Export canceled by user.");
                                    return;
                                }
                            }

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
                    }
                    else
                    {
                        string layerName = $"Overture {layerNameBase} - {releaseVersion}";
                        string shpPath = Path.Combine(outputFolder, $"{layerName.Replace(" ", "_")}.shp");

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
                throw new Exception($"Failed to create feature layer: {ex.Message}", ex);
            }
        }



        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
