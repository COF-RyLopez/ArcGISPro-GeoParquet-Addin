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

        private void DeleteParquetFile(string parquetPath)
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
                    var map = MapView.Active?.Map;
                    if (map == null)
                        throw new Exception("No active map found");

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

            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    geomTypes.Add(reader.GetString(0));
                }
            }

            foreach (var geomType in geomTypes)
            {
                progress?.Report($"Processing {geomType} features...");

                // Build query that preserves all original columns including complex types
                string filteredQuery = $@"
                    SELECT 
                        *,
                        ST_Transform({GEOMETRY_COLUMN}, '{DEFAULT_SRS}', '{DEFAULT_SRS}') AS {GEOMETRY_COLUMN}
                    FROM current_table
                    WHERE ST_GeometryType(geometry) = '{geomType}'";

                // Simplify the layer naming but keep geometry type suffix to avoid file collisions
                string shortGeomType = GetShortGeometryType(geomType);
                string layerName = $"{layerNameBase}_{shortGeomType}";
                string parquetPath = Path.Combine(outputFolder, $"{layerName}.parquet");

                progress?.Report($"Exporting {layerName} to GeoParquet...");
                await ExportToGeoParquet(filteredQuery, parquetPath, layerName);

                if (File.Exists(parquetPath))
                {
                    var fileInfo = new FileInfo(parquetPath);
                    if (fileInfo.Length > 0)
                    {
                        progress?.Report($"Adding {layerName} to map...");
                        await QueuedTask.Run(() =>
                        {
                            var map = MapView.Active?.Map;
                            if (map != null)
                            {
                                LayerFactory.Instance.CreateLayer(new Uri(parquetPath), map);
                            }
                        });
                    }
                }
            }
        }

        private async Task ExportToGeoParquet(string query, string outputPath, string layerName)
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = BuildGeoParquetCopyCommand(query, outputPath);
                System.Diagnostics.Debug.WriteLine($"Export command for {layerName}: {command.CommandText}");
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to export GeoParquet for layer {layerName}", ex);
            }
        }

        private string BuildGeoParquetCopyCommand(string selectQuery, string outputPath)
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

        // Helper method to get a shorter geometry type name
        private string GetShortGeometryType(string geomType)
        {
            switch (geomType)
            {
                case "POINT":
                    return "PT";
                case "LINESTRING":
                    return "LN";
                case "POLYGON":
                    return "PG";
                case "MULTIPOINT":
                    return "MPT";
                case "MULTILINESTRING":
                    return "MLN";
                case "MULTIPOLYGON":
                    return "MPG";
                default:
                    return geomType;
            }
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
