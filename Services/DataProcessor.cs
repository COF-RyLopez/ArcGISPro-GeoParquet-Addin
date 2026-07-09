using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
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
    public class DataProcessor : IDisposable
    {
        private readonly DuckDBManager _duckDb;
        private readonly GeocoderEngine _geocoder;

        public readonly LayerManager LayerManager;
        public readonly ParquetExporter ParquetExporter;

        // Proxies keep the many existing call sites unchanged while the
        // connection lifecycle lives in DuckDBManager (Phase 2c stage 2).
        private DuckDBConnection _connection => _duckDb.Connection;
        private bool _isInitialized => _duckDb.IsInitialized;

        // Constants
        private const string DEFAULT_SRS = "EPSG:4326";
        private const string STRUCT_TYPE = "STRUCT";

// Add fields for state management
        private bool _isDisposed;

        // Add a class-level field to store the current extent
        private ExtentBounds _currentExtent;


        private string _outputSessionSuffix = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);

        /// <summary>
        /// When set, layers added to the map will receive cartographic symbology
        /// matching this style definition instead of default ArcGIS Pro symbology.
        /// </summary>
        public MapStyleDefinition SelectedMapStyle { get; set; }
        public CartographicProfile SelectedCartographicProfile { get; set; }




        public int LastAddedLayerCount => LayerManager.LastAddedLayerCount;
        public string LastAppliedStyleName => LayerManager.LastAppliedStyleName;

        public DataProcessor()
        {
            _duckDb = new DuckDBManager();
            _geocoder = new GeocoderEngine(_duckDb);
            LayerManager = new LayerManager();
            ParquetExporter = new ParquetExporter(_duckDb, LayerManager);
        }

        private static void SaveDiagnosticLog(string prefix, string content)
        {
            try
            {
                var root = AppDomain.CurrentDomain.BaseDirectory;
                var docs = System.IO.Path.Combine(root, "docs", "diagnostics");
                System.IO.Directory.CreateDirectory(docs);
                var filename = System.IO.Path.Combine(docs, $"{prefix}_{DateTime.UtcNow:yyyyMMdd_HHmmss_fff}.log");
                System.IO.File.WriteAllText(filename, content);
                System.Diagnostics.Debug.WriteLine($"Saved diagnostic log: {filename}");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to save diagnostic log: {ex.Message}");
            }
        }

        /// <summary>
        /// Starts a new output session so exported parquet file paths are unique per load.
        /// This prevents ArcGIS from reusing stale cache metadata tied to a previous file path.
        /// </summary>
        public void BeginNewOutputSession()
        {
            _outputSessionSuffix = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
        }

        public Task InitializeDuckDBAsync(CancellationToken cancellationToken = default)
            => _duckDb.InitializeAsync(cancellationToken);

        /// <summary>
        /// Exports a small GeoJSON sample of an Overture S3 dataset for the
        /// preview map — a cheap dry-run of a load. Uses the same bbox
        /// pushdown filter as the real load but skips ST_Intersects and caps
        /// the row count, so it returns in seconds. Only id + geometry are
        /// selected: flat columns keep the GDAL GeoJSON writer happy and the
        /// output small.
        /// </summary>
        /// <param name="s3Path">Overture S3 glob (same format as IngestFileAsync).</param>
        /// <param name="outputGeoJsonPath">Destination .geojson file path.</param>
        /// <param name="extent">Optional WGS84 extent filter.</param>
        /// <param name="maxFeatures">Row cap for the sample.</param>
        public Task ExportPreviewSampleAsync(string s3Path, string outputGeoJsonPath, ExtentBounds extent = null, int maxFeatures = 2000, CancellationToken cancellationToken = default)
            => ParquetExporter.ExportPreviewSampleAsync(s3Path, outputGeoJsonPath, extent, maxFeatures, cancellationToken);

        public async Task<bool> IngestFileAsync(
            string s3Path,
            ExtentBounds extent = null,
            string actualS3Type = null,
            IProgress<string> progress = null,
            CancellationToken cancellationToken = default,
            bool includeSourceProvenance = false)
        {
            try
            {
                progress?.Report($"Connecting to S3: {actualS3Type ?? "data"}...");

                // Store the extent for later use in CreateFeatureLayerAsync
                _currentExtent = extent;

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

                string typeLabel = actualS3Type ?? "data";
                string readParquetOptions = ShouldUseFastS3Read(s3Path)
                    ? S3Ingester.S3FastReadParquetOptions
                    : S3Ingester.S3ReadParquetOptions;

                // Create a fresh command for this operation to ensure clean state
                using var command = _connection.CreateCommand();
                var schemaStopwatch = Stopwatch.StartNew();
                List<string> columnNames;
                try
                {
                    columnNames = await LoadS3SchemaAsync(command, s3Path, typeLabel, readParquetOptions, cancellationToken);
                }
                catch (Exception ex) when (!UsesUnionByName(readParquetOptions))
                {
                    System.Diagnostics.Debug.WriteLine($"[{typeLabel}] Fast schema read failed, retrying with union_by_name: {ex.Message}");
                    readParquetOptions = S3Ingester.S3ReadParquetOptions;
                    columnNames = await LoadS3SchemaAsync(command, s3Path, typeLabel, readParquetOptions, cancellationToken);
                }
                schemaStopwatch.Stop();
                System.Diagnostics.Debug.WriteLine(
                    $"[perf][ingest:{typeLabel}] schema columns={columnNames.Count} read={DescribeReadOptions(readParquetOptions)} provenance={includeSourceProvenance} elapsed={FormatElapsed(schemaStopwatch.Elapsed)}");

                var flattenedFields = S3Ingester.GetFlattenedFieldNames(actualS3Type, columnNames, includeSourceProvenance);
                var schemaReport = OvertureSchema.BuildCompatibilityReport(actualS3Type, columnNames, flattenedFields);
                ReportSchemaCompatibility(typeLabel, schemaReport, progress);

                progress?.Report($"Loading data from S3 (this may take a moment)...");

                if (extent != null)
                {
                    System.Diagnostics.Debug.WriteLine($"[{typeLabel}] Spatial filter: requested extent=({GeoParquetSql.FormatCoordinate(extent.XMin)}, {GeoParquetSql.FormatCoordinate(extent.YMin)}) to ({GeoParquetSql.FormatCoordinate(extent.XMax)}, {GeoParquetSql.FormatCoordinate(extent.YMax)}), bbox overlap + ST_Intersects");
                    progress?.Report($"Applying spatial filter for current map extent...");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("WARNING: No extent provided - loading all data without spatial filter!");
                }

                // Build query with optional column projection and geometry clipping.
                // The pure builder trims heavy structs/arrays when safe and applies
                // the bbox pushdown + ST_Intersects filter and clipping (see S3Ingester).
                string query = S3Ingester.BuildLoadQuery(s3Path, actualS3Type, columnNames, extent, includeSourceProvenance, readParquetOptions);

                // Clear command text before setting new query to ensure clean state
                command.CommandText = string.Empty;
                command.CommandText = query;
                
                System.Diagnostics.Debug.WriteLine($"Executing data load query for {typeLabel}...");
                var loadStopwatch = Stopwatch.StartNew();
                try
                {
                    await ExecuteNonQueryWithRetriesAsync(command, typeLabel, cancellationToken);
                }
                catch (Exception ex) when (!UsesUnionByName(readParquetOptions))
                {
                    System.Diagnostics.Debug.WriteLine($"[{typeLabel}] Fast data load failed, retrying with union_by_name: {ex.Message}");
                    readParquetOptions = S3Ingester.S3ReadParquetOptions;
                    columnNames = await LoadS3SchemaAsync(command, s3Path, typeLabel, readParquetOptions, cancellationToken);
                    query = S3Ingester.BuildLoadQuery(s3Path, actualS3Type, columnNames, extent, includeSourceProvenance, readParquetOptions);
                    command.CommandText = query;
                    await ExecuteNonQueryWithRetriesAsync(command, typeLabel, cancellationToken);
                }
                loadStopwatch.Stop();
                System.Diagnostics.Debug.WriteLine(
                    $"[perf][ingest:{typeLabel}] s3-load read={DescribeReadOptions(readParquetOptions)} provenance={includeSourceProvenance} elapsed={FormatElapsed(loadStopwatch.Elapsed)}");

                var rowCountStopwatch = Stopwatch.StartNew();
                command.CommandText = "SELECT COUNT(*) FROM current_table";
                if (string.IsNullOrWhiteSpace(command.CommandText))
                {
                    System.Diagnostics.Debug.WriteLine("Count query was not set before execution");
                    return false;
                }
                var count = await ExecuteScalarWithRetriesAsync(command, cancellationToken);
                rowCountStopwatch.Stop();
                progress?.Report($"Successfully loaded {count:N0} rows from S3");
                System.Diagnostics.Debug.WriteLine($"[{typeLabel}] Loaded {count} rows");
                System.Diagnostics.Debug.WriteLine(
                    $"[perf][ingest:{typeLabel}] row-count rows={count} provenance={includeSourceProvenance} elapsed={FormatElapsed(rowCountStopwatch.Elapsed)}");

                // Early exit for empty datasets to avoid unnecessary processing
                if (Convert.ToInt64(count) == 0)
                {
                    progress?.Report("Dataset is empty - no features to process");
                    System.Diagnostics.Debug.WriteLine("Early exit: Dataset contains no rows");
                    return false; // Indicate no data to process
                }

                return true;
            }
            catch (OperationCanceledException ex)
            {
                System.Diagnostics.Debug.WriteLine($"Ingest cancelled for {s3Path}: {ex.Message}");
                progress?.Report("Ingest operation cancelled");
                return false;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Query error: {ex.Message}");
                return false;
            }
        }

        private static async Task ExecuteNonQueryWithRetriesAsync(DuckDB.NET.Data.DuckDBCommand command, string label, CancellationToken cancellationToken)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            if (string.IsNullOrWhiteSpace(command.CommandText))
                throw new InvalidOperationException("Command text is empty before ExecuteNonQueryWithRetriesAsync");

            int maxAttempts = 3;
            int attemptExec = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (true)
            {
                attemptExec++;
                try
                {
                    System.Diagnostics.Debug.WriteLine($"[{label}] ExecuteNonQuery attempt {attemptExec}");
                    await command.ExecuteNonQueryAsync(cancellationToken);
                    sw.Stop();
                    System.Diagnostics.Debug.WriteLine($"[{label}] ExecuteNonQuery succeeded in {sw.ElapsedMilliseconds}ms on attempt {attemptExec}");
                    return;
                }
                catch (OperationCanceledException)
                {
                    System.Diagnostics.Debug.WriteLine($"[{label}] ExecuteNonQuery cancelled on attempt {attemptExec}");
                    throw;
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[{label}] ExecuteNonQuery attempt {attemptExec} failed: {ex.Message}");
                    if (attemptExec >= maxAttempts || cancellationToken.IsCancellationRequested)
                    {
                        System.Diagnostics.Debug.WriteLine($"[{label}] ExecuteNonQuery giving up after {attemptExec} attempts");
                        throw;
                    }
                    // Backoff
                    int delayMs = 250 * attemptExec; // 250ms, 500ms, ...
                    try { await Task.Delay(delayMs, cancellationToken); } catch { throw; }
                }
            }
        }

        private static async Task<object> ExecuteScalarWithRetriesAsync(DuckDB.NET.Data.DuckDBCommand command, CancellationToken cancellationToken)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            if (string.IsNullOrWhiteSpace(command.CommandText))
                throw new InvalidOperationException("Command text is empty before ExecuteScalarWithRetriesAsync");

            int maxAttempts = 3;
            int attemptScalar = 0;
            while (true)
            {
                attemptScalar++;
                try
                {
                    return await command.ExecuteScalarAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    System.Diagnostics.Debug.WriteLine($"ExecuteScalar cancelled on attempt {attemptScalar}");
                    throw;
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"ExecuteScalar attempt {attemptScalar} failed: {ex.Message}");
                    if (attemptScalar >= maxAttempts || cancellationToken.IsCancellationRequested)
                        throw;
                    int delayMs = 250 * attemptScalar;
                    try { await Task.Delay(delayMs, cancellationToken); } catch { throw; }
                }
            }
        }

        private static bool ShouldUseFastS3Read(string s3Path)
        {
            return !string.IsNullOrWhiteSpace(s3Path) &&
                   s3Path.Contains("s3://overturemaps", StringComparison.OrdinalIgnoreCase) &&
                   s3Path.Contains("/release/", StringComparison.OrdinalIgnoreCase);
        }

        private static bool UsesUnionByName(string readParquetOptions)
        {
            return !string.IsNullOrWhiteSpace(readParquetOptions) &&
                   readParquetOptions.Contains("union_by_name", StringComparison.OrdinalIgnoreCase);
        }

        private static string DescribeReadOptions(string readParquetOptions)
        {
            return UsesUnionByName(readParquetOptions) ? "union_by_name" : "fast";
        }

        private static string FormatElapsed(TimeSpan elapsed)
        {
            return elapsed.TotalSeconds >= 1
                ? $"{elapsed.TotalSeconds:F2}s"
                : $"{elapsed.TotalMilliseconds:F0}ms";
        }

        private static async Task<List<string>> LoadS3SchemaAsync(
            DuckDBCommand command,
            string s3Path,
            string typeLabel,
            string readParquetOptions,
            CancellationToken cancellationToken)
        {
            string schemaQuery = $@"
                    CREATE OR REPLACE TABLE temp AS 
                    SELECT * FROM {S3Ingester.BuildReadParquetExpression(s3Path, readParquetOptions)} LIMIT 0;
                ";
            command.CommandText = schemaQuery;
            System.Diagnostics.Debug.WriteLine($"Executing schema query for {typeLabel} ({DescribeReadOptions(readParquetOptions)})...");
            try
            {
                command.CommandText = schemaQuery;
                await ExecuteNonQueryWithRetriesAsync(command, typeLabel + "-schema", cancellationToken);
            }
            catch (OperationCanceledException)
            {
                System.Diagnostics.Debug.WriteLine($"Schema query cancelled for {typeLabel}");
                throw;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error executing schema query: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Query that failed: {schemaQuery.Substring(0, Math.Min(300, schemaQuery.Length))}");
                throw;
            }

            command.CommandText = "DESCRIBE temp";
            var columnNames = new List<string>();
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync())
            {
                columnNames.Add(reader.GetString(0));
            }
            System.Diagnostics.Debug.WriteLine($"Schema validated: {columnNames.Count} columns found");
            return columnNames;
        }

        private static void ReportSchemaCompatibility(
            string typeLabel,
            OvertureSchema.SchemaCompatibilityReport report,
            IProgress<string> progress)
        {
            if (report == null)
                return;

            System.Diagnostics.Debug.WriteLine(
                $"[{typeLabel}] Overture schema reference={report.ReferenceSchemaVersion} missing={report.MissingExpectedColumns.Count} unknown={report.UnknownColumns.Count} flattened={report.FlattenedFields.Count}");

            if (report.FlattenedFields.Count > 0)
            {
                progress?.Report($"Schema-aware fields: flattened {report.FlattenedFields.Count} Overture nested field(s): {FormatList(report.FlattenedFields, 8)}");
            }

            if (report.MissingExpectedColumns.Count > 0)
            {
                progress?.Report($"Schema note: {report.MissingExpectedColumns.Count} expected {report.ReferenceSchemaVersion} column(s) were not present for {typeLabel}: {FormatList(report.MissingExpectedColumns, 6)}");
            }

            if (report.UnknownColumns.Count > 0)
            {
                progress?.Report($"Schema note: found {report.UnknownColumns.Count} column(s) not in the {report.ReferenceSchemaVersion} {typeLabel} baseline: {FormatList(report.UnknownColumns, 6)}");
            }
        }

        private static string FormatList(IReadOnlyList<string> values, int maxItems)
        {
            if (values == null || values.Count == 0)
                return "none";

            var shown = values.Take(maxItems).ToList();
            string suffix = values.Count > shown.Count ? $" (+{values.Count - shown.Count} more)" : string.Empty;
            return string.Join(", ", shown) + suffix;
        }

        /// <summary>
        /// Infer ISO 3166-1 alpha-2 country code from extent center (WGS84) for filtering address data.
        /// Returns null if extent center is not in a known bounding box. Reduces address load from global to one country.
        /// </summary>
        private static string TryGetCountryCodeFromExtent(ExtentBounds extent)
        {
            if (extent == null) return null;
            double midLon = (extent.XMin + extent.XMax) / 2.0;
            double midLat = (extent.YMin + extent.YMax) / 2.0;
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

        public async Task<DataTable> GetPreviewDataAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT * FROM current_table LIMIT 1000";
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
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
        public async Task<List<Dictionary<string, object>>> GetSchemaAsync(string s3Path, CancellationToken cancellationToken = default)
        {
            try
            {
                // Use the same approach as IngestFileAsync for consistency
                using var command = _connection.CreateCommand();
                command.CommandText = $@"
                    CREATE OR REPLACE TABLE temp_schema AS 
                    SELECT * FROM read_parquet('{s3Path}', {S3Ingester.S3ReadParquetOptions}) LIMIT 0;
                ";
                await command.ExecuteNonQueryAsync(cancellationToken);

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
        public Task CreateFeatureLayerAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot, string compression = "ZSTD", CancellationToken cancellationToken = default)
            => ParquetExporter.CreateFeatureLayersAsync(layerNameBase, progress, parentS3Theme, actualS3Type, dataOutputPathRoot, _outputSessionSuffix, compression, cancellationToken);




        /// <summary>
        /// Adds all pending layers to the map in optimal stacking order (polygons → lines → points)
        /// </summary>
        public Task AddAllLayersToMapAsync(IProgress<string> progress = null)
            => LayerManager.AddAllLayersToMapAsync(SelectedMapStyle, SelectedCartographicProfile, progress);

/// <summary>
        /// Clears any pending layers (useful for cleanup or reset operations)
        /// </summary>
        public void ClearPendingLayers()
        {
            LayerManager.ClearPendingLayers();
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

        // Geocoding moved to GeocoderEngine (Phase 2c stage 2); these
        // wrappers preserve the public API for existing callers.
        public Task<List<GeocodeCandidate>> SearchAddressCandidatesAsync(string parquetGlobPath, string normalizedQuery, ExtentBounds extent = null, int maxResults = 25, CancellationToken cancellationToken = default)
            => _geocoder.SearchAddressCandidatesAsync(parquetGlobPath, normalizedQuery, extent, maxResults, cancellationToken);

        public Task<List<GeocodeCandidate>> SearchPlaceCandidatesAsync(string parquetGlobPath, string normalizedQuery, ExtentBounds extent = null, int maxResults = 25, CancellationToken cancellationToken = default)
            => _geocoder.SearchPlaceCandidatesAsync(parquetGlobPath, normalizedQuery, extent, maxResults, cancellationToken);

        public void Dispose()
        {
            if (_isDisposed) return;

            _duckDb?.Dispose();
            LayerManager?.ClearPendingLayers();
            _isDisposed = true;
            GC.SuppressFinalize(this);
        }

    }
}

