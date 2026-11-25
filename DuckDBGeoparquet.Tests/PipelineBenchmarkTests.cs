using DuckDB.NET.Data;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace DuckDBGeoparquet.Tests
{
    /// <summary>
    /// Opt-in, network-bound benchmark of the download pipeline. It runs the
    /// same SQL sequence as DataProcessor.PrepareDatasetAsync and
    /// ParquetExporter.CreateFeatureLayersAsync (schema probe, load, row count,
    /// _source archive, display table, per-geometry-type export) against real
    /// Overture/OSM data, built by the same builders, and reports time per
    /// stage. ArcGIS map drawing is not covered; it only runs inside Pro.
    ///
    /// Off by default so the normal suite stays offline and fast. Run with:
    ///   GEOPARQUET_BENCH=1 dotnet test DuckDBGeoparquet.Tests --filter "FullyQualifiedName~PipelineBenchmark" --logger "console;verbosity=detailed"
    /// Optional: GEOPARQUET_BENCH_RELEASE (default below), GEOPARQUET_BENCH_SCENARIOS
    /// (comma list of scenario names), GEOPARQUET_BENCH_OUT (markdown report path),
    /// GEOPARQUET_BENCH_PROVENANCE=1 (display provenance columns), GEOPARQUET_BENCH_EXTRA_SQL
    /// (SQL run after the session settings, for trying DuckDB settings).
    /// </summary>
    public sealed class PipelineBenchmarkTests : IDisposable
    {
        private const string DefaultRelease = "2026-11-18.0";
        private const string OvertureS3 = "s3://overturemaps-us-west-2/release";
        private const string OvertureHttps = "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release";

        // Downtown Fresno (~4 km square) and the Fresno/Clovis metro (~30 x 28 km).
        private static readonly ExtentBounds Small = new(-119.80, 36.72, -119.76, 36.76);
        private static readonly ExtentBounds Large = new(-119.95, 36.65, -119.60, 36.90);

        private readonly ITestOutputHelper _output;
        private readonly string _folder = Path.Combine(Path.GetTempPath(), "geoparquet-bench-" + Guid.NewGuid().ToString("N"));

        public PipelineBenchmarkTests(ITestOutputHelper output)
        {
            _output = output;
            Directory.CreateDirectory(_folder);
        }

        public void Dispose()
        {
            try { Directory.Delete(_folder, true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
        }

        private sealed record Scenario(string Name, string Theme, string Type, ExtentBounds Extent, string CustomUrl = null);

        private static IEnumerable<Scenario> AllScenarios()
        {
            foreach (var (label, extent) in new[] { ("small", Small), ("large", Large) })
            {
                yield return new Scenario($"places-{label}", "places", "place", extent);
                yield return new Scenario($"buildings-{label}", "buildings", "building", extent);
                yield return new Scenario($"segments-{label}", "transportation", "segment", extent);
                yield return new Scenario($"osm-buildings-{label}", PresetCatalog.OsmThemeKey, "OSM Buildings", extent,
                    PresetCatalog.OpenStreetMap.First(p => p.Name == "OSM Buildings").Url);
            }
        }

        [Fact]
        public async Task PipelineBenchmark_ReportsStageTimings()
        {
            if (Environment.GetEnvironmentVariable("GEOPARQUET_BENCH") != "1")
            {
                _output.WriteLine("Skipped: set GEOPARQUET_BENCH=1 to run the network benchmark.");
                return;
            }

            string release = Environment.GetEnvironmentVariable("GEOPARQUET_BENCH_RELEASE") ?? DefaultRelease;
            var wanted = (Environment.GetEnvironmentVariable("GEOPARQUET_BENCH_SCENARIOS") ?? "")
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToHashSet();
            var scenarios = AllScenarios().Where(s => wanted.Count == 0 || wanted.Contains(s.Name)).ToList();

            var report = new StringBuilder();
            report.AppendLine($"Release {release}, {Environment.ProcessorCount} CPUs, {RuntimeLabel()}, provenance={Environment.GetEnvironmentVariable("GEOPARQUET_BENCH_PROVENANCE") == "1"}");
            report.AppendLine();
            report.AppendLine("| Scenario | Files | Rows | Setup | Catalog | Schema | Load | Count | Archive | Display | Discover | Export | Total |");
            report.AppendLine("|---|---|---|---|---|---|---|---|---|---|---|---|---|");

            foreach (var scenario in scenarios)
            {
                string row;
                try { row = await RunScenarioAsync(scenario, release); }
                catch (Exception ex) when (DuckDbNetworkErrors.IsNetworkOrProxyFailure(ex.Message) || ex is System.Net.Http.HttpRequestException || ex.Message.Contains("HTTP", StringComparison.Ordinal))
                {
                    row = $"| {scenario.Name} | unreachable from this machine: {ex.Message.Split('\n')[0]} |";
                }
                report.AppendLine(row);
                _output.WriteLine(row);
            }

            string text = report.ToString();
            _output.WriteLine(text);
            string outPath = Environment.GetEnvironmentVariable("GEOPARQUET_BENCH_OUT");
            if (!string.IsNullOrWhiteSpace(outPath))
                File.WriteAllText(outPath, text);
        }

        private string RuntimeLabel()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT version()";
            return $"DuckDB {command.ExecuteScalar()}";
        }

        private async Task<string> RunScenarioAsync(Scenario scenario, string release)
        {
            // Fresh connection per scenario, configured like DuckDBManager does.
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();
            void Sql(string sql) { using var c = connection.CreateCommand(); c.CommandText = sql; c.ExecuteNonQuery(); }
            object Scalar(string sql) { using var c = connection.CreateCommand(); c.CommandText = sql; return c.ExecuteScalar(); }

            var setupWatch = Stopwatch.StartNew();
            Sql("LOAD spatial; LOAD httpfs;");
            Sql(HttpProxySettingsBuilder.BuildSessionSettingsSql(new AddinSettings(), null,
                Math.Max(1, Environment.ProcessorCount), Path.Combine(_folder, "tmp")));
            // Overture and OSM are public buckets: never sign with ambient AWS credentials.
            Sql("SET s3_access_key_id=''; SET s3_secret_access_key=''; SET s3_session_token='';");
            string extraSql = Environment.GetEnvironmentVariable("GEOPARQUET_BENCH_EXTRA_SQL");
            if (!string.IsNullOrWhiteSpace(extraSql)) Sql(extraSql);

            var times = new Dictionary<string, TimeSpan> { ["setup"] = setupWatch.Elapsed };
            var total = Stopwatch.StartNew();
            T Time<T>(string stage, Func<T> action)
            {
                var sw = Stopwatch.StartNew();
                try { return action(); }
                finally { times[stage] = times.GetValueOrDefault(stage) + sw.Elapsed; }
            }

            // The wizard's "display provenance" option adds derived source_* columns.
            bool provenance = Environment.GetEnvironmentVariable("GEOPARQUET_BENCH_PROVENANCE") == "1";
            bool isOverture = scenario.CustomUrl == null;
            string sourceUri = isOverture
                ? $"{OvertureS3}/{release}/theme={scenario.Theme}/type={scenario.Type}/*.parquet"
                : scenario.CustomUrl;

            IReadOnlyList<string> files = null;
            if (isOverture)
            {
                var catalogWatch = Stopwatch.StartNew();
                files = await FindOvertureFilesAsync(connection, release, scenario).ConfigureAwait(false);
                times["catalog"] = catalogWatch.Elapsed;
            }

            // Same read-option choice and schema probe as DataProcessor.IngestFileCoreAsync.
            string readOptions = sourceUri.Contains("s3://overturemaps", StringComparison.OrdinalIgnoreCase) &&
                                 sourceUri.Contains("/release/", StringComparison.OrdinalIgnoreCase)
                ? S3Ingester.S3FastReadParquetOptions
                : S3Ingester.S3ReadParquetOptions;
            string readExpression = S3Ingester.BuildReadParquetExpression(sourceUri, readOptions, files);
            var columns = Time("schema", () =>
            {
                Sql($"CREATE OR REPLACE TABLE temp AS SELECT * FROM {readExpression} LIMIT 0;");
                var names = new List<string>();
                using var c = connection.CreateCommand();
                c.CommandText = "DESCRIBE temp";
                using var reader = c.ExecuteReader();
                while (reader.Read()) names.Add(reader.GetString(0));
                return names;
            });

            Time("load", () =>
            {
                Sql(S3Ingester.BuildLoadQuery(sourceUri, scenario.Type, columns, scenario.Extent,
                    readParquetOptions: readOptions, preserveSource: true, files: files));
                return 0;
            });
            long rows = Time("count", () => Convert.ToInt64(Scalar("SELECT COUNT(*) FROM current_table")));

            if (rows > 0)
            {
                string staging = Path.Combine(_folder, scenario.Name);
                string archive = Path.Combine(staging, "_source", scenario.Type + ".geoparquet");
                Directory.CreateDirectory(Path.GetDirectoryName(archive));
                Time("archive", () =>
                {
                    Sql(GeoParquetSql.BuildCopyCommand(
                        $"SELECT * EXCLUDE({GeoParquetSql.InternalGeometryTypeColumn}) FROM current_table", archive, "ZSTD"));
                    return 0;
                });
                Time("display", () =>
                {
                    Sql(S3Ingester.BuildDisplayTableCommand(scenario.Type,
                        columns.Append(GeoParquetSql.InternalGeometryTypeColumn).ToList(), provenance));
                    return 0;
                });
                var geometryTypes = Time("discover", () =>
                {
                    var types = new List<string>();
                    using var c = connection.CreateCommand();
                    c.CommandText = $"SELECT DISTINCT {GeoParquetSql.InternalGeometryTypeColumn} FROM current_table WHERE geometry IS NOT NULL AND {GeoParquetSql.InternalGeometryTypeColumn} IS NOT NULL";
                    using var reader = c.ExecuteReader();
                    while (reader.Read()) types.Add(reader.GetString(0));
                    return types;
                });
                bool hasBbox = Convert.ToInt32(Scalar("SELECT COUNT(*) FROM pragma_table_info('current_table') WHERE lower(name)='bbox'")) > 0;
                Time("export", () =>
                {
                    foreach (var geometryType in geometryTypes)
                    {
                        string output = Path.Combine(staging, $"{scenario.Type}_{geometryType}.parquet");
                        Sql(GeoParquetSql.BuildCopyCommand(
                            GeoParquetSql.BuildGeometryExportSelect(geometryType, hasBbox, true), output, "ZSTD"));
                    }
                    return 0;
                });
            }
            total.Stop();

            string F(string stage) => times.TryGetValue(stage, out var t) ? $"{t.TotalSeconds:F2}s" : "-";
            return $"| {scenario.Name} | {(files == null ? "all" : files.Count.ToString(CultureInfo.InvariantCulture))} | {rows:N0} | {F("setup")} | {F("catalog")} | {F("schema")} | {F("load")} | {F("count")} | {F("archive")} | {F("display")} | {F("discover")} | {F("export")} | {(total.Elapsed + times["setup"]).TotalSeconds:F2}s |";
        }

        /// <summary>
        /// Uses the real STAC catalog (OvertureCatalog) when reachable. Where it
        /// is not (some sandboxes block stac.overturemaps.org) it builds the same
        /// per-file bounds from Parquet bbox statistics once, caches them, and
        /// applies the same overlap test, so the load stage sees the same files.
        /// </summary>
        private async Task<IReadOnlyList<string>> FindOvertureFilesAsync(DuckDBConnection connection, string release, Scenario scenario)
        {
            string cacheDir = Path.Combine(Path.GetTempPath(), "geoparquet-bench-catalog");
            try
            {
                return await new OvertureCatalog().FindFilesAsync(release, scenario.Theme, scenario.Type, scenario.Extent, cacheDir);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _output.WriteLine($"[{scenario.Name}] STAC catalog unavailable ({ex.GetType().Name}); using Parquet footer statistics.");
            }

            string cache = Path.Combine(cacheDir, $"footer_{release}_{scenario.Theme}_{scenario.Type}.json");
            Dictionary<string, double[]> bounds;
            if (File.Exists(cache))
                bounds = JsonSerializer.Deserialize<Dictionary<string, double[]>>(File.ReadAllText(cache));
            else
            {
                bounds = new Dictionary<string, double[]>();
                using var c = connection.CreateCommand();
                c.CommandText = $@"
                    SELECT file_name,
                           min(TRY_CAST(stats_min_value AS DOUBLE)) FILTER (WHERE path_in_schema = 'bbox, xmin'),
                           min(TRY_CAST(stats_min_value AS DOUBLE)) FILTER (WHERE path_in_schema = 'bbox, ymin'),
                           max(TRY_CAST(stats_max_value AS DOUBLE)) FILTER (WHERE path_in_schema = 'bbox, xmax'),
                           max(TRY_CAST(stats_max_value AS DOUBLE)) FILTER (WHERE path_in_schema = 'bbox, ymax')
                    FROM parquet_metadata('{OvertureS3}/{release}/theme={scenario.Theme}/type={scenario.Type}/*.parquet')
                    GROUP BY file_name";
                using var reader = c.ExecuteReader();
                while (reader.Read())
                    bounds[reader.GetString(0)] = Enumerable.Range(1, 4)
                        .Select(i => reader.IsDBNull(i) ? double.NaN : reader.GetDouble(i)).ToArray();
                Directory.CreateDirectory(cacheDir);
                File.WriteAllText(cache, JsonSerializer.Serialize(bounds));
            }

            var area = scenario.Extent;
            return bounds
                .Where(kv => kv.Value.Any(double.IsNaN) ||
                             (kv.Value[0] <= area.XMax && kv.Value[2] >= area.XMin && kv.Value[1] <= area.YMax && kv.Value[3] >= area.YMin))
                .Select(kv => kv.Key.Replace(OvertureS3, OvertureHttps, StringComparison.Ordinal))
                .OrderBy(f => f, StringComparer.Ordinal)
                .ToList();
        }
    }
}
