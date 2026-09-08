using DuckDB.NET.Data;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public sealed class DuckDBExecutionTests : IDisposable
    {
        private readonly DuckDBConnection _connection = new("DataSource=:memory:");
        private readonly string _folder = Path.Combine(Path.GetTempPath(), "overture-tests-" + Guid.NewGuid().ToString("N"));
        public DuckDBExecutionTests()
        {
            Directory.CreateDirectory(_folder);
            _connection.Open();
        }
        private void Sql(string sql) { using var command = _connection.CreateCommand(); command.CommandText = sql; command.ExecuteNonQuery(); }
        private object Scalar(string sql) { using var command = _connection.CreateCommand(); command.CommandText = sql; return command.ExecuteScalar(); }
        private void Spatial()
        {
            try { Sql("LOAD spatial"); }
            catch (DuckDBException) { Sql("INSTALL spatial; LOAD spatial"); }
        }

        [Fact]
        public void ExportRoundTrip_PreservesRoadSourceAndQuotedPath()
        {
            Spatial();
            string file = Path.Combine(_folder, "O'Brien.parquet");
            Sql(GeoParquetSql.BuildCopyCommand("SELECT 'road1' id, ST_GeomFromText('LINESTRING(-2 0, 2 0)') geometry, ['connector1','connector2'] connectors, ['source1'] sources", file));
            Sql(S3Ingester.BuildLoadQuery(file, "segment", new[] { "id", "geometry", "connectors", "sources" },
                new ExtentBounds(-1, -1, 1, 1), preserveSource: true));
            Assert.Equal("LINESTRING (-2 0, 2 0)", Scalar("SELECT ST_AsText(geometry) FROM current_table"));
            Assert.Equal("connector2", Scalar("SELECT connectors[2] FROM current_table"));
            Assert.Equal("source1", Scalar("SELECT sources[1] FROM current_table"));
            string archive = Path.Combine(_folder, "source.geoparquet");
            Sql(GeoParquetSql.BuildCopyCommand($"SELECT * EXCLUDE({GeoParquetSql.InternalGeometryTypeColumn}) FROM current_table", archive));
            Sql(S3Ingester.BuildDisplayTableCommand("segment", new[] { "id", "geometry", "connectors", "sources", GeoParquetSql.InternalGeometryTypeColumn }, false));
            Assert.Equal("LINESTRING (-2 0, 2 0)", Scalar("SELECT ST_AsText(geometry) FROM current_table"));
            Assert.Equal(0, Convert.ToInt32(Scalar("SELECT count(*) FROM pragma_table_info('current_table') WHERE name='connectors'")));
            Assert.Equal("connector2", Scalar($"SELECT connectors[2] FROM read_parquet('{GeoParquetSql.EscapeSqlLiteral(archive)}')"));

        }

        [Theory]
        [InlineData("12 Main St", 0.0001, 1)]
        [InlineData("12 1/2 Main St", 0, 0)]
        [InlineData("123 Main St", 0, 0)]
        [InlineData("12 Main St Apt 2", 0, 0)]
        public void AddressMatching_UsesRadiusAndFullHouseNumber(string address, double offset, long expected)
        {
            Spatial();
            Sql(GersSql.BuildUtilityFunctionsCommand());
            string file = Path.Combine(_folder, "addresses.parquet");
            Sql(GeoParquetSql.BuildCopyCommand("SELECT 'gers1' id, '12' number, 'Main St' street, ST_Point(-119,36) geometry, struct_pack(xmin := -119, xmax := -119, ymin := 36, ymax := 36) bbox", file));
            Sql($"CREATE TABLE gers_user_input AS SELECT '1' record_id, 'local1' source_record_key, '' AS name, '{address}' address, '' city, '' state, '' postcode, {-119 + offset:R}::DOUBLE longitude, 36::DOUBLE latitude");
            var options = new GersifyOptions { InputExtent = new ExtentBounds(-119 + offset, 36, -119 + offset, 36) };
            Sql(GersSql.BuildAddressesCandidateTablesCommand(file, options, new[] { "id", "number", "street", "geometry", "bbox" }));
            Assert.Equal(expected, Convert.ToInt64(Scalar("SELECT count(*) FROM gers_accepted")));
        }

        [Theory]
        [InlineData("12 Main St Apt 2", 0)]
        [InlineData("12 Main St Apt 3", 1)]
        public void AddressMatching_RequiresMatchingUnit(string address, long expected)
        {
            Spatial(); Sql(GersSql.BuildUtilityFunctionsCommand());
            string file = Path.Combine(_folder, "units.parquet");
            Sql(GeoParquetSql.BuildCopyCommand("SELECT 'gers1' id, '12' number, 'Main St' street, 'Apt 3' unit, ST_Point(-119,36) geometry", file));
            Sql($"CREATE TABLE gers_user_input AS SELECT '1' record_id, 'local1' source_record_key, '' AS name, '{address}' address, '' city, '' state, '' postcode, -119::DOUBLE longitude, 36::DOUBLE latitude");
            Sql(GersSql.BuildAddressesCandidateTablesCommand(file, new GersifyOptions(), new[] { "id", "number", "street", "unit", "geometry" }));
            Assert.Equal(expected, Convert.ToInt64(Scalar("SELECT count(*) FROM gers_accepted")));
        }

        [Theory]
        [InlineData(179.9999, 0, -179.9999, 0)]
        [InlineData(0, 89.9999, 180, 89.9999)]
        public void AddressMatching_HandlesDatelineAndPolarDistances(double x, double y, double candidateX, double candidateY)
        {
            Spatial(); Sql(GersSql.BuildUtilityFunctionsCommand());
            string file = Path.Combine(_folder, "edge.parquet");
            Sql(GeoParquetSql.BuildCopyCommand(FormattableString.Invariant($"SELECT 'gers1' id, '12' number, 'Main St' street, ST_Point({candidateX:R},{candidateY:R}) geometry, struct_pack(xmin := {candidateX:R}, xmax := {candidateX:R}, ymin := {candidateY:R}, ymax := {candidateY:R}) bbox"), file));
            Sql(FormattableString.Invariant($"CREATE TABLE gers_user_input AS SELECT '1' record_id, 'local1' source_record_key, '' AS name, '12 Main St' address, '' city, '' state, '' postcode, {x:R}::DOUBLE longitude, {y:R}::DOUBLE latitude"));
            Sql(GersSql.BuildAddressesCandidateTablesCommand(file, new GersifyOptions { InputExtent = new ExtentBounds(x,y,x,y) }, new[] { "id", "number", "street", "geometry", "bbox" }));
            Assert.Equal(1, Convert.ToInt64(Scalar("SELECT count(*) FROM gers_accepted")));
            Assert.InRange(Convert.ToDouble(Scalar("SELECT distance_m FROM gers_accepted")), 20, 25);
        }

        [Fact]
        public async Task LocalSearch_FindsFullAddressWithPostalCity()
        {
            Spatial();
            string file = Path.Combine(_folder, "city.parquet");
            Sql(GeoParquetSql.BuildCopyCommand("SELECT '12' number, 'Main St' street, 'Fresno' postal_city, 'CA' region, '93721' postcode, ST_Point(-119,36) geometry", file));
            using var manager = new DuckDBManager();
            manager.Connection.Open();
            using (var command = manager.Connection.CreateCommand()) { command.CommandText = "LOAD spatial"; command.ExecuteNonQuery(); }
            var candidates = await manager.RunAsync(() => new GeocoderEngine(manager).SearchAddressCandidatesAsync(file, "12 main st fresno ca 93721"));
            Assert.Single(candidates);
            Assert.NotNull(GeocodeAcceptance.Evaluate("12 Main St, Fresno CA 93721", candidates).Match);
        }

        [Fact]
        public void ExplicitCatalogFiles_ReadOnlySelectedAssets()
        {
            Spatial();
            string selected = Path.Combine(_folder, "selected.parquet");
            string excluded = Path.Combine(_folder, "excluded.parquet");
            Sql(GeoParquetSql.BuildCopyCommand("SELECT 'selected' id, ST_Point(0,0) geometry", selected));
            Sql(GeoParquetSql.BuildCopyCommand("SELECT 'excluded' id, ST_Point(0,0) geometry", excluded));
            Sql(S3Ingester.BuildLoadQuery(Path.Combine(_folder, "*.parquet"), "address", new[] { "id", "geometry" },
                new ExtentBounds(-1, -1, 1, 1), preserveSource: true, files: new[] { selected }));
            Assert.Equal(1, Convert.ToInt32(Scalar("SELECT count(*) FROM current_table")));
            Assert.Equal("selected", Scalar("SELECT id FROM current_table"));
        }

        [Fact]
        public async Task Worker_InterruptsNativeQueryAndAllowsNextJob()
        {
            using var queue = new DuckDBJobQueue();
            using var cancellation = new CancellationTokenSource();
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT sum(sin(i)) FROM range(10000000000) t(i)";
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var running = queue.RunAsync(async () =>
            {
                started.SetResult(true);
                return await command.ExecuteScalarAsync();
            }, command.Cancel, cancellation.Token);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            // Let the native query start; test has a bounded wall-clock failure signal.
            await Task.Delay(100);
            Assert.False(running.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(TimeSpan.FromSeconds(5)));
            var value = await queue.RunAsync(() => Task.FromResult(Scalar("SELECT 42")), command.Cancel);
            Assert.Equal(42, Convert.ToInt32(value));
        }

        [Fact]
        public async Task Worker_QueuedCancellationCannotInterruptActiveJob()
        {
            using var queue = new DuckDBJobQueue();
            var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int interrupted = 0;
            var first = queue.RunAsync(async () => { entered.SetResult(true); return await release.Task; }, () => Interlocked.Increment(ref interrupted));
            await entered.Task;
            using var cancelled = new CancellationTokenSource();
            var second = queue.RunAsync(() => Task.FromResult(true), () => Interlocked.Increment(ref interrupted), cancelled.Token);
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
            Assert.Equal(0, interrupted);
            release.SetResult(true);
            Assert.True(await first);
        }

        public void Dispose() { _connection.Dispose(); Directory.Delete(_folder, true); }
    }
}
