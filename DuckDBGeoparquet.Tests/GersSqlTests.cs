using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GersSqlTests
    {
        [Fact]
        public void BuildPlacesCandidateTablesCommand_UsesSchemaAwarePlaceExpressions()
        {
            var options = new GersifyOptions
            {
                InputExtent = new ExtentBounds(-120, 36, -119, 37),
                MaxDistanceMeters = 100,
                NameSimilarityThreshold = 0.8,
                AddressSimilarityThreshold = 0.7,
                AcceptScoreThreshold = 70
            };

            string sql = GersSql.BuildPlacesCandidateTablesCommand(
                "/data/release/place/*.parquet",
                options,
                ["id", "names", "geometry", "bbox"]);

            Assert.Contains("CAST(names.primary AS VARCHAR)", sql);
            Assert.Contains("coalesce('', '') AS overture_address", sql);
            Assert.Contains("bbox.xmin <= -119", sql);
            Assert.DoesNotContain("addresses[1].freeform", sql);
            Assert.Contains("jaro_winkler_similarity", sql);
        }

        [Fact]
        public void BuildPlacesCandidateTablesCommand_SkipsBboxFilterWhenBboxColumnMissing()
        {
            var options = new GersifyOptions
            {
                InputExtent = new ExtentBounds(-120, 36, -119, 37)
            };

            string sql = GersSql.BuildPlacesCandidateTablesCommand(
                "/data/release/place/*.parquet",
                options,
                ["id", "name", "geometry"]);

            Assert.Contains("CAST(name AS VARCHAR)", sql);
            Assert.DoesNotContain("bbox.xmin", sql);
        }

        [Fact]
        public void BuildTraceSourcesCommand_BuildsBridgeGlobAndOsmUrl()
        {
            var options = new TraceSourcesOptions
            {
                BridgeRoot = "s3://overturemaps-us-west-2/bridgefiles",
                Release = "2026-06-17.0",
                Theme = "places",
                Type = "place",
                InputCsvPath = "/tmp/input.csv",
                OutputFolder = "/tmp"
            };

            string glob = GersSql.BuildBridgeGlob(options.BridgeRoot, options.Release);
            string sql = GersSql.BuildTraceSourcesCommand(options, "/tmp/output.csv");

            Assert.Equal("s3://overturemaps-us-west-2/bridgefiles/2026-06-17.0/**/*.parquet", glob);
            Assert.Contains("theme = 'places'", sql);
            Assert.Contains("type = 'place'", sql);
            Assert.Contains("BuildOsmUrl", sql);
            Assert.Contains("/tmp/output.csv", sql);
        }
    }
}
