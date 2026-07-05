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
            Assert.DoesNotContain("unnest(addresses)", sql);
            Assert.Contains("jaro_winkler_similarity", sql);
            Assert.Contains("o.overture_name_norm,\n                        o.overture_street_norm,", sql);
        }

        [Fact]
        public void BuildPlacesCandidateTablesCommand_AllowsAddressOnlyCandidateLane()
        {
            var options = new GersifyOptions
            {
                MaxDistanceMeters = 75,
                NameSimilarityThreshold = 0.86,
                AddressSimilarityThreshold = 0.72,
                AcceptScoreThreshold = 72
            };

            string sql = GersSql.BuildPlacesCandidateTablesCommand(
                "/data/release/place/*.parquet",
                options,
                ["id", "name", "address_freeform", "geometry"]);

            Assert.Contains("CAST(address_freeform AS VARCHAR)", sql);
            Assert.DoesNotContain("unnest(addresses)", sql);
            Assert.Contains("OR (u.user_street_norm <> '' AND o.overture_street_norm <> '')", sql);
            Assert.Contains("OR (u.user_name_norm = '' AND u.user_street_norm <> '' AND o.overture_name_norm <> '')", sql);
            Assert.Contains("WHEN user_name_norm = '' OR overture_name_norm = '' THEN NULL", sql);
            Assert.Contains("jaro_winkler_similarity(user_street_norm, overture_street_norm)", sql);
            Assert.Contains("WHEN name_similarity IS NULL THEN (address_similarity * 80.0) + (distance_similarity * 20.0)", sql);
            Assert.Contains("AND (name_similarity IS NOT NULL OR address_similarity IS NOT NULL)", sql);
            Assert.Contains("AND distance_m <= 15", sql);
            Assert.Contains("ELSE 'nearby_only'", sql);
            Assert.Contains("a.match_strategy AS gers_match_strategy", sql);
        }

        [Fact]
        public void BuildPlacesCandidateTablesCommand_UsesOvertureAddressContextWhenAvailable()
        {
            var options = new GersifyOptions();

            string sql = GersSql.BuildPlacesCandidateTablesCommand(
                "/data/release/place/*.parquet",
                options,
                ["id", "names", "addresses", "geometry"]);

            Assert.Contains("string_agg(CAST(a.freeform AS VARCHAR), ' | ')", sql);
            Assert.Contains("string_agg(DISTINCT CAST(a.locality AS VARCHAR), ' | ')", sql);
            Assert.Contains("string_agg(DISTINCT CAST(a.region AS VARCHAR), ' | ')", sql);
            Assert.Contains("string_agg(DISTINCT left(CAST(a.postcode AS VARCHAR), 5), ' | ')", sql);
        }

        [Fact]
        public void BuildPlacesCandidateTablesCommand_UsesFlattenedOvertureAddressContextWhenAvailable()
        {
            var options = new GersifyOptions();

            string sql = GersSql.BuildPlacesCandidateTablesCommand(
                "/data/release/place/*.parquet",
                options,
                ["id", "names", "address_freeform", "address_locality", "address_region", "address_postcode", "geometry"]);

            Assert.Contains("CAST(address_freeform AS VARCHAR)", sql);
            Assert.Contains("CAST(address_locality AS VARCHAR)", sql);
            Assert.Contains("CAST(address_region AS VARCHAR)", sql);
            Assert.Contains("left(CAST(address_postcode AS VARCHAR), 5)", sql);
            Assert.DoesNotContain("unnest(addresses)", sql);
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
