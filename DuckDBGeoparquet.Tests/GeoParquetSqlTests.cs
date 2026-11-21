using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Globalization;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GeoParquetSqlTests
    {
        [Theory]
        [InlineData("SNAPPY", "SNAPPY")]
        [InlineData("snappy", "SNAPPY")]
        [InlineData("GZIP", "GZIP")]
        [InlineData("gzip", "GZIP")]
        [InlineData("ZSTD", "ZSTD")]
        [InlineData("LZ4", "ZSTD")]
        [InlineData("", "ZSTD")]
        [InlineData(null, "ZSTD")]
        public void ValidateCompression_NormalizesOrDefaultsToZstd(string input, string expected)
        {
            Assert.Equal(expected, GeoParquetSql.ValidateCompression(input));
        }

        [Fact]
        public void CopyCommand_EscapesApostrophesInProjectPath()
        {
            Assert.Contains("O''Brien", GeoParquetSql.BuildCopyCommand("SELECT 1", "C:/O'Brien/data.parquet"));
        }

        [Fact]
        public void BuildCopyCommand_UsesForwardSlashesAndValidatedCompression()
        {
            string sql = GeoParquetSql.BuildCopyCommand(
                "SELECT 1", @"C:\data\out\file.parquet", "snappy");

            Assert.Contains("'C:/data/out/file.parquet'", sql);
            Assert.Contains("COMPRESSION 'SNAPPY'", sql);
            Assert.Contains($"ROW_GROUP_SIZE {GeoParquetSql.RowGroupSize}", sql);
            Assert.Contains("FORMAT 'PARQUET'", sql);
            Assert.Contains("SELECT 1", sql);
            Assert.Contains($"{GeoParquetSql.GeoParquetVersionCopyOption} 'V1'", sql);
        }

        [Theory]
        [InlineData(null, "V1")]
        [InlineData("", "V1")]
        [InlineData("V1", "V1")]
        [InlineData("1.x (default)", "V1")]
        [InlineData("v2", "V2")]
        [InlineData("2.0", "V2")]
        [InlineData("2.0.0", "V2")]
        [InlineData("2", "V2")]
        public void NormalizeGeoParquetVersion_MapsUiAndCopyValues(string input, string expected)
        {
            Assert.Equal(expected, GeoParquetSql.NormalizeGeoParquetVersion(input));
        }

        [Fact]
        public void BuildCopyCommand_PinsV1For1xDefault()
        {
            string sql = GeoParquetSql.BuildCopyCommand("SELECT 1", "/tmp/out.parquet", "ZSTD", "V1");

            Assert.Contains($"{GeoParquetSql.GeoParquetVersionCopyOption} 'V1'", sql);
            Assert.DoesNotContain("'V2'", sql);
        }

        [Fact]
        public void BuildCopyCommand_AddsGeoParquetVersionV2()
        {
            string sql = GeoParquetSql.BuildCopyCommand("SELECT 1", "/tmp/out.parquet", "ZSTD", "2.0");

            Assert.Contains($"{GeoParquetSql.GeoParquetVersionCopyOption} '{GeoParquetSql.GeoParquetVersion2}'", sql);
        }

        [Theory]
        [InlineData("v1.5.3", 1, 5, 3)]
        [InlineData("1.4.1", 1, 4, 1)]
        [InlineData("v1.5.3-dev123", 1, 5, 3)]
        public void TryParseDuckDbVersion_ReadsCommonVersionStrings(string input, int major, int minor, int build)
        {
            Assert.True(GeoParquetSql.TryParseDuckDbVersion(input, out var version));
            Assert.Equal(new Version(major, minor, build), version);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("not-a-version")]
        public void TryParseDuckDbVersion_RejectsInvalidStrings(string input)
        {
            Assert.False(GeoParquetSql.TryParseDuckDbVersion(input, out var version));
            Assert.Null(version);
        }

        [Theory]
        [InlineData("1.4.1", true)]
        [InlineData("v1.5.3", true)]
        [InlineData("1.4.0", false)]
        [InlineData("1.3.2", false)]
        [InlineData(null, false)]
        public void SupportsGeoParquetV2_GatesOnDuckDb141(string versionText, bool expected)
        {
            Assert.Equal(expected, GeoParquetSql.SupportsGeoParquetV2(versionText));
        }

        [Fact]
        public void EnsureGeoParquetVersionWritable_AllowsV2OnDuckDb153()
        {
            var version = new Version(1, 5, 3);
            GeoParquetSql.EnsureGeoParquetVersionWritable("V2", version, "v1.5.3");
        }

        [Fact]
        public void EnsureGeoParquetVersionWritable_AllowsV1OnOldDuckDb()
        {
            GeoParquetSql.EnsureGeoParquetVersionWritable("V1", new Version(1, 3, 2), "v1.3.2");
        }

        [Fact]
        public void EnsureGeoParquetVersionWritable_RejectsV2OnOldOrUnknownDuckDb()
        {
            var old = Assert.Throws<InvalidOperationException>(
                () => GeoParquetSql.EnsureGeoParquetVersionWritable("2.0", new Version(1, 3, 2), "v1.3.2"));
            Assert.Contains("1.4.1", old.Message);
            Assert.Contains("v1.3.2", old.Message);

            var unknown = Assert.Throws<InvalidOperationException>(
                () => GeoParquetSql.EnsureGeoParquetVersionWritable("V2", null));
            Assert.Contains("unknown", unknown.Message);
            Assert.Contains(GeoParquetSql.GeoParquetVersionCopyOption, unknown.Message);
        }

        [Fact]
        public void CoordinateFormatting_IsCultureInvariant()
        {
            // A German-locale machine writing "36,76" instead of "36.76"
            // into SQL was a real production bug — pin the invariant.
            var original = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");
                var extent = new ExtentBounds(-119.80264120716375, 36.76077899574986,
                                              -119.7224864965243, 36.84735915194328);

                string predicate = GeoParquetSql.BuildBboxOverlapPredicate(extent);
                string polygon = GeoParquetSql.BuildExtentPolygon(extent);

                Assert.DoesNotContain(",76", predicate);
                Assert.Contains("-119.80264120716375", predicate);
                Assert.Contains("36.84735915194328", polygon);
            }
            finally
            {
                CultureInfo.CurrentCulture = original;
            }
        }

        [Fact]
        public void BuildBboxOverlapPredicate_ComparesOpposingEdges()
        {
            var extent = new ExtentBounds(1, 2, 3, 4);
            string predicate = GeoParquetSql.BuildBboxOverlapPredicate(extent);

            // Overlap test: dataset bbox min edges against extent max edges
            // and vice versa.
            Assert.Contains("bbox.xmin <= 3", predicate);
            Assert.Contains("bbox.xmax >= 1", predicate);
            Assert.Contains("bbox.ymin <= 4", predicate);
            Assert.Contains("bbox.ymax >= 2", predicate);
        }

        [Theory]
        [InlineData(null, "")]
        [InlineData("", "")]
        [InlineData("plain", "plain")]
        [InlineData("O'Brien's", "O''Brien''s")]
        public void EscapeSqlLiteral_DoublesSingleQuotes(string input, string expected)
        {
            Assert.Equal(expected, GeoParquetSql.EscapeSqlLiteral(input));
        }

        [Fact]
        public void BuildExtentPolygon_ClosesTheRing()
        {
            var extent = new ExtentBounds(10, 20, 30, 40);
            string polygon = GeoParquetSql.BuildExtentPolygon(extent);

            Assert.StartsWith("ST_GeomFromText('POLYGON((", polygon);
            // First and last vertex must match for a valid ring.
            Assert.Contains("10 20, 30 20, 30 40, 10 40, 10 20", polygon);
        }

        [Fact]
        public void BuildGeometryExportSelect_UsesCachedGeometryTypeAndExcludesInternalColumn()
        {
            string sql = GeoParquetSql.BuildGeometryExportSelect("MULTIPOLYGON", currentTableHasBbox: true, hasCachedGeometryType: true);

            Assert.Contains($"* EXCLUDE(geometry, bbox, {GeoParquetSql.InternalGeometryTypeColumn})", sql);
            Assert.Contains($"{GeoParquetSql.InternalGeometryTypeColumn} = 'MULTIPOLYGON'", sql);
            Assert.DoesNotContain("ST_GeometryType(geometry) = 'MULTIPOLYGON'", sql);
            Assert.Contains("struct_pack", sql);
        }

        [Fact]
        public void BuildGeometryExportSelect_FallsBackToGeometryFunctionWhenNoCachedColumn()
        {
            string sql = GeoParquetSql.BuildGeometryExportSelect("POINT", currentTableHasBbox: false, hasCachedGeometryType: false);

            Assert.Contains("* EXCLUDE(geometry)", sql);
            Assert.Contains("ST_GeometryType(geometry) = 'POINT'", sql);
            Assert.DoesNotContain(GeoParquetSql.InternalGeometryTypeColumn, sql);
            Assert.DoesNotContain("struct_pack", sql);
        }
    }
}
