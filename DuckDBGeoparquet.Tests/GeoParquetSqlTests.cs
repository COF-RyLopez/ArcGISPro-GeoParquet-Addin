using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
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
        public void BuildCopyCommand_UsesForwardSlashesAndValidatedCompression()
        {
            string sql = GeoParquetSql.BuildCopyCommand(
                "SELECT 1", @"C:\data\out\file.parquet", "snappy");

            Assert.Contains("'C:/data/out/file.parquet'", sql);
            Assert.Contains("COMPRESSION 'SNAPPY'", sql);
            Assert.Contains($"ROW_GROUP_SIZE {GeoParquetSql.RowGroupSize}", sql);
            Assert.Contains("FORMAT 'PARQUET'", sql);
            Assert.Contains("SELECT 1", sql);
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
