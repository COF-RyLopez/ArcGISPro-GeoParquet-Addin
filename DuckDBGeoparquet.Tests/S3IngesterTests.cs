using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System.Collections.Generic;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class S3IngesterTests
    {
        [Fact]
        public void BuildColumnProjection_RetainsAllColumns_WhenTypeHasExclusionsButNonePresent()
        {
            // "building" only drops "sources"; a file without it keeps every
            // column, yielding an explicit full projection (not null).
            var columns = new List<string> { "id", "geometry", "bbox" };
            Assert.Equal("id, geometry, bbox", S3Ingester.BuildColumnProjection("building", columns));
        }

        [Fact]
        public void BuildColumnProjection_ReturnsNull_ForUnknownType()
        {
            var columns = new List<string> { "id", "geometry" };
            Assert.Null(S3Ingester.BuildColumnProjection("not_a_real_theme", columns));
        }

        [Fact]
        public void BuildColumnProjection_ReturnsNull_ForNullOrEmptyInputs()
        {
            Assert.Null(S3Ingester.BuildColumnProjection(null, new List<string> { "id" }));
            Assert.Null(S3Ingester.BuildColumnProjection("place", null));
            Assert.Null(S3Ingester.BuildColumnProjection("place", new List<string>()));
        }

        [Fact]
        public void BuildColumnProjection_DropsExcludedColumns()
        {
            // "place" excludes addresses, brand, emails, phones, socials, sources, websites.
            var columns = new List<string> { "id", "geometry", "bbox", "names", "sources", "websites", "brand" };
            string projection = S3Ingester.BuildColumnProjection("place", columns);

            Assert.NotNull(projection);
            Assert.Contains("id", projection);
            Assert.Contains("names", projection);
            Assert.DoesNotContain("sources", projection);
            Assert.DoesNotContain("websites", projection);
            Assert.DoesNotContain("brand", projection);
        }

        [Fact]
        public void BuildColumnProjection_NeverDropsGeometryOrBboxSubColumns()
        {
            // Even if a geometry/bbox_* column somehow collided with an exclusion
            // name, the projection must always retain it.
            var columns = new List<string> { "geometry", "bbox_xmin", "bbox_ymax", "sources" };
            string projection = S3Ingester.BuildColumnProjection("place", columns);

            Assert.NotNull(projection);
            Assert.Contains("geometry", projection);
            Assert.Contains("bbox_xmin", projection);
            Assert.Contains("bbox_ymax", projection);
            Assert.DoesNotContain("sources", projection);
        }

        [Fact]
        public void BuildLoadQuery_NoExtent_SelectsStarWhenNoProjection()
        {
            // A null/unknown type yields no projection → SELECT *.
            var columns = new List<string> { "id", "geometry", "bbox" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/*.parquet", actualS3Type: null, columns, extent: null);

            Assert.Contains("CREATE OR REPLACE TABLE current_table", query);
            Assert.Contains("SELECT *", query);
            Assert.Contains("read_parquet('s3://bucket/*.parquet', filename=true, hive_partitioning=1)", query);
            // No extent → no spatial filter and no clipping.
            Assert.DoesNotContain("WHERE", query);
            Assert.DoesNotContain("ST_Intersection", query);
        }

        [Fact]
        public void BuildLoadQuery_NoExtent_UsesProjectionWhenExclusionsApply()
        {
            var columns = new List<string> { "id", "geometry", "bbox", "sources" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            Assert.Contains("SELECT id, geometry, bbox", query);
            Assert.DoesNotContain("sources", query);
        }

        [Fact]
        public void BuildLoadQuery_ForPlaces_FlattensAddressFreeformWhenAddressesArePresent()
        {
            var columns = new List<string> { "id", "names", "addresses", "geometry", "bbox", "sources" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            Assert.Contains("CAST(addresses[1].freeform AS VARCHAR) AS address_freeform", query);
            Assert.DoesNotContain("SELECT id, names, addresses", query);
        }

        [Fact]
        public void BuildLoadQuery_WithExtentAndBbox_ClipsAndRepacksBbox()
        {
            var extent = new ExtentBounds(1, 2, 3, 4);
            var columns = new List<string> { "id", "geometry", "bbox" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent);

            // bbox present → repack via struct_pack after clipping.
            Assert.Contains("WITH clipped AS", query);
            Assert.Contains("ST_Intersection(geometry,", query);
            Assert.Contains("struct_pack", query);
            Assert.Contains("AS bbox", query);
            // Pushdown predicate + refinement filter are both present.
            Assert.Contains("bbox.xmin <= 3", query);
            Assert.Contains("ST_Intersects(geometry,", query);
        }

        [Fact]
        public void BuildLoadQuery_WithExtentNoBbox_ClipsGeometryWithoutRepack()
        {
            var extent = new ExtentBounds(1, 2, 3, 4);
            var columns = new List<string> { "id", "geometry" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/data/*", actualS3Type: null, columns, extent);

            Assert.Contains("ST_Intersection(geometry,", query);
            Assert.Contains("* EXCLUDE(geometry)", query);
            // No bbox column → no struct_pack repack.
            Assert.DoesNotContain("struct_pack", query);
        }
    }
}
