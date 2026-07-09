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
            Assert.Contains("*", query);
            Assert.Contains($"AS {GeoParquetSql.InternalGeometryTypeColumn}", query);
            Assert.Contains("read_parquet('s3://bucket/*.parquet', filename=true, hive_partitioning=1, union_by_name=1)", query);
            // No extent → no spatial filter and no clipping.
            Assert.DoesNotContain("WHERE", query);
            Assert.DoesNotContain("ST_Intersection", query);
        }

        [Fact]
        public void BuildReadParquetExpression_DefaultsToUnionByNameAndSupportsFastPath()
        {
            string fallback = S3Ingester.BuildReadParquetExpression("s3://bucket/*.parquet");
            string fast = S3Ingester.BuildReadParquetExpression("s3://bucket/*.parquet", S3Ingester.S3FastReadParquetOptions);

            Assert.Contains("union_by_name=1", fallback);
            Assert.DoesNotContain("union_by_name=1", fast);
            Assert.Contains("filename=true, hive_partitioning=1", fast);
        }

        [Fact]
        public void BuildLoadQuery_NoExtent_UsesProjectionWhenExclusionsApply()
        {
            var columns = new List<string> { "id", "geometry", "bbox", "sources" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            Assert.Contains("id, geometry, bbox", query);
            Assert.DoesNotContain("sources", query);
        }

        [Fact]
        public void BuildLoadQuery_WithSourceProvenance_AddsFlattenedSourceFields()
        {
            var columns = new List<string> { "id", "geometry", "bbox", "sources" };
            string query = S3Ingester.BuildLoadQuery(
                "s3://bucket/building/*",
                "building",
                columns,
                extent: null,
                includeSourceProvenance: true);

            Assert.Contains("AS source_count", query);
            Assert.Contains("AS source_datasets", query);
            Assert.Contains("AS source_primary_dataset", query);
            Assert.Contains("AS source_primary_record_id", query);
            Assert.Contains("AS source_primary_update_time", query);
            Assert.Contains("AS source_url", query);
            Assert.Contains("AS source_edit_url", query);
            Assert.Contains("AS source_contribution_url", query);
            Assert.Contains("AS source_edit_platform", query);
            Assert.Contains("FROM unnest(sources) AS t(s)", query);
            Assert.Contains("https://www.openstreetmap.org/edit?way=", query);
            Assert.Contains("https://livingatlas.arcgis.com/community-maps/", query);
            Assert.DoesNotContain("SELECT id, geometry, bbox, sources", query);
        }

        [Fact]
        public void BuildLoadQuery_WithSourceProvenanceDisabled_DoesNotAddSourceFields()
        {
            var columns = new List<string> { "id", "geometry", "bbox", "sources" };
            string query = S3Ingester.BuildLoadQuery(
                "s3://bucket/building/*",
                "building",
                columns,
                extent: null,
                includeSourceProvenance: false);

            Assert.DoesNotContain("source_primary_dataset", query);
            Assert.DoesNotContain("source_edit_url", query);
            Assert.DoesNotContain("FROM unnest(sources) AS t(s)", query);
            Assert.DoesNotContain("SELECT id, geometry, bbox, sources", query);
        }

        [Fact]
        public void BuildLoadQuery_WithSourceProvenanceNoSourcesColumn_DoesNotAddSourceFields()
        {
            var columns = new List<string> { "id", "geometry", "bbox" };
            string query = S3Ingester.BuildLoadQuery(
                "s3://bucket/building/*",
                "building",
                columns,
                extent: null,
                includeSourceProvenance: true);

            Assert.DoesNotContain("source_primary_dataset", query);
            Assert.DoesNotContain("FROM unnest(sources) AS t(s)", query);
        }

        [Fact]
        public void BuildLoadQuery_ForPlaces_FlattensAddressFieldsWhenAddressesArePresent()
        {
            var columns = new List<string> { "id", "names", "addresses", "geometry", "bbox", "sources" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            Assert.Contains("AS names_primary", query);
            Assert.Contains("AS names_common", query);
            Assert.Contains("AS display_name", query);
            Assert.Contains("string_agg(CAST(a.freeform AS VARCHAR), ' | ')", query);
            Assert.Contains("AS address_freeform", query);
            Assert.Contains("AS address_locality", query);
            Assert.Contains("AS address_region", query);
            Assert.Contains("AS address_postcode", query);
            Assert.Contains("AS address_country", query);
            Assert.DoesNotContain("SELECT id, names, addresses", query);
        }

        [Fact]
        public void BuildLoadQuery_FlattensCartographyHintsToZoomAndScaleFields()
        {
            var columns = new List<string> { "id", "cartography", "geometry", "bbox" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/division/*", "division", columns, extent: null);

            Assert.Contains("AS cartography_prominence", query);
            Assert.Contains("AS cartography_min_zoom", query);
            Assert.Contains("AS cartography_max_zoom", query);
            Assert.Contains("AS cartography_sort_key", query);
            Assert.Contains("AS cartography_min_scale", query);
            Assert.Contains("AS cartography_max_scale", query);
            Assert.Contains("591657527.591555 / power(2", query);
        }

        [Fact]
        public void BuildLoadQuery_ForPlaces_FlattensTaxonomyCategoryStatusAndBrandFields()
        {
            var columns = new List<string>
            {
                "id", "names", "categories", "taxonomy", "basic_category",
                "operating_status", "brand", "confidence", "geometry", "bbox"
            };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            Assert.Contains("AS categories_primary", query);
            Assert.Contains("AS categories_alternate", query);
            Assert.Contains("AS taxonomy_summary", query);
            Assert.Contains("AS place_basic_category", query);
            Assert.Contains("AS place_operating_status", query);
            Assert.Contains("AS brand_wikidata", query);
            Assert.Contains("AS brand_names_primary", query);
        }

        [Fact]
        public void BuildLoadQuery_ForSegments_FlattensTransportationQaFields()
        {
            var columns = new List<string>
            {
                "id", "names", "connectors", "road_surface", "speed_limits",
                "access_restrictions", "routes", "geometry", "bbox"
            };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/segment/*", "segment", columns, extent: null);

            Assert.Contains("AS connector_count", query);
            Assert.Contains("AS road_surface_values", query);
            Assert.Contains("AS speed_limit_rule_count", query);
            Assert.Contains("AS access_restriction_count", query);
            Assert.Contains("AS route_count", query);
        }

        [Fact]
        public void GetFlattenedFieldNames_ReportsOnlyNewFlattenedFields()
        {
            var columns = new List<string> { "id", "names", "display_name", "cartography", "sources", "geometry" };
            var fields = S3Ingester.GetFlattenedFieldNames("division", columns, includeSourceProvenance: true);

            Assert.Contains("names_primary", fields);
            Assert.Contains("names_common", fields);
            Assert.DoesNotContain("display_name", fields);
            Assert.Contains("cartography_min_zoom", fields);
            Assert.Contains("cartography_min_scale", fields);
            Assert.Contains("source_primary_dataset", fields);
        }

        [Fact]
        public void BuildLoadQuery_ForPlaces_DoesNotDuplicateExistingFlattenedAddressFields()
        {
            var columns = new List<string> { "id", "names", "addresses", "address_freeform", "geometry", "bbox" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            Assert.DoesNotContain("AS address_freeform", query);
            Assert.Contains("AS address_locality", query);
        }

        [Fact]
        public void BuildLoadQuery_WithExtentAndBbox_ClipsAndRepacksBbox()
        {
            var extent = new ExtentBounds(1, 2, 3, 4);
            var columns = new List<string> { "id", "geometry", "bbox" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/building/*", "building", columns, extent);

            // bbox present → repack via struct_pack after clipping.
            Assert.Contains("WITH clipped AS", query);
            Assert.Contains("ST_Intersection(geometry,", query);
            Assert.Equal(1, CountOccurrences(query, "ST_Intersects(geometry,"));
            Assert.Contains("struct_pack", query);
            Assert.Contains("AS bbox", query);
            Assert.Contains($"AS {GeoParquetSql.InternalGeometryTypeColumn}", query);
            // Pushdown predicate + refinement filter are both present.
            Assert.Contains("bbox.xmin <= 3", query);
        }

        [Fact]
        public void BuildLoadQuery_WithExtentNoBbox_ClipsGeometryWithoutRepack()
        {
            var extent = new ExtentBounds(1, 2, 3, 4);
            var columns = new List<string> { "id", "geometry" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/data/*", actualS3Type: null, columns, extent);

            Assert.Contains("ST_Intersection(geometry,", query);
            Assert.Equal(1, CountOccurrences(query, "ST_Intersects(geometry,"));
            Assert.Contains("* EXCLUDE(geometry)", query);
            Assert.Contains($"AS {GeoParquetSql.InternalGeometryTypeColumn}", query);
            // No bbox column → no struct_pack repack.
            Assert.DoesNotContain("struct_pack", query);
        }

        [Theory]
        [InlineData("address")]
        [InlineData("place")]
        [InlineData("connector")]
        public void BuildLoadQuery_WithExtentForPointOnlyTypes_PreservesOriginalGeometry(string actualType)
        {
            var extent = new ExtentBounds(1, 2, 3, 4);
            var columns = new List<string> { "id", "geometry", "bbox", "sources" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/data/*", actualType, columns, extent);

            Assert.DoesNotContain("ST_Intersection", query);
            Assert.Contains("ST_Intersects(geometry,", query);
            Assert.Contains("ST_GeometryType(geometry)", query);
            Assert.Contains($"AS {GeoParquetSql.InternalGeometryTypeColumn}", query);
        }

        private static int CountOccurrences(string value, string pattern)
        {
            int count = 0;
            int index = 0;
            while ((index = value.IndexOf(pattern, index, System.StringComparison.Ordinal)) >= 0)
            {
                count++;
                index += pattern.Length;
            }
            return count;
        }
    }
}
