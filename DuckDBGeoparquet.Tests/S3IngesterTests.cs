using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System.Collections.Generic;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class S3IngesterTests
    {
        [Fact]
        public void DefaultExtract_PreservesWholeRoadGeometry()
        {
            var sql = S3Ingester.BuildLoadQuery("roads.parquet", "segment",
                new[] { "id", "geometry", "bbox", "connectors", "sources" }, new ExtentBounds(1, 2, 3, 4));
            Assert.DoesNotContain("ST_Intersection(", sql);
        }

        [Fact]
        public void BuildLoadQuery_WithExtentAndAttributeFilter_AppendsFilterToWhere()
        {
            var sql = S3Ingester.BuildLoadQuery("roads.parquet", "segment",
                new[] { "id", "geometry", "bbox" }, new ExtentBounds(1, 2, 3, 4),
                attributeFilter: "subtype = 'road'");
            Assert.Contains("ST_Intersects(geometry,", sql);
            Assert.Contains("AND (subtype = 'road')", sql);
        }

        [Fact]
        public void BuildLoadQuery_NoExtent_AttributeFilterBecomesWhere()
        {
            var sql = S3Ingester.BuildLoadQuery("roads.parquet", "segment",
                new[] { "id", "geometry" }, extent: null,
                attributeFilter: "WHERE subtype = 'road'");
            Assert.Contains("WHERE (subtype = 'road')", sql);
            Assert.DoesNotContain("ST_Intersects", sql);
        }

        [Fact]
        public void BuildLoadQuery_ClippedPath_CarriesAttributeFilter()
        {
            var sql = S3Ingester.BuildLoadQuery("roads.parquet", "water",
                new[] { "id", "geometry", "bbox" }, new ExtentBounds(1, 2, 3, 4),
                clipGeometry: true, attributeFilter: "subtype = 'ocean'");
            Assert.Contains("ST_Intersection(", sql);
            Assert.Contains("AND (subtype = 'ocean')", sql);
        }

        [Fact]
        public void BuildLoadQuery_WithAoiPolygon_IntersectsPolygonButPushesDownEnvelopeBbox()
        {
            string wkt = "POLYGON((1 2, 3 2, 2 4, 1 2))";
            var sql = S3Ingester.BuildLoadQuery("roads.parquet", "segment",
                new[] { "id", "geometry", "bbox" }, new ExtentBounds(1, 2, 3, 4),
                aoiPolygonWkt: wkt);

            // Exact filter uses the drawn polygon, not the envelope rectangle.
            Assert.Contains($"ST_Intersects(geometry, ST_GeomFromText('{wkt}'))", sql);
            Assert.DoesNotContain("POLYGON((1 2, 3 2, 3 4, 1 4, 1 2))", sql);
            // The fast pushdown still uses the envelope's bbox overlap.
            Assert.Contains("bbox.xmin <= 3", sql);
            // No clip requested → whole geometries kept.
            Assert.DoesNotContain("ST_Intersection(", sql);
        }

        [Fact]
        public void BuildLoadQuery_WithAoiPolygonAndClip_ClipsToPolygonEvenWithPreserveSource()
        {
            string wkt = "POLYGON((1 2, 3 2, 2 4, 1 2))";
            var sql = S3Ingester.BuildLoadQuery("roads.parquet", "water",
                new[] { "id", "geometry", "bbox", "sources" }, new ExtentBounds(1, 2, 3, 4),
                clipGeometry: true, preserveSource: true, aoiPolygonWkt: wkt);

            // The ingest runs with preserveSource (the _source archive), and an
            // explicit clip request still clips — same contract as the
            // attribute filter: it applies to source and display alike.
            Assert.Contains($"ST_Intersection(geometry, ST_GeomFromText('{wkt}'))", sql);
            // bbox struct is rebuilt from the clipped geometry.
            Assert.Contains("struct_pack", sql);
            // preserveSource must not add derived/provenance columns.
            Assert.DoesNotContain("AS source_datasets", sql);
            Assert.Contains("* EXCLUDE(geometry, bbox)", sql);
        }

        [Theory]
        [InlineData("address")]
        [InlineData("place")]
        [InlineData("connector")]
        public void BuildLoadQuery_PointOnlyTypes_NeverClipped(string actualType)
        {
            var sql = S3Ingester.BuildLoadQuery("data.parquet", actualType,
                new[] { "id", "geometry", "bbox" }, new ExtentBounds(1, 2, 3, 4),
                clipGeometry: true, aoiPolygonWkt: "POLYGON((1 2, 3 2, 2 4, 1 2))");

            // Clipping an intersecting point is a no-op, so skip ST_Intersection
            // but keep the exact polygon intersects filter.
            Assert.DoesNotContain("ST_Intersection(", sql);
            Assert.Contains("ST_Intersects(geometry, ST_GeomFromText(", sql);
        }

        [Fact]
        public void BuildLoadQuery_PreserveSourceWithoutClip_Unchanged()
        {
            // The pre-AOI contract: preserveSource without a clip request keeps
            // whole geometries and the full column set.
            var sql = S3Ingester.BuildLoadQuery("roads.parquet", "segment",
                new[] { "id", "geometry", "bbox", "sources" }, new ExtentBounds(1, 2, 3, 4),
                preserveSource: true);

            Assert.DoesNotContain("ST_Intersection(", sql);
            Assert.DoesNotContain("AS source_datasets", sql);
            Assert.Contains("ST_Intersects(geometry,", sql);
        }

        [Fact]
        public void BuildLoadQuery_InvalidAttributeFilter_Throws()
        {
            Assert.Throws<System.ArgumentException>(() => S3Ingester.BuildLoadQuery(
                "roads.parquet", "segment", new[] { "id", "geometry" },
                new ExtentBounds(1, 2, 3, 4), attributeFilter: "1=1; DROP TABLE x"));
        }

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
            // "place" excludes addresses, brand, emails, phones, socials, sources, taxonomy, websites.
            var columns = new List<string> { "id", "geometry", "bbox", "names", "sources", "websites", "brand", "taxonomy" };
            string projection = S3Ingester.BuildColumnProjection("place", columns);

            Assert.NotNull(projection);
            Assert.Contains("id", projection);
            Assert.Contains("names", projection);
            Assert.DoesNotContain("sources", projection);
            Assert.DoesNotContain("websites", projection);
            Assert.DoesNotContain("brand", projection);
            Assert.DoesNotContain("taxonomy", projection); // v2: taxonomy struct excluded, derived as taxonomy_primary instead
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
        public void BuildLoadQuery_PlaceWithTaxonomy_DerivesTaxonomyPrimary()
        {
            // Schema v2: taxonomy struct excluded, taxonomy_primary derived during load
            var columns = new List<string> { "id", "geometry", "taxonomy", "basic_category" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            // taxonomy excluded from projection
            Assert.DoesNotContain(", taxonomy,", query);
            Assert.DoesNotContain(", taxonomy ", query);
            // taxonomy_primary derived as CAST(taxonomy.primary AS VARCHAR)
            Assert.Contains("taxonomy.primary", query);
            Assert.Contains("taxonomy_primary", query);
            // basic_category kept as-is (scalar field)
            Assert.Contains("basic_category", query);
        }

        [Fact]
        public void BuildLoadQuery_PlaceWithCategories_DeriveCategoriesPrimary()
        {
            // Schema v1: categories struct excluded, categories_primary derived during load
            var columns = new List<string> { "id", "geometry", "categories" };
            string query = S3Ingester.BuildLoadQuery("s3://bucket/place/*", "place", columns, extent: null);

            // categories not in v2 exclusions (only taxonomy is), so it's kept but we derive categories_primary
            Assert.Contains("categories.primary", query);
            Assert.Contains("categories_primary", query);
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

            Assert.Contains("string_agg(CAST(a.freeform AS VARCHAR), ' | ')", query);
            Assert.Contains("AS address_freeform", query);
            Assert.Contains("AS address_locality", query);
            Assert.Contains("AS address_region", query);
            Assert.Contains("AS address_postcode", query);
            Assert.Contains("AS address_country", query);
            Assert.DoesNotContain("SELECT id, names, addresses", query);
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
            string query = S3Ingester.BuildLoadQuery("s3://bucket/building/*", "building", columns, extent, clipGeometry: true);

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
            string query = S3Ingester.BuildLoadQuery("s3://bucket/data/*", actualS3Type: null, columns, extent, clipGeometry: true);

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
