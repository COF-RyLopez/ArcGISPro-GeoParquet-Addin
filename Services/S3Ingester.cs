using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Pure query-text builders for the S3 → DuckDB ingest step. Given a set of
    /// discovered column names and the requested extent, these produce the
    /// column projection and the CREATE OR REPLACE TABLE current_table query
    /// that <see cref="DataProcessor.IngestFileAsync"/> executes.
    ///
    /// No ArcGIS or DuckDB dependencies — everything here is deterministic
    /// string construction, which makes it unit-testable without ArcGIS Pro
    /// running (see DuckDBGeoparquet.Tests). Extracted from DataProcessor
    /// (Phase 2c stage 2).
    /// </summary>
    public static class S3Ingester
    {
        private const string BboxColumn = "bbox";
        private const string GeometryColumn = "geometry";
        public const string S3FastReadParquetOptions = "filename=true, hive_partitioning=1";
        public const string S3ReadParquetOptions = $"{S3FastReadParquetOptions}, union_by_name=1";

        public static string BuildReadParquetExpression(string s3Path, string readParquetOptions = null)
        {
            string options = string.IsNullOrWhiteSpace(readParquetOptions)
                ? S3ReadParquetOptions
                : readParquetOptions;
            return $"read_parquet('{GeoParquetSql.EscapeSqlLiteral(s3Path)}', {options})";
        }

        /// <summary>
        /// Build a projection list that drops known heavy optional columns for the given dataset type.
        /// Returns null to indicate "SELECT *" when no drops apply.
        /// </summary>
        public static string BuildColumnProjection(string actualS3Type, IReadOnlyList<string> columnNames)
        {
            if (string.IsNullOrWhiteSpace(actualS3Type) || columnNames == null || columnNames.Count == 0)
                return null;

            if (!OvertureSchema.ColumnExclusions.TryGetValue(actualS3Type, out var dropSet) || dropSet.Count == 0)
                return null;

            var projected = columnNames
                .Where(name =>
                    // Never drop geometry or bbox even if listed
                    name.Equals(GeometryColumn, StringComparison.OrdinalIgnoreCase) ||
                    name.StartsWith($"{BboxColumn}_", StringComparison.OrdinalIgnoreCase) ||
                    !dropSet.Contains(name))
                .ToList();

            // If projection would drop everything (unlikely), fall back to *
            if (!projected.Any())
                return null;

            return string.Join(", ", projected);
        }

        /// <summary>
        /// Builds the CREATE OR REPLACE TABLE current_table query for an ingest.
        /// Applies the type-specific column projection, and — when an extent is
        /// supplied — the bbox pushdown + ST_Intersects spatial filter and
        /// ST_Intersection clipping (preserving the bbox struct when present).
        /// </summary>
        public static string BuildLoadQuery(
            string s3Path,
            string actualS3Type,
            IReadOnlyList<string> columnNames,
            ExtentBounds extent,
            bool includeSourceProvenance = false,
            string readParquetOptions = null)
        {
            var projectedColumns = BuildColumnProjection(actualS3Type, columnNames);
            string readParquet = BuildReadParquetExpression(s3Path, readParquetOptions);

            string spatialFilter = "";
            string extentPolygon = "";
            if (extent != null)
            {
                extentPolygon = GeoParquetSql.BuildExtentPolygon(extent);

                // Bbox overlap first (pushdown), then ST_Intersects so only features whose geometry
                // actually intersects the extent are kept — prevents data extending beyond the frame.
                spatialFilter = $@"
                        WHERE {GeoParquetSql.BuildBboxOverlapPredicate(extent)}
                          AND ST_Intersects(geometry, {extentPolygon})";
            }

            // Determine which columns to project (if any) while always retaining geometry
            var projectedColumnList = AppendDownloadDerivedColumns(projectedColumns ?? "*", actualS3Type, columnNames, includeSourceProvenance);
            bool hasBboxColumn = columnNames != null && columnNames.Any(c => c.Equals(BboxColumn, StringComparison.OrdinalIgnoreCase));
            bool preservesOriginalGeometry = IsPointOnlyType(actualS3Type);
            var projectedColumnsWithoutGeometry = projectedColumns != null
                ? string.Join(", ", projectedColumns.Split(',').Select(c => c.Trim()).Where(c => !c.Equals("geometry", StringComparison.OrdinalIgnoreCase)))
                : "* EXCLUDE(geometry)";
            var projectedColumnsWithoutGeometryOrBbox = projectedColumns != null
                ? string.Join(", ", projectedColumns.Split(',').Select(c => c.Trim()).Where(c =>
                    !c.Equals("geometry", StringComparison.OrdinalIgnoreCase) &&
                    !c.Equals(BboxColumn, StringComparison.OrdinalIgnoreCase)))
                : (hasBboxColumn ? "* EXCLUDE(geometry, bbox)" : "* EXCLUDE(geometry)");
            if (string.IsNullOrWhiteSpace(projectedColumnsWithoutGeometry))
                projectedColumnsWithoutGeometry = "* EXCLUDE(geometry)";
            if (string.IsNullOrWhiteSpace(projectedColumnsWithoutGeometryOrBbox))
                projectedColumnsWithoutGeometryOrBbox = hasBboxColumn ? "* EXCLUDE(geometry, bbox)" : "* EXCLUDE(geometry)";
            projectedColumnsWithoutGeometry = AppendDownloadDerivedColumns(projectedColumnsWithoutGeometry, actualS3Type, columnNames, includeSourceProvenance);
            projectedColumnsWithoutGeometryOrBbox = AppendDownloadDerivedColumns(projectedColumnsWithoutGeometryOrBbox, actualS3Type, columnNames, includeSourceProvenance);

            if (extent != null && !string.IsNullOrEmpty(extentPolygon))
            {
                if (preservesOriginalGeometry)
                {
                    return $@"
                        CREATE OR REPLACE TABLE current_table AS
                        SELECT
                            {projectedColumnList},
                            ST_GeometryType(geometry) AS {GeoParquetSql.InternalGeometryTypeColumn}
                        FROM {readParquet}
                        {spatialFilter}";
                }

                // Clip geometries to extent - this keeps all intersecting features but clips them to the extent
                // Use ST_Intersection to clip after the WHERE ST_Intersects filter has already refined matches.
                if (hasBboxColumn)
                {
                    return $@"
                        CREATE OR REPLACE TABLE current_table AS
                        WITH clipped AS (
                            SELECT
                                {projectedColumnsWithoutGeometryOrBbox},
                                ST_Intersection(geometry, {extentPolygon}) AS clipped_geometry
                            FROM {readParquet}
                            {spatialFilter}
                        )
                        SELECT
                            * EXCLUDE(clipped_geometry),
                            clipped_geometry AS geometry,
                            ST_GeometryType(clipped_geometry) AS {GeoParquetSql.InternalGeometryTypeColumn},
                            CASE
                                WHEN clipped_geometry IS NOT NULL THEN struct_pack(
                                    xmin := ST_XMin(clipped_geometry),
                                    ymin := ST_YMin(clipped_geometry),
                                    xmax := ST_XMax(clipped_geometry),
                                    ymax := ST_YMax(clipped_geometry)
                                )
                                ELSE NULL
                            END AS bbox
                        FROM clipped";
                }

                return $@"
                        CREATE OR REPLACE TABLE current_table AS
                        WITH clipped AS (
                            SELECT
                                {projectedColumnsWithoutGeometry},
                                ST_Intersection(geometry, {extentPolygon}) AS clipped_geometry
                            FROM {readParquet}
                            {spatialFilter}
                        )
                        SELECT
                            * EXCLUDE(clipped_geometry),
                            clipped_geometry AS geometry,
                            ST_GeometryType(clipped_geometry) AS {GeoParquetSql.InternalGeometryTypeColumn}
                        FROM clipped";
            }

            return $@"
                        CREATE OR REPLACE TABLE current_table AS
                        SELECT
                            {projectedColumnList},
                            ST_GeometryType(geometry) AS {GeoParquetSql.InternalGeometryTypeColumn}
                        FROM {readParquet}
                        {spatialFilter}";
        }

        public static bool IsPointOnlyType(string actualS3Type)
        {
            return string.Equals(actualS3Type, "address", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(actualS3Type, "place", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(actualS3Type, "connector", StringComparison.OrdinalIgnoreCase);
        }

        public static IReadOnlyList<string> GetFlattenedFieldNames(
            string actualS3Type,
            IReadOnlyList<string> columnNames,
            bool includeSourceProvenance = false)
        {
            var flattened = new List<string>();
            AddSharedFlattenedFieldNames(flattened, columnNames);

            if (string.Equals(actualS3Type, "place", StringComparison.OrdinalIgnoreCase))
                AddPlaceFlattenedFieldNames(flattened, columnNames);

            if (string.Equals(actualS3Type, "segment", StringComparison.OrdinalIgnoreCase))
                AddSegmentFlattenedFieldNames(flattened, columnNames);

            if (includeSourceProvenance)
                AddSourceFlattenedFieldNames(flattened, columnNames);

            return flattened;
        }

        private static string AppendDownloadDerivedColumns(string projection, string actualS3Type, IReadOnlyList<string> columnNames, bool includeSourceProvenance)
        {
            projection = AppendSharedDerivedColumns(projection, columnNames);
            projection = AppendPlaceDerivedColumns(projection, actualS3Type, columnNames);
            projection = AppendSegmentDerivedColumns(projection, actualS3Type, columnNames);
            return includeSourceProvenance
                ? AppendSourceProvenanceColumns(projection, columnNames)
                : projection;
        }

        private static string AppendSharedDerivedColumns(string projection, IReadOnlyList<string> columnNames)
        {
            var derivedColumns = new List<string>();

            if (HasColumn(columnNames, "names"))
            {
                AddDerivedColumn(derivedColumns, columnNames, "names_primary", "TRY_CAST(names.primary AS VARCHAR)");
                AddDerivedColumn(derivedColumns, columnNames, "names_common", "TRY_CAST(names.common AS VARCHAR)");
                AddDerivedColumn(derivedColumns, columnNames, "display_name",
                    "COALESCE(NULLIF(trim(TRY_CAST(names.primary AS VARCHAR)), ''), NULLIF(trim(TRY_CAST(names.common AS VARCHAR)), ''))");
            }

            if (HasColumn(columnNames, "cartography"))
            {
                AddDerivedColumn(derivedColumns, columnNames, "cartography_prominence", "TRY_CAST(cartography.prominence AS INTEGER)");
                AddDerivedColumn(derivedColumns, columnNames, "cartography_min_zoom", "TRY_CAST(cartography.min_zoom AS INTEGER)");
                AddDerivedColumn(derivedColumns, columnNames, "cartography_max_zoom", "TRY_CAST(cartography.max_zoom AS INTEGER)");
                AddDerivedColumn(derivedColumns, columnNames, "cartography_sort_key", "TRY_CAST(cartography.sort_key AS INTEGER)");
                AddDerivedColumn(derivedColumns, columnNames, "cartography_min_scale", "CAST(ROUND(591657527.591555 / power(2, TRY_CAST(cartography.min_zoom AS DOUBLE))) AS BIGINT)");
                AddDerivedColumn(derivedColumns, columnNames, "cartography_max_scale", "CAST(ROUND(591657527.591555 / power(2, TRY_CAST(cartography.max_zoom AS DOUBLE))) AS BIGINT)");
            }

            return derivedColumns.Count == 0
                ? projection
                : $"{projection}, {string.Join(", ", derivedColumns)}";
        }

        private static string AppendPlaceDerivedColumns(string projection, string actualS3Type, IReadOnlyList<string> columnNames)
        {
            if (!string.Equals(actualS3Type, "place", StringComparison.OrdinalIgnoreCase) || columnNames == null)
            {
                return projection;
            }

            var derivedColumns = new List<string>();

            if (HasColumn(columnNames, "categories"))
            {
                AddDerivedColumn(derivedColumns, columnNames, "categories_primary", "TRY_CAST(categories.primary AS VARCHAR)");
                AddDerivedColumn(derivedColumns, columnNames, "categories_alternate", "TRY_CAST(categories.alternate AS VARCHAR)");
            }

            if (HasColumn(columnNames, "basic_category"))
                AddDerivedColumn(derivedColumns, columnNames, "place_basic_category", "TRY_CAST(basic_category AS VARCHAR)");

            if (HasColumn(columnNames, "taxonomy"))
                AddDerivedColumn(derivedColumns, columnNames, "taxonomy_summary", "TRY_CAST(taxonomy AS VARCHAR)");

            if (HasColumn(columnNames, "operating_status"))
                AddDerivedColumn(derivedColumns, columnNames, "place_operating_status", "TRY_CAST(operating_status AS VARCHAR)");

            if (HasColumn(columnNames, "brand"))
            {
                AddDerivedColumn(derivedColumns, columnNames, "brand_wikidata", "TRY_CAST(brand.wikidata AS VARCHAR)");
                AddDerivedColumn(derivedColumns, columnNames, "brand_names_primary", "TRY_CAST(brand.names.primary AS VARCHAR)");
            }

            if (HasColumn(columnNames, "addresses"))
            {
                AddDerivedColumn(derivedColumns, columnNames, "address_freeform",
                    "(SELECT string_agg(CAST(a.freeform AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.freeform IS NOT NULL AND trim(CAST(a.freeform AS VARCHAR)) <> '')");
                AddDerivedColumn(derivedColumns, columnNames, "address_locality",
                    "(SELECT string_agg(DISTINCT CAST(a.locality AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.locality IS NOT NULL AND trim(CAST(a.locality AS VARCHAR)) <> '')");
                AddDerivedColumn(derivedColumns, columnNames, "address_region",
                    "(SELECT string_agg(DISTINCT CAST(a.region AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.region IS NOT NULL AND trim(CAST(a.region AS VARCHAR)) <> '')");
                AddDerivedColumn(derivedColumns, columnNames, "address_postcode",
                    "(SELECT string_agg(DISTINCT left(CAST(a.postcode AS VARCHAR), 5), ' | ') FROM unnest(addresses) AS t(a) WHERE a.postcode IS NOT NULL AND trim(CAST(a.postcode AS VARCHAR)) <> '')");
                AddDerivedColumn(derivedColumns, columnNames, "address_country",
                    "(SELECT string_agg(DISTINCT CAST(a.country AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.country IS NOT NULL AND trim(CAST(a.country AS VARCHAR)) <> '')");
            }

            return derivedColumns.Count == 0
                ? projection
                : $"{projection}, {string.Join(", ", derivedColumns)}";
        }

        private static string AppendSegmentDerivedColumns(string projection, string actualS3Type, IReadOnlyList<string> columnNames)
        {
            if (!string.Equals(actualS3Type, "segment", StringComparison.OrdinalIgnoreCase) || columnNames == null)
                return projection;

            var derivedColumns = new List<string>();

            if (HasColumn(columnNames, "connectors"))
                AddDerivedColumn(derivedColumns, columnNames, "connector_count", "(SELECT COUNT(*) FROM unnest(connectors) AS t(c))");

            if (HasColumn(columnNames, "road_surface"))
                AddDerivedColumn(derivedColumns, columnNames, "road_surface_values",
                    "(SELECT string_agg(DISTINCT TRY_CAST(r.value AS VARCHAR), ' | ') FROM unnest(road_surface) AS t(r) WHERE r.value IS NOT NULL)");

            if (HasColumn(columnNames, "speed_limits"))
                AddDerivedColumn(derivedColumns, columnNames, "speed_limit_rule_count", "(SELECT COUNT(*) FROM unnest(speed_limits) AS t(s))");

            if (HasColumn(columnNames, "access_restrictions"))
                AddDerivedColumn(derivedColumns, columnNames, "access_restriction_count", "(SELECT COUNT(*) FROM unnest(access_restrictions) AS t(a))");

            if (HasColumn(columnNames, "routes"))
                AddDerivedColumn(derivedColumns, columnNames, "route_count", "(SELECT COUNT(*) FROM unnest(routes) AS t(r))");

            return derivedColumns.Count == 0
                ? projection
                : $"{projection}, {string.Join(", ", derivedColumns)}";
        }

        private static string AppendSourceProvenanceColumns(string projection, IReadOnlyList<string> columnNames)
        {
            if (columnNames == null ||
                !columnNames.Any(c => c.Equals("sources", StringComparison.OrdinalIgnoreCase)))
            {
                return projection;
            }

            var derivedColumns = new List<string>();
            AddSourceColumn(derivedColumns, columnNames, "source_count",
                "(SELECT COUNT(*) FROM unnest(sources) AS t(s) WHERE s.dataset IS NOT NULL OR s.record_id IS NOT NULL)");
            AddSourceColumn(derivedColumns, columnNames, "source_datasets",
                "(SELECT string_agg(DISTINCT CAST(s.dataset AS VARCHAR), ' | ') FROM unnest(sources) AS t(s) WHERE s.dataset IS NOT NULL AND trim(CAST(s.dataset AS VARCHAR)) <> '')");
            AddSourceColumn(derivedColumns, columnNames, "source_primary_dataset", PrimarySourceField("dataset"));
            AddSourceColumn(derivedColumns, columnNames, "source_primary_record_id", PrimarySourceField("record_id"));
            AddSourceColumn(derivedColumns, columnNames, "source_primary_update_time", PrimarySourceField("update_time"));

            string primaryDataset = PrimarySourceField("dataset");
            string primaryRecordId = PrimarySourceField("record_id");
            string editUrl = BuildSourceEditUrlExpression(primaryDataset, primaryRecordId);
            string contributionUrl = BuildSourceContributionUrlExpression(primaryDataset);

            AddSourceColumn(derivedColumns, columnNames, "source_url",
                $"COALESCE({editUrl}, {contributionUrl})");
            AddSourceColumn(derivedColumns, columnNames, "source_edit_url", editUrl);
            AddSourceColumn(derivedColumns, columnNames, "source_contribution_url", contributionUrl);
            AddSourceColumn(derivedColumns, columnNames, "source_edit_platform", BuildSourceEditPlatformExpression(primaryDataset));

            return derivedColumns.Count == 0
                ? projection
                : $"{projection}, {string.Join(", ", derivedColumns)}";
        }

        private static string PrimarySourceField(string fieldName) =>
            $"(SELECT CAST(s.{fieldName} AS VARCHAR) FROM unnest(sources) AS t(s) WHERE s.{fieldName} IS NOT NULL AND trim(CAST(s.{fieldName} AS VARCHAR)) <> '' LIMIT 1)";

        private static string BuildSourceEditUrlExpression(string datasetExpression, string recordIdExpression) =>
            $@"CASE
                    WHEN {datasetExpression} IS NULL OR {recordIdExpression} IS NULL OR lower({datasetExpression}) NOT LIKE '%openstreetmap%' THEN NULL
                    WHEN regexp_matches({recordIdExpression}, '^n[0-9]+(@.*)?$')
                        THEN 'https://www.openstreetmap.org/edit?node=' || regexp_extract({recordIdExpression}, '^n([0-9]+)', 1)
                    WHEN regexp_matches({recordIdExpression}, '^w[0-9]+(@.*)?$')
                        THEN 'https://www.openstreetmap.org/edit?way=' || regexp_extract({recordIdExpression}, '^w([0-9]+)', 1)
                    WHEN regexp_matches({recordIdExpression}, '^r[0-9]+(@.*)?$')
                        THEN 'https://www.openstreetmap.org/edit?relation=' || regexp_extract({recordIdExpression}, '^r([0-9]+)', 1)
                    ELSE NULL
                END";

        private static string BuildSourceContributionUrlExpression(string datasetExpression) =>
            $@"CASE
                    WHEN {datasetExpression} IS NULL OR trim({datasetExpression}) = '' THEN '{GersSourceEditResolver.OvertureBridgeDocsUrl}'
                    WHEN lower({datasetExpression}) LIKE '%openstreetmap%' THEN '{GersSourceEditResolver.OpenStreetMapContributionUrl}'
                    WHEN lower({datasetExpression}) LIKE '%esri%' THEN '{GersSourceEditResolver.EsriCommunityMapsContributionUrl}'
                    WHEN lower({datasetExpression}) LIKE '%meta%' THEN '{GersSourceEditResolver.MetaContributionUrl}'
                    WHEN lower({datasetExpression}) LIKE '%microsoft%' THEN '{GersSourceEditResolver.MicrosoftMapsContributionUrl}'
                    WHEN lower({datasetExpression}) LIKE '%pinmeto%' THEN '{GersSourceEditResolver.PinMeToContributionUrl}'
                    WHEN lower({datasetExpression}) LIKE '%geoboundaries%' THEN '{GersSourceEditResolver.GeoBoundariesContributionUrl}'
                    ELSE '{GersSourceEditResolver.OvertureBridgeDocsUrl}'
                END";

        private static string BuildSourceEditPlatformExpression(string datasetExpression) =>
            $@"CASE
                    WHEN {datasetExpression} IS NULL OR trim({datasetExpression}) = '' THEN 'Unknown source'
                    WHEN lower({datasetExpression}) LIKE '%openstreetmap%' THEN 'OpenStreetMap'
                    WHEN lower({datasetExpression}) LIKE '%esri%' THEN 'Esri Community Maps'
                    WHEN lower({datasetExpression}) LIKE '%meta%' THEN 'Meta'
                    WHEN lower({datasetExpression}) LIKE '%microsoft%' THEN 'Microsoft'
                    WHEN lower({datasetExpression}) LIKE '%pinmeto%' THEN 'PinMeTo'
                    WHEN lower({datasetExpression}) LIKE '%geoboundaries%' THEN 'geoBoundaries'
                    ELSE {datasetExpression}
                END";

        private static void AddSourceColumn(
            ICollection<string> derivedColumns,
            IReadOnlyList<string> columnNames,
            string columnName,
            string expression)
        {
            AddDerivedColumn(derivedColumns, columnNames, columnName, expression);
        }

        private static void AddDerivedColumn(
            ICollection<string> derivedColumns,
            IReadOnlyList<string> columnNames,
            string columnName,
            string expression)
        {
            if (HasColumn(columnNames, columnName))
                return;

            derivedColumns.Add($"{expression} AS {columnName}");
        }

        private static bool HasColumn(IReadOnlyList<string> columnNames, string columnName)
        {
            return columnNames != null &&
                   columnNames.Any(c => c.Equals(columnName, StringComparison.OrdinalIgnoreCase));
        }

        private static void AddSharedFlattenedFieldNames(ICollection<string> flattened, IReadOnlyList<string> columnNames)
        {
            if (HasColumn(columnNames, "names"))
            {
                AddFlattenedFieldName(flattened, columnNames, "names_primary");
                AddFlattenedFieldName(flattened, columnNames, "names_common");
                AddFlattenedFieldName(flattened, columnNames, "display_name");
            }

            if (HasColumn(columnNames, "cartography"))
            {
                AddFlattenedFieldName(flattened, columnNames, "cartography_prominence");
                AddFlattenedFieldName(flattened, columnNames, "cartography_min_zoom");
                AddFlattenedFieldName(flattened, columnNames, "cartography_max_zoom");
                AddFlattenedFieldName(flattened, columnNames, "cartography_sort_key");
                AddFlattenedFieldName(flattened, columnNames, "cartography_min_scale");
                AddFlattenedFieldName(flattened, columnNames, "cartography_max_scale");
            }
        }

        private static void AddPlaceFlattenedFieldNames(ICollection<string> flattened, IReadOnlyList<string> columnNames)
        {
            if (HasColumn(columnNames, "categories"))
            {
                AddFlattenedFieldName(flattened, columnNames, "categories_primary");
                AddFlattenedFieldName(flattened, columnNames, "categories_alternate");
            }

            if (HasColumn(columnNames, "basic_category"))
                AddFlattenedFieldName(flattened, columnNames, "place_basic_category");

            if (HasColumn(columnNames, "taxonomy"))
                AddFlattenedFieldName(flattened, columnNames, "taxonomy_summary");

            if (HasColumn(columnNames, "operating_status"))
                AddFlattenedFieldName(flattened, columnNames, "place_operating_status");

            if (HasColumn(columnNames, "brand"))
            {
                AddFlattenedFieldName(flattened, columnNames, "brand_wikidata");
                AddFlattenedFieldName(flattened, columnNames, "brand_names_primary");
            }

            if (HasColumn(columnNames, "addresses"))
            {
                AddFlattenedFieldName(flattened, columnNames, "address_freeform");
                AddFlattenedFieldName(flattened, columnNames, "address_locality");
                AddFlattenedFieldName(flattened, columnNames, "address_region");
                AddFlattenedFieldName(flattened, columnNames, "address_postcode");
                AddFlattenedFieldName(flattened, columnNames, "address_country");
            }
        }

        private static void AddSegmentFlattenedFieldNames(ICollection<string> flattened, IReadOnlyList<string> columnNames)
        {
            if (HasColumn(columnNames, "connectors"))
                AddFlattenedFieldName(flattened, columnNames, "connector_count");
            if (HasColumn(columnNames, "road_surface"))
                AddFlattenedFieldName(flattened, columnNames, "road_surface_values");
            if (HasColumn(columnNames, "speed_limits"))
                AddFlattenedFieldName(flattened, columnNames, "speed_limit_rule_count");
            if (HasColumn(columnNames, "access_restrictions"))
                AddFlattenedFieldName(flattened, columnNames, "access_restriction_count");
            if (HasColumn(columnNames, "routes"))
                AddFlattenedFieldName(flattened, columnNames, "route_count");
        }

        private static void AddSourceFlattenedFieldNames(ICollection<string> flattened, IReadOnlyList<string> columnNames)
        {
            if (!HasColumn(columnNames, "sources"))
                return;

            AddFlattenedFieldName(flattened, columnNames, "source_count");
            AddFlattenedFieldName(flattened, columnNames, "source_datasets");
            AddFlattenedFieldName(flattened, columnNames, "source_primary_dataset");
            AddFlattenedFieldName(flattened, columnNames, "source_primary_record_id");
            AddFlattenedFieldName(flattened, columnNames, "source_primary_update_time");
            AddFlattenedFieldName(flattened, columnNames, "source_url");
            AddFlattenedFieldName(flattened, columnNames, "source_edit_url");
            AddFlattenedFieldName(flattened, columnNames, "source_contribution_url");
            AddFlattenedFieldName(flattened, columnNames, "source_edit_platform");
        }

        private static void AddFlattenedFieldName(ICollection<string> flattened, IReadOnlyList<string> columnNames, string columnName)
        {
            if (HasColumn(columnNames, columnName) ||
                flattened.Contains(columnName, StringComparer.OrdinalIgnoreCase))
            {
                return;
            }

            flattened.Add(columnName);
        }
    }
}
