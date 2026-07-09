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

        private static string AppendDownloadDerivedColumns(string projection, string actualS3Type, IReadOnlyList<string> columnNames, bool includeSourceProvenance)
        {
            projection = AppendPlaceDerivedColumns(projection, actualS3Type, columnNames);
            return includeSourceProvenance
                ? AppendSourceProvenanceColumns(projection, columnNames)
                : projection;
        }

        private static string AppendPlaceDerivedColumns(string projection, string actualS3Type, IReadOnlyList<string> columnNames)
        {
            if (!string.Equals(actualS3Type, "place", StringComparison.OrdinalIgnoreCase) ||
                columnNames == null ||
                !columnNames.Any(c => c.Equals("addresses", StringComparison.OrdinalIgnoreCase)))
            {
                return projection;
            }

            var derivedColumns = new List<string>();
            AddDerivedAddressColumn(derivedColumns, columnNames, "address_freeform",
                "(SELECT string_agg(CAST(a.freeform AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.freeform IS NOT NULL AND trim(CAST(a.freeform AS VARCHAR)) <> '')");
            AddDerivedAddressColumn(derivedColumns, columnNames, "address_locality",
                "(SELECT string_agg(DISTINCT CAST(a.locality AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.locality IS NOT NULL AND trim(CAST(a.locality AS VARCHAR)) <> '')");
            AddDerivedAddressColumn(derivedColumns, columnNames, "address_region",
                "(SELECT string_agg(DISTINCT CAST(a.region AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.region IS NOT NULL AND trim(CAST(a.region AS VARCHAR)) <> '')");
            AddDerivedAddressColumn(derivedColumns, columnNames, "address_postcode",
                "(SELECT string_agg(DISTINCT left(CAST(a.postcode AS VARCHAR), 5), ' | ') FROM unnest(addresses) AS t(a) WHERE a.postcode IS NOT NULL AND trim(CAST(a.postcode AS VARCHAR)) <> '')");
            AddDerivedAddressColumn(derivedColumns, columnNames, "address_country",
                "(SELECT string_agg(DISTINCT CAST(a.country AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.country IS NOT NULL AND trim(CAST(a.country AS VARCHAR)) <> '')");

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

        private static void AddDerivedAddressColumn(
            ICollection<string> derivedColumns,
            IReadOnlyList<string> columnNames,
            string columnName,
            string expression)
        {
            if (columnNames.Any(c => c.Equals(columnName, StringComparison.OrdinalIgnoreCase)))
                return;

            derivedColumns.Add($"{expression} AS {columnName}");
        }

        private static void AddSourceColumn(
            ICollection<string> derivedColumns,
            IReadOnlyList<string> columnNames,
            string columnName,
            string expression)
        {
            if (columnNames.Any(c => c.Equals(columnName, StringComparison.OrdinalIgnoreCase)))
                return;

            derivedColumns.Add($"{expression} AS {columnName}");
        }
    }
}
