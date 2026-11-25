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

        public static string BuildReadParquetExpression(string s3Path, string readParquetOptions = null, IReadOnlyList<string> files = null)
        {
            string options = string.IsNullOrWhiteSpace(readParquetOptions)
                ? S3ReadParquetOptions
                : readParquetOptions;
            string source = files == null ? $"'{GeoParquetSql.EscapeSqlLiteral(s3Path)}'"
                : "[" + string.Join(",", files.Select(f => $"'{GeoParquetSql.EscapeSqlLiteral(f)}'")) + "]";
            return $"read_parquet({source}, {options})";
        }

        /// <summary>
        /// Wraps a read expression so a source whose geometry column has a
        /// different name (or holds WKB bytes) surfaces a GEOMETRY-typed
        /// column called "geometry", which the rest of the pipeline assumes.
        /// Returns the expression unchanged for ordinary GeoParquet.
        /// </summary>
        public static string WrapGeometryColumn(string readExpression, string geometryColumn, bool geometryIsWkb)
        {
            bool rename = !string.IsNullOrWhiteSpace(geometryColumn) &&
                          !geometryColumn.Equals(GeometryColumn, StringComparison.OrdinalIgnoreCase);
            if (!rename && !geometryIsWkb) return readExpression;

            string source = rename ? QuoteIdentifier(geometryColumn) : GeometryColumn;
            string expression = geometryIsWkb ? $"ST_GeomFromWKB({source})" : source;
            return $"(SELECT * EXCLUDE({source}), {expression} AS {GeometryColumn} FROM {readExpression})";
        }

        /// <summary>Quotes a SQL identifier (doubling embedded quotes).</summary>
        public static string QuoteIdentifier(string identifier) =>
            "\"" + (identifier ?? string.Empty).Replace("\"", "\"\"") + "\"";

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
        /// When <paramref name="aoiPolygonWkt"/> is supplied (an internally
        /// built WKT from a drawn polygon AOI, see
        /// <see cref="GeoParquetSql.BuildPolygonWkt"/>), ST_Intersects and
        /// clipping use that polygon while the bbox pushdown keeps using the
        /// polygon's envelope in <paramref name="extent"/>.
        /// </summary>
        public static string BuildLoadQuery(
            string s3Path,
            string actualS3Type,
            IReadOnlyList<string> columnNames,
            ExtentBounds extent,
            bool includeSourceProvenance = false,
            string readParquetOptions = null,
            bool clipGeometry = false,
            bool preserveSource = false, IReadOnlyList<string> files = null,
            string geometryColumn = null, bool geometryIsWkb = false, string attributeFilter = null,
            string aoiPolygonWkt = null)
        {
            var projectedColumns = preserveSource ? null : BuildColumnProjection(actualS3Type, columnNames);
            string readParquet = WrapGeometryColumn(
                BuildReadParquetExpression(s3Path, readParquetOptions, files), geometryColumn, geometryIsWkb);

            // Validated-or-throw: the UI reports validation errors before any
            // query is built, so this is the trust-boundary backstop.
            string attributePredicate = GeoParquetSql.BuildAttributeFilterPredicate(attributeFilter);

            string spatialFilter = "";
            string extentPolygon = "";
            if (extent != null)
            {
                extentPolygon = string.IsNullOrWhiteSpace(aoiPolygonWkt)
                    ? GeoParquetSql.BuildExtentPolygon(extent)
                    : GeoParquetSql.BuildAoiPolygonExpression(aoiPolygonWkt);

                // Bbox overlap first (pushdown), then ST_Intersects so only features whose geometry
                // actually intersects the extent are kept — prevents data extending beyond the frame.
                spatialFilter = $@"
                        WHERE {(columnNames?.Any(c => c.Equals(BboxColumn, StringComparison.OrdinalIgnoreCase)) == true ? GeoParquetSql.BuildBboxOverlapPredicate(extent) + " AND " : "")}ST_Intersects(geometry, {extentPolygon})";
                if (attributePredicate != null)
                    spatialFilter += $" AND {attributePredicate}";
            }
            else if (attributePredicate != null)
            {
                spatialFilter = $"WHERE {attributePredicate}";
            }

            // Determine which columns to project (if any) while always retaining geometry
            var projectedColumnList = preserveSource ? "*" : AppendDownloadDerivedColumns(projectedColumns ?? "*", actualS3Type, columnNames, includeSourceProvenance);
            bool hasBboxColumn = columnNames != null && columnNames.Any(c => c.Equals(BboxColumn, StringComparison.OrdinalIgnoreCase));
            // Clipping an intersecting point to the AOI is a no-op, so point-only
            // types always keep their original geometry. preserveSource no longer
            // suppresses clipping: a clip request applies to the ingest, so both
            // the _source archive and display layers reflect it (same contract as
            // the attribute filter).
            bool preservesOriginalGeometry = !clipGeometry || IsPointOnlyType(actualS3Type);
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
            if (!preserveSource)
            {
                projectedColumnsWithoutGeometry = AppendDownloadDerivedColumns(projectedColumnsWithoutGeometry, actualS3Type, columnNames, includeSourceProvenance);
                projectedColumnsWithoutGeometryOrBbox = AppendDownloadDerivedColumns(projectedColumnsWithoutGeometryOrBbox, actualS3Type, columnNames, includeSourceProvenance);
            }

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

        public static string BuildDisplayTableCommand(string type, IReadOnlyList<string> columns, bool provenance)
        {
            string projection = BuildColumnProjection(type, columns) ?? "*";
            projection = AppendDownloadDerivedColumns(projection, type, columns, provenance);
            return $"CREATE OR REPLACE TABLE current_table AS SELECT {projection} FROM current_table";
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
            if (!string.Equals(actualS3Type, "place", StringComparison.OrdinalIgnoreCase) || columnNames == null)
            {
                return projection;
            }

            var derivedColumns = new List<string>();

            // Derive flattened address fields from addresses array
            if (columnNames.Any(c => c.Equals("addresses", StringComparison.OrdinalIgnoreCase)))
            {
                AddDerivedAddressColumn(derivedColumns, columnNames, "address_freeform",
                    JoinListValues("addresses", AddressItem, "freeform", distinct: false));
                AddDerivedAddressColumn(derivedColumns, columnNames, "address_locality",
                    JoinListValues("addresses", AddressItem, "locality", distinct: true));
                AddDerivedAddressColumn(derivedColumns, columnNames, "address_region",
                    JoinListValues("addresses", AddressItem, "region", distinct: true));
                AddDerivedAddressColumn(derivedColumns, columnNames, "address_postcode",
                    JoinListValues("addresses", AddressItem, "postcode", distinct: true,
                        valueExpression: $"left(CAST({AddressItem}.postcode AS VARCHAR), 5)"));
                AddDerivedAddressColumn(derivedColumns, columnNames, "address_country",
                    JoinListValues("addresses", AddressItem, "country", distinct: true));
            }

            // Schema v2 (2026-09-23+): Derive taxonomy_primary from taxonomy struct (v2) or categories struct (v1).
            // Taxonomy contains primary/hierarchy/alternates arrays; extract just the primary scalar for ArcGIS.
            if (columnNames.Any(c => c.Equals("taxonomy", StringComparison.OrdinalIgnoreCase)))
            {
                AddDerivedAddressColumn(derivedColumns, columnNames, "taxonomy_primary",
                    "CAST(taxonomy.primary AS VARCHAR)");
            }
            else if (columnNames.Any(c => c.Equals("categories", StringComparison.OrdinalIgnoreCase)))
            {
                // v1 fallback: categories struct also has .primary
                AddDerivedAddressColumn(derivedColumns, columnNames, "categories_primary",
                    "CAST(categories.primary AS VARCHAR)");
            }

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
                $"COALESCE(len(list_filter(sources, lambda {SourceItem}: {SourceItem}.dataset IS NOT NULL OR {SourceItem}.record_id IS NOT NULL)), 0)");
            AddSourceColumn(derivedColumns, columnNames, "source_datasets",
                JoinListValues("sources", SourceItem, "dataset", distinct: true));
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

        // Per-row list functions instead of correlated "(SELECT ... FROM unnest(list))"
        // subqueries: DuckDB evaluates these as vectorized expressions, where each
        // correlated subquery became its own join. On a 250k-feature provenance
        // download the display table went from minutes to seconds (see
        // DuckDBGeoparquet.Tests/PipelineBenchmarkTests.cs). Lambda parameter
        // names are distinctive so they never shadow a real column.
        private const string AddressItem = "addr_item";
        private const string SourceItem = "src_item";

        /// <summary>Values of <paramref name="field"/> that are non-null and not blank, in list order.</summary>
        private static string NonBlankListValues(string listColumn, string item, string field, string valueExpression = null) =>
            $"list_transform(list_filter({listColumn}, lambda {item}: {item}.{field} IS NOT NULL AND trim(CAST({item}.{field} AS VARCHAR)) <> ''), " +
            $"lambda {item}: {valueExpression ?? $"CAST({item}.{field} AS VARCHAR)"})";

        /// <summary>
        /// ' | '-joined non-blank values, NULL when there are none (matching
        /// string_agg over an empty set). Distinct values keep first-seen order.
        /// </summary>
        private static string JoinListValues(string listColumn, string item, string field, bool distinct, string valueExpression = null)
        {
            string values = NonBlankListValues(listColumn, item, field, valueExpression);
            if (distinct)
                values = $"list_filter({values}, lambda dv_item, dv_index: list_position({values}, dv_item) = dv_index)";
            return $"NULLIF(array_to_string({values}, ' | '), '')";
        }

        private static string PrimarySourceField(string fieldName) =>
            $"{NonBlankListValues("sources", SourceItem, fieldName)}[1]";

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
