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
        public static string BuildLoadQuery(string s3Path, string actualS3Type, IReadOnlyList<string> columnNames, ExtentBounds extent)
        {
            var projectedColumns = BuildColumnProjection(actualS3Type, columnNames);

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
            var projectedColumnList = AppendPlaceDerivedColumns(projectedColumns ?? "*", actualS3Type, columnNames);
            bool hasBboxColumn = columnNames != null && columnNames.Any(c => c.Equals(BboxColumn, StringComparison.OrdinalIgnoreCase));
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
            projectedColumnsWithoutGeometry = AppendPlaceDerivedColumns(projectedColumnsWithoutGeometry, actualS3Type, columnNames);
            projectedColumnsWithoutGeometryOrBbox = AppendPlaceDerivedColumns(projectedColumnsWithoutGeometryOrBbox, actualS3Type, columnNames);

            if (extent != null && !string.IsNullOrEmpty(extentPolygon))
            {
                // Clip geometries to extent - this keeps all intersecting features but clips them to the extent
                // Use ST_Intersection to clip, and only include features that actually intersect
                if (hasBboxColumn)
                {
                    return $@"
                        CREATE OR REPLACE TABLE current_table AS
                        WITH clipped AS (
                            SELECT
                                {projectedColumnsWithoutGeometryOrBbox},
                                CASE
                                    WHEN ST_Intersects(geometry, {extentPolygon})
                                    THEN ST_Intersection(geometry, {extentPolygon})
                                    ELSE NULL
                                END AS clipped_geometry
                            FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                            {spatialFilter}
                        )
                        SELECT
                            * EXCLUDE(clipped_geometry),
                            clipped_geometry AS geometry,
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
                        SELECT
                            {projectedColumnsWithoutGeometry},
                            CASE
                                WHEN ST_Intersects(geometry, {extentPolygon})
                                THEN ST_Intersection(geometry, {extentPolygon})
                                ELSE NULL
                            END as geometry
                        FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                        {spatialFilter}";
            }

            return $@"
                        CREATE OR REPLACE TABLE current_table AS
                        SELECT {projectedColumnList}
                        FROM read_parquet('{s3Path}', filename=true, hive_partitioning=1)
                        {spatialFilter}";
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
    }
}
