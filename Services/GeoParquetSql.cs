using DuckDBGeoparquet.Models;
using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Pure SQL-fragment builders for the DuckDB GeoParquet pipeline.
    /// No ArcGIS or DuckDB dependencies — everything here is deterministic
    /// string construction, which makes it unit-testable without ArcGIS Pro
    /// running (see DuckDBGeoparquet.Tests).
    ///
    /// All coordinate formatting is culture-invariant: a German-locale
    /// machine writing "36,76" instead of "36.76" into SQL was a real bug.
    /// </summary>
    public static class GeoParquetSql
    {
        public const string InternalGeometryTypeColumn = "__overture_geom_type";

        /// <summary>Row group size for GeoParquet COPY output. 100k rows is a
        /// good balance of compression, read performance, and memory.</summary>
        public const int RowGroupSize = 100000;

        /// <summary>
        /// DuckDB COPY option that selects GeoParquet metadata + storage.
        /// Documented for DuckDB 1.5: NONE | V1 | V2 | BOTH (default V1).
        /// V2 writes native Parquet GEOMETRY/GEOGRAPHY logical types and
        /// geo metadata version 2.0.0.
        /// </summary>
        public const string GeoParquetVersionCopyOption = "GEOPARQUET_VERSION";

        /// <summary>DuckDB COPY value for GeoParquet 1.x (WKB BYTE_ARRAY + geo metadata).</summary>
        public const string GeoParquetVersion1x = "V1";

        /// <summary>DuckDB COPY value for GeoParquet 2.0 (native Parquet GEOMETRY types).</summary>
        public const string GeoParquetVersion2 = "V2";

        /// <summary>
        /// DuckDB 1.4.1 added GEOPARQUET_VERSION including V2 (PR 19246).
        /// The add-in ships DuckDB.NET 1.5.3, which is well above this floor.
        /// </summary>
        public static readonly Version MinimumDuckDbVersionForGeoParquetV2 = new Version(1, 4, 1);

        /// <summary>
        /// Normalizes a user-supplied compression name to one DuckDB accepts,
        /// defaulting to ZSTD for anything unrecognized.
        /// </summary>
        public static string ValidateCompression(string compression) =>
            compression?.ToUpperInvariant() switch
            {
                "SNAPPY" => "SNAPPY",
                "GZIP" => "GZIP",
                "ZSTD" => "ZSTD",
                _ => "ZSTD"
            };

        /// <summary>
        /// Maps UI / caller values onto DuckDB COPY GEOPARQUET_VERSION tokens.
        /// Unrecognized or empty input is GeoParquet 1.x (the default).
        /// </summary>
        public static string NormalizeGeoParquetVersion(string version)
        {
            if (string.IsNullOrWhiteSpace(version))
                return GeoParquetVersion1x;

            string compact = version.Trim().Replace(" ", string.Empty).ToUpperInvariant();
            if (compact is "V2" or "2" or "2.0" or "2.0.0")
                return GeoParquetVersion2;

            return GeoParquetVersion1x;
        }

        /// <summary>
        /// Parses DuckDB <c>version()</c> strings such as "v1.5.3" or "v1.5.3-dev123".
        /// </summary>
        public static bool TryParseDuckDbVersion(string versionText, out Version version)
        {
            version = null;
            if (string.IsNullOrWhiteSpace(versionText))
                return false;

            var match = Regex.Match(versionText.Trim(), @"v?(\d+)\.(\d+)\.(\d+)", RegexOptions.IgnoreCase);
            if (!match.Success)
                return false;

            version = new Version(
                int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture),
                int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture),
                int.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture));
            return true;
        }

        /// <summary>
        /// True when this DuckDB build can write GeoParquet 2.0 via
        /// <c>GEOPARQUET_VERSION 'V2'</c> (native Parquet GEOMETRY types).
        /// </summary>
        public static bool SupportsGeoParquetV2(Version duckDbVersion) =>
            duckDbVersion != null && duckDbVersion >= MinimumDuckDbVersionForGeoParquetV2;

        public static bool SupportsGeoParquetV2(string duckDbVersionText) =>
            TryParseDuckDbVersion(duckDbVersionText, out var parsed) && SupportsGeoParquetV2(parsed);

        /// <summary>
        /// Throws if the caller asked for GeoParquet 2.0 on a DuckDB that cannot
        /// write it. Does not silently fall back to 1.x.
        /// </summary>
        public static void EnsureGeoParquetVersionWritable(string requestedVersion, Version duckDbVersion, string duckDbVersionText = null)
        {
            if (NormalizeGeoParquetVersion(requestedVersion) != GeoParquetVersion2)
                return;

            if (SupportsGeoParquetV2(duckDbVersion))
                return;

            string running = !string.IsNullOrWhiteSpace(duckDbVersionText)
                ? duckDbVersionText
                : duckDbVersion?.ToString() ?? "unknown";

            throw new InvalidOperationException(
                "GeoParquet 2.0 export requires DuckDB 1.4.1 or later, which added the COPY option " +
                $"{GeoParquetVersionCopyOption} '{GeoParquetVersion2}' (native Parquet GEOMETRY logical types). " +
                $"This session is running DuckDB {running}.");
        }

        /// <summary>
        /// Builds the COPY … TO … (FORMAT 'PARQUET') statement that exports a
        /// query result as GeoParquet. GeoParquet 1.x is the default and omits
        /// GEOPARQUET_VERSION so DuckDB keeps its V1 writer path. Version 2.0
        /// adds GEOPARQUET_VERSION 'V2' (native Parquet GEOMETRY types).
        /// </summary>
        public static string BuildCopyCommand(string selectQuery, string outputPath, string compression = "ZSTD", string geoParquetVersion = null)
        {
            string versionOption = string.Empty;
            if (NormalizeGeoParquetVersion(geoParquetVersion) == GeoParquetVersion2)
            {
                versionOption = $@",
                    {GeoParquetVersionCopyOption} '{GeoParquetVersion2}'";
            }

            return $@"
                COPY (
                    {selectQuery}
                ) TO '{EscapeSqlLiteral(outputPath.Replace('\\', '/'))}'
                WITH (
                    FORMAT 'PARQUET',
                    GEOPARQUET_VERSION 'V1',
                    ROW_GROUP_SIZE {RowGroupSize},
                    COMPRESSION '{ValidateCompression(compression)}'{versionOption}
                );";
        }

        /// <summary>Formats a coordinate for SQL with invariant culture.</summary>
        public static string FormatCoordinate(double value) =>
            value.ToString("G", CultureInfo.InvariantCulture);

        /// <summary>Escapes a value for embedding in a single-quoted SQL literal.</summary>
        public static string EscapeSqlLiteral(string value) =>
            string.IsNullOrEmpty(value) ? string.Empty : value.Replace("'", "''");

        /// <summary>
        /// Builds the ST_GeomFromText expression for an extent's polygon,
        /// used for ST_Intersects checks and geometry clipping.
        /// </summary>
        public static string BuildExtentPolygon(ExtentBounds extent)
        {
            string xMin = FormatCoordinate(extent.XMin);
            string yMin = FormatCoordinate(extent.YMin);
            string xMax = FormatCoordinate(extent.XMax);
            string yMax = FormatCoordinate(extent.YMax);
            return $"ST_GeomFromText('POLYGON(({xMin} {yMin}, {xMax} {yMin}, {xMax} {yMax}, {xMin} {yMax}, {xMin} {yMin}))')";
        }

        /// <summary>
        /// Builds the bbox-column overlap predicate (no WHERE keyword). This
        /// is the fast pushdown filter: DuckDB can skip row groups whose bbox
        /// statistics don't overlap the extent, so it runs before any
        /// ST_Intersects refinement.
        /// </summary>
        public static string BuildBboxOverlapPredicate(ExtentBounds extent)
        {
            return $"bbox.xmin <= {FormatCoordinate(extent.XMax)}" +
                   $" AND bbox.xmax >= {FormatCoordinate(extent.XMin)}" +
                   $" AND bbox.ymin <= {FormatCoordinate(extent.YMax)}" +
                   $" AND bbox.ymax >= {FormatCoordinate(extent.YMin)}";
        }

        public static string BuildGeometryExportSelect(string geometryType, bool currentTableHasBbox, bool hasCachedGeometryType)
        {
            string escapedGeomType = EscapeSqlLiteral(geometryType);
            string geometryTypeFilter = hasCachedGeometryType
                ? $"{InternalGeometryTypeColumn} = '{escapedGeomType}'"
                : $"ST_GeometryType(geometry) = '{escapedGeomType}'";
            string excludedColumns = currentTableHasBbox
                ? hasCachedGeometryType ? $"geometry, bbox, {InternalGeometryTypeColumn}" : "geometry, bbox"
                : hasCachedGeometryType ? $"geometry, {InternalGeometryTypeColumn}" : "geometry";

            if (currentTableHasBbox)
            {
                return $@"
                            SELECT
                                * EXCLUDE({excludedColumns}),
                                geometry,
                                struct_pack(
                                    xmin := ST_XMin(geometry),
                                    ymin := ST_YMin(geometry),
                                    xmax := ST_XMax(geometry),
                                    ymax := ST_YMax(geometry)
                                ) AS bbox
                            FROM current_table
                            WHERE geometry IS NOT NULL
                              AND {geometryTypeFilter}";
            }

            return $@"
                            SELECT
                                * EXCLUDE({excludedColumns}),
                                geometry
                            FROM current_table
                            WHERE geometry IS NOT NULL
                              AND {geometryTypeFilter}";
        }
    }
}
