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
            string version = NormalizeGeoParquetVersion(geoParquetVersion) == GeoParquetVersion2
                ? GeoParquetVersion2
                : "V1";

            return $@"
                COPY (
                    {selectQuery}
                ) TO '{EscapeSqlLiteral(outputPath.Replace('\\', '/'))}'
                WITH (
                    FORMAT 'PARQUET',
                    {GeoParquetVersionCopyOption} '{version}',
                    ROW_GROUP_SIZE {RowGroupSize},
                    COMPRESSION '{ValidateCompression(compression)}'
                );";
        }

        /// <summary>Formats a coordinate for SQL with invariant culture.</summary>
        public static string FormatCoordinate(double value) =>
            value.ToString("G", CultureInfo.InvariantCulture);

        public const int MaxAttributeFilterLength = 2000;

        // Statement/DDL keywords and file-, network- or environment-reading
        // functions a WHERE fragment must never contain. SELECT alone already
        // blocks every subquery and table function. Checked word-by-word on
        // text with string literals and quoted identifiers stripped.
        private static readonly string[] AttributeFilterDeniedWords =
        {
            "select", "insert", "update", "delete", "drop", "create", "alter",
            "attach", "detach", "copy", "install", "load", "call", "pragma",
            "set", "reset", "export", "import", "union", "intersect", "except",
            "grant", "execute", "prepare", "vacuum", "checkpoint", "begin",
            "commit", "rollback", "transaction", "use", "summarize", "describe", "show", "with",
            "read_parquet", "parquet_scan", "read_csv", "read_csv_auto",
            "read_json", "read_json_auto", "read_json_objects", "read_text",
            "read_blob", "read_xlsx", "st_read", "glob", "getenv", "sniff_csv",
            "parquet_metadata", "parquet_schema", "parquet_kv_metadata",
            "parquet_file_metadata", "database_size", "current_setting"
        };

        /// <summary>Trims a user-typed attribute filter and strips one leading
        /// WHERE keyword. Returns null when there is no filter.</summary>
        public static string NormalizeAttributeFilter(string filter)
        {
            filter = filter?.Trim();
            if (string.IsNullOrEmpty(filter)) return null;
            if (filter.Length > 6 && filter.StartsWith("where", StringComparison.OrdinalIgnoreCase) &&
                char.IsWhiteSpace(filter[5]))
                filter = filter.Substring(6).Trim();
            return filter.Length == 0 ? null : filter;
        }

        /// <summary>
        /// Validates a user-supplied WHERE-clause fragment before it is embedded
        /// in a download query. This is a trust boundary: the fragment is typed
        /// free-form, so refuse statement chaining, comments, subqueries, and
        /// DuckDB functions that read files, the network, or the environment.
        /// Returns a user-facing error message, or null when the filter is
        /// acceptable (empty filters are acceptable: they mean "no filter").
        /// </summary>
        public static string ValidateAttributeFilter(string filter)
        {
            filter = NormalizeAttributeFilter(filter);
            if (filter == null) return null;

            if (filter.Length > MaxAttributeFilterLength)
                return $"Attribute filter is too long (over {MaxAttributeFilterLength} characters).";

            // Strip single-quoted string literals ('' escapes a quote) and
            // double-quoted identifiers so their contents aren't scanned.
            var stripped = new System.Text.StringBuilder(filter.Length);
            for (int i = 0; i < filter.Length; i++)
            {
                char c = filter[i];
                if (c == '\'' || c == '"')
                {
                    char quote = c;
                    int j = i + 1;
                    while (j < filter.Length)
                    {
                        if (filter[j] == quote)
                        {
                            if (j + 1 < filter.Length && filter[j + 1] == quote) { j += 2; continue; }
                            break;
                        }
                        j++;
                    }
                    if (j >= filter.Length)
                        return quote == '\''
                            ? "Attribute filter has an unclosed string quote (')."
                            : "Attribute filter has an unclosed identifier quote (\").";
                    stripped.Append(quote).Append(quote);
                    i = j;
                    continue;
                }
                stripped.Append(c);
            }

            string text = stripped.ToString();
            if (text.Contains(';')) return "Attribute filter can't contain ';'.";
            if (text.Contains("--") || text.Contains("/*") || text.Contains("*/"))
                return "Attribute filter can't contain SQL comments.";
            if (text.Contains('$'))
                return "Attribute filter can't contain '$'.";

            foreach (System.Text.RegularExpressions.Match m in
                System.Text.RegularExpressions.Regex.Matches(text, @"[A-Za-z_][A-Za-z0-9_]*"))
            {
                string word = m.Value.ToLowerInvariant();
                if (System.Array.IndexOf(AttributeFilterDeniedWords, word) >= 0)
                    return $"Attribute filter can't use '{m.Value}'. Use a plain condition like subtype = 'road'.";
            }

            return null;
        }

        /// <summary>
        /// Wraps a validated attribute filter in parentheses for embedding in a
        /// WHERE clause. Throws when the filter fails validation — callers show
        /// the validation message to the user before queries are built, so this
        /// throw is the backstop, not the UX.
        /// </summary>
        public static string BuildAttributeFilterPredicate(string filter)
        {
            string error = ValidateAttributeFilter(filter);
            if (error != null) throw new ArgumentException(error, nameof(filter));
            filter = NormalizeAttributeFilter(filter);
            return filter == null ? null : $"({filter})";
        }

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
        /// Builds a WKT POLYGON from a drawn AOI's outer ring, closing the ring
        /// when needed. Coordinates come from the add-in's own geometry (never
        /// user text), are required to be finite, and are formatted with
        /// invariant culture — so the resulting WKT is safe to embed in SQL.
        /// Throws on fewer than 3 distinct vertices or non-finite coordinates.
        /// </summary>
        public static string BuildPolygonWkt(System.Collections.Generic.IReadOnlyList<(double X, double Y)> ring)
        {
            if (ring == null || ring.Count < 3)
                throw new ArgumentException("A polygon area needs at least 3 vertices.", nameof(ring));
            foreach (var (x, y) in ring)
                if (!double.IsFinite(x) || !double.IsFinite(y))
                    throw new ArgumentException("Polygon area has an invalid coordinate.", nameof(ring));

            var points = new System.Collections.Generic.List<(double X, double Y)>(ring);
            if (points[0] != points[^1]) points.Add(points[0]);
            if (points.Count < 4)
                throw new ArgumentException("A polygon area needs at least 3 distinct vertices.", nameof(ring));

            var sb = new System.Text.StringBuilder("POLYGON((");
            for (int i = 0; i < points.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append(FormatCoordinate(points[i].X)).Append(' ').Append(FormatCoordinate(points[i].Y));
            }
            return sb.Append("))").ToString();
        }

        /// <summary>
        /// Backstop check before an AOI WKT is embedded in SQL. Both producers
        /// are the add-in's own (BuildPolygonWkt, or ArcGIS's WKT exporter for
        /// selected features), so this only has to confirm the expected shape:
        /// a 2D POLYGON/MULTIPOLYGON keyword with a numeric body.
        /// </summary>
        public static bool IsValidAoiWkt(string wkt) =>
            !string.IsNullOrWhiteSpace(wkt) &&
            System.Text.RegularExpressions.Regex.IsMatch(
                wkt, @"^\s*(MULTIPOLYGON|POLYGON)\s*\(\s*[-+0-9eE.,()\s]+\)\s*$");

        /// <summary>
        /// Wraps an internally built AOI polygon WKT (see BuildPolygonWkt) as a
        /// DuckDB geometry expression for ST_Intersects / ST_Intersection.
        /// </summary>
        public static string BuildAoiPolygonExpression(string wkt) =>
            $"ST_GeomFromText('{EscapeSqlLiteral(wkt)}')";

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
