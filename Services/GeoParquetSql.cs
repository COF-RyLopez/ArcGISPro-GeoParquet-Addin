using DuckDBGeoparquet.Models;
using System.Globalization;

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
        /// <summary>Row group size for GeoParquet COPY output. 100k rows is a
        /// good balance of compression, read performance, and memory.</summary>
        public const int RowGroupSize = 100000;

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
        /// Builds the COPY … TO … (FORMAT 'PARQUET') statement that exports a
        /// query result as GeoParquet.
        /// </summary>
        public static string BuildCopyCommand(string selectQuery, string outputPath, string compression = "ZSTD")
        {
            return $@"
                COPY (
                    {selectQuery}
                ) TO '{outputPath.Replace('\\', '/')}'
                WITH (
                    FORMAT 'PARQUET',
                    ROW_GROUP_SIZE {RowGroupSize},
                    COMPRESSION '{ValidateCompression(compression)}'
                );";
        }

        /// <summary>Formats a coordinate for SQL with invariant culture.</summary>
        public static string FormatCoordinate(double value) =>
            value.ToString("G", CultureInfo.InvariantCulture);

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
    }
}
