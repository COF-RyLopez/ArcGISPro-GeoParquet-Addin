namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// A user-saved GeoParquet source: any http(s)/s3 URL or glob pointing at
    /// GeoParquet data. Geometry details are detected once, when the source is
    /// added (see DataProcessor.ProbeUserSourceAsync), so downloads don't
    /// re-discover them.
    /// </summary>
    public sealed class UserDataSource
    {
        public string Name { get; set; }

        public string Url { get; set; }

        /// <summary>Name of the geometry column in the source file(s).</summary>
        public string GeometryColumn { get; set; } = "geometry";

        /// <summary>
        /// True when the geometry column holds WKB bytes (BLOB) instead of a
        /// GEOMETRY-typed column, and needs ST_GeomFromWKB on read.
        /// </summary>
        public bool GeometryIsWkb { get; set; }
    }
}
