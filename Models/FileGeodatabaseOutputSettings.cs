namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Optional post-export setting: keep writing GeoParquet, then copy those
    /// files into a file geodatabase with ArcGIS Pro's Export Features tool.
    /// GeoParquet remains the default when <see cref="Enabled"/> is false.
    /// </summary>
    public sealed class FileGeodatabaseOutputSettings
    {
        /// <summary>
        /// When false (the default), the add-in stops at GeoParquet and adds
        /// those layers to the map.
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// User-chosen .gdb path. When empty, a geodatabase named
        /// <c>Overture.gdb</c> is created under the data output folder.
        /// A non-.gdb folder is treated as the parent of that default gdb.
        /// </summary>
        public string GeodatabasePath { get; set; }

        /// <summary>
        /// When true, Export Features is run with overwriteoutput enabled so
        /// existing feature class names are replaced. When false, colliding
        /// names are suffixed instead — matching the parquet "replace or skip"
        /// choice.
        /// </summary>
        public bool OverwriteExisting { get; set; } = true;

        public static FileGeodatabaseOutputSettings GeoParquetDefault { get; } = new();
    }
}
