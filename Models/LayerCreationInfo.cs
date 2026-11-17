using System;

namespace DuckDBGeoparquet.Models
{
    public class LayerCreationInfo
    {
        public string FilePath { get; set; }
        public string LayerName { get; set; }
        public string GeometryType { get; set; }
        public int StackingPriority { get; set; }
        public string ParentTheme { get; set; }
        public string ActualType { get; set; }

        /// <summary>
        /// True when <see cref="FilePath"/> points at a file geodatabase
        /// feature class rather than a GeoParquet file. Map add and layer
        /// settings (including attribute domains) use this to distinguish
        /// Parquet-only limitations from FGDB output.
        /// </summary>
        public bool IsFileGeodatabase { get; set; }
    }
}
