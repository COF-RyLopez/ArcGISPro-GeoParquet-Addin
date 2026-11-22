namespace DuckDBGeoparquet.Services
{
    /// <summary>Which configured source a data load takes its extent from.</summary>
    public enum ExtentSource
    {
        None,
        Map,
        Custom,
        Typed,
        Layer,
        Polygon
    }

    /// <summary>
    /// Pure extent-resolution helpers extracted from WizardDockpaneViewModel
    /// (Phase 2c stage 3). No ArcGIS dependencies: the ViewModel still reads
    /// the live map / custom <c>Envelope</c> and does the actual projection,
    /// but the source-selection precedence and the "does this need projecting
    /// to WGS84?" decision live here so they are unit-testable.
    /// </summary>
    public static class ExtentResolution
    {
        /// <summary>WKID of WGS84 (EPSG:4326) — the SR the load pipeline filters in.</summary>
        public const int Wgs84Wkid = 4326;

        /// <summary>
        /// Mirrors the ViewModel's precedence: each AOI option counts only when
        /// its radio is on and its prerequisite exists (an active map, a drawn
        /// rectangle, a parseable typed bbox, a chosen layer, a drawn polygon).
        /// The radio buttons are mutually exclusive in the UI, so at most one
        /// use* flag is true; the ordering here is just a stable tie-break.
        /// </summary>
        public static ExtentSource ChooseSource(
            bool useCurrentMapExtent, bool hasActiveMap,
            bool useCustomExtent, bool hasCustomExtent,
            bool useTypedExtent = false, bool hasTypedExtent = false,
            bool useLayerExtent = false, bool hasLayerExtent = false,
            bool usePolygonExtent = false, bool hasPolygonExtent = false)
        {
            if (useCurrentMapExtent && hasActiveMap) return ExtentSource.Map;
            if (useCustomExtent && hasCustomExtent) return ExtentSource.Custom;
            if (useTypedExtent && hasTypedExtent) return ExtentSource.Typed;
            if (useLayerExtent && hasLayerExtent) return ExtentSource.Layer;
            if (usePolygonExtent && hasPolygonExtent) return ExtentSource.Polygon;
            return ExtentSource.None;
        }

        /// <summary>
        /// True when an extent in the given spatial reference must be projected
        /// to WGS84 before use. A null WKID (unknown SR) is treated as needing
        /// projection, matching the original guard.
        /// </summary>
        public static bool NeedsProjectionToWgs84(int? wkid) => wkid != Wgs84Wkid;
    }
}
