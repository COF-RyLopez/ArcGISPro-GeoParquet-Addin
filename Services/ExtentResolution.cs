namespace DuckDBGeoparquet.Services
{
    /// <summary>Which configured source a data load takes its extent from.</summary>
    public enum ExtentSource
    {
        None,
        Map,
        Custom
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
        /// Mirrors the ViewModel's precedence: the current map extent wins when
        /// "use current map extent" is on and a map is active; otherwise the
        /// custom extent is used when one is set; otherwise there is no extent.
        /// </summary>
        public static ExtentSource ChooseSource(bool useCurrentMapExtent, bool hasActiveMap, bool useCustomExtent, bool hasCustomExtent)
        {
            if (useCurrentMapExtent && hasActiveMap) return ExtentSource.Map;
            if (useCustomExtent && hasCustomExtent) return ExtentSource.Custom;
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
