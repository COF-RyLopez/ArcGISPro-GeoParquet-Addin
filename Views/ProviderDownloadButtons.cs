using ArcGIS.Desktop.Framework.Contracts;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Per-provider ribbon buttons in the Data Sources group. Each opens the
    /// GeoParquet Data Loader and expands that provider's group in the theme
    /// tree. Overture themes are the top-level nodes, so its button just shows
    /// the pane (and restores the Overture release header).
    /// </summary>
    internal abstract class ProviderDownloadButton : Button
    {
        protected abstract string ThemeKey { get; }
        protected override void OnClick() => WizardDockpaneViewModel.ShowFocused(ThemeKey);
    }

    internal class DownloadOvertureButton : ProviderDownloadButton
    {
        protected override string ThemeKey => null; // top-level themes; nothing to expand
    }

    internal class DownloadOsmButton : ProviderDownloadButton
    {
        protected override string ThemeKey => Services.PresetCatalog.OsmThemeKey;
    }

    internal class DownloadSourceCoopButton : ProviderDownloadButton
    {
        protected override string ThemeKey => Services.PresetCatalog.SourceCoopThemeKey;
    }

    internal class DownloadCustomSourcesButton : ProviderDownloadButton
    {
        protected override string ThemeKey => WizardDockpaneViewModel.CustomSourcesThemeKey;
    }
}
