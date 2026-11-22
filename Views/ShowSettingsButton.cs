using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;

namespace DuckDBGeoparquet.Views
{
    /// <summary>Opens the modal GeoParquet Settings property sheet.</summary>
    internal class ShowSettingsButton : Button
    {
        private const string SheetId = "DuckDBGeoparquet_SettingsSheet";
        private const string PageId = "DuckDBGeoparquet_SettingsPage";

        protected override void OnClick()
        {
            if (PropertySheet.IsVisible)
                return;
            PropertySheet.ShowDialog(SheetId, PageId, null);
        }
    }
}
