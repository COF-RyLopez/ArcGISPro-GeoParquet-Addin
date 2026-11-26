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
            // Typed empty array: a bare null is ambiguous between the
            // ShowDialog overloads (CS0121).
            PropertySheet.ShowDialog(SheetId, PageId, System.Array.Empty<object>());
        }
    }
}
