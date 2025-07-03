using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;

namespace DuckDBGeoparquet.Views
{
    internal class SourceCoopDockpaneShowButton : Button
    {
        protected override void OnClick()
        {
            SourceCoopDockpaneViewModel.Show();
        }
    }
} 