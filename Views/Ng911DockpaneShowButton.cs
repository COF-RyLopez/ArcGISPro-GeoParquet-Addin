using ArcGIS.Desktop.Framework.Contracts;

namespace DuckDBGeoparquet.Views
{
    internal class Ng911DockpaneShowButton : Button
    {
        protected override void OnClick()
        {
            Ng911DockpaneViewModel.Show();
        }

        protected override void OnUpdate()
        {
            Enabled = true;
        }
    }
}


