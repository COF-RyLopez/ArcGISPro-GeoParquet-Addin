using ArcGIS.Desktop.Framework.Contracts;
using System;

namespace DuckDBGeoparquet.Views
{
    internal class GersifyShowButton : Button
    {
        protected override void OnClick()
        {
            try
            {
                GersifyDockpaneViewModel.Show();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error opening GERSify Data: {ex.Message}");
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    "Unable to open GERSify Data. See ArcGIS Pro logs for details.",
                    "GERSify Data",
                    System.Windows.MessageBoxButton.OK,
                    System.Windows.MessageBoxImage.Error);
            }
        }

        protected override void OnUpdate()
        {
            Enabled = true;
        }
    }
}
