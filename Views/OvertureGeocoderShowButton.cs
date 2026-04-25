using ArcGIS.Desktop.Framework.Contracts;
using System;

namespace DuckDBGeoparquet.Views
{
    internal class OvertureGeocoderShowButton : Button
    {
        protected override void OnClick()
        {
            try
            {
                OvertureGeocoderDockpaneViewModel.Show();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error opening Overture geocoder: {ex.Message}");
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    "Unable to open Overture ROI Geocoder. See ArcGIS Pro logs for details.",
                    "Overture ROI Geocoder",
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
