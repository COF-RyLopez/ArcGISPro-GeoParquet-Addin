using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using System;

namespace DuckDBGeoparquet.Views
{
    internal class FeatureServiceShowButton : Button
    {
        protected override void OnClick()
        {
            try
            {
                WizardDockpaneViewModel.ShowFeatureServiceTab();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error opening Feature Service tab: {ex.Message}");
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    "Unable to open the Feature Service controls. Please check ArcGIS Pro logs for details.",
                    "Error Opening Feature Service",
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


