using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using System;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Ribbon button that opens the Add GeoParquet Data Source dialog.
    /// </summary>
    internal class AddSourceButton : Button
    {
        protected override void OnClick()
        {
            try
            {
                var window = new AddSourceWindow
                {
                    Owner = FrameworkApplication.Current.MainWindow
                };
                window.ShowDialog();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error opening Add Data Source: {ex.Message}");
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    "Unable to open Add Data Source. Please check ArcGIS Pro logs for details.",
                    "Error Opening Add Data Source",
                    System.Windows.MessageBoxButton.OK,
                    System.Windows.MessageBoxImage.Error);
            }
        }
    }
}
