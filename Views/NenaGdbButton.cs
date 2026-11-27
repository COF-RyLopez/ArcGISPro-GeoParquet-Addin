using ArcGIS.Desktop.Framework.Contracts;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using MessageBox = ArcGIS.Desktop.Framework.Dialogs.MessageBox;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Builds a NENA NG9-1-1 file geodatabase from a statewide NENA GeoPackage and its export report, using
    /// NENA's v3.0 template bundled with the add-in (no download).
    /// </summary>
    internal class NenaGdbButton : Button
    {
        private const string Title = "NENA File Geodatabase";
        private bool _running;

        protected override async void OnClick()
        {
            var dialog = new Microsoft.Win32.OpenFileDialog
            {
                Title = "Select a statewide NENA GeoPackage",
                Filter = "NENA GeoPackage (*.gpkg)|*.gpkg"
            };
            if (dialog.ShowDialog() != true)
                return;

            string gpkg = dialog.FileName;
            string gdb = NenaGdbBuilder.DefaultGeodatabasePath(gpkg);
            bool overwrite = false;
            if (Directory.Exists(gdb))
            {
                var answer = MessageBox.Show($"{gdb} already exists. Replace it?", Title,
                    MessageBoxButton.YesNo, MessageBoxImage.Question);
                if (answer != MessageBoxResult.Yes)
                    return;
                overwrite = true;
            }

            _running = true;
            try
            {
                string addInFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? ".";
                var builder = new NenaGdbBuilder(new ArcGisGeoprocessingRunner());
                var result = await builder.BuildAsync(gpkg, gdb, NenaGdbBuilder.BundledTemplatePath(addInFolder), overwrite);
                string log = Path.ChangeExtension(gdb, ".load.txt");
                await File.WriteAllLinesAsync(log, result.Messages);
                string last = result.Messages.LastOrDefault() ?? string.Empty;
                MessageBox.Show($"{last}\n\nDetails: {log}", Title,
                    MessageBoxButton.OK, result.Succeeded ? MessageBoxImage.Information : MessageBoxImage.Error);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"NENA GDB build failed: {ex}");
                MessageBox.Show($"Building the NENA file geodatabase failed: {ex.Message}", Title,
                    MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                _running = false;
            }
        }

        protected override void OnUpdate()
        {
            Enabled = !_running;
        }
    }
}
