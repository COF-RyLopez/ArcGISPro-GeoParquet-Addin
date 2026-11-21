using ArcGIS.Desktop.Framework.Controls;
using DuckDBGeoparquet.Services;
using System;
using System.Windows;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Ribbon-launched dialog for saving any GeoParquet URL as a custom data
    /// source, without opening the Overture wizard first. Validates with the
    /// same DESCRIBE probe and writes the same user_sources.json store; if the
    /// wizard dockpane is open, its Custom Sources node refreshes in place.
    /// </summary>
    public partial class AddSourceWindow : ProWindow
    {
        public AddSourceWindow()
        {
            InitializeComponent();
        }

        private async void OnAddClick(object sender, RoutedEventArgs e)
        {
            string name = NameBox.Text?.Trim();
            string url = UrlBox.Text?.Trim();
            string storePath = UserSourceStore.GetStorePath(ProjectDataLocator.GetAddinDataBase());
            var sources = UserSourceStore.Load(storePath);

            string error = UserSourceStore.ValidateNewSource(name, url, sources);
            if (error != null)
            {
                StatusBlock.Text = error;
                return;
            }

            AddButton.IsEnabled = false;
            StatusBlock.Text = $"Validating {url} ...";
            try
            {
                // ponytail: throwaway DuckDB per probe; share the wizard's instance if probes ever feel slow
                using var processor = new DataProcessor();
                await processor.InitializeDuckDBAsync();
                var source = await processor.ProbeUserSourceAsync(name, url);

                sources.Add(source);
                UserSourceStore.Save(storePath, sources);
                WizardDockpaneViewModel.Instance?.RebuildCustomSourcesNode();

                StatusBlock.Text = $"Added '{name}' (geometry column: {source.GeometryColumn}).";
                NameBox.Text = string.Empty;
                UrlBox.Text = string.Empty;
                NameBox.Focus();
            }
            catch (Exception ex)
            {
                StatusBlock.Text = $"Could not add source: {ex.Message}";
            }
            finally
            {
                AddButton.IsEnabled = true;
            }
        }

        private void OnCloseClick(object sender, RoutedEventArgs e) => Close();
    }
}
