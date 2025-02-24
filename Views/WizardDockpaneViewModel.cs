using ArcGIS.Core.CIM;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using ArcGIS.Core.Geometry;
using System;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Collections.Generic;
using System.Data;
using DuckDBGeoparquet.Services;
using Microsoft.Win32;
using ArcGIS.Desktop.Catalog;
using System.Net.Http;
using System.Text.Json;

namespace DuckDBGeoparquet.Views
{
    internal class WizardDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_WizardDockpane";
        private readonly DataProcessor _dataProcessor;
        private const string RELEASE_URL = "https://labs.overturemaps.org/data/releases.json";
        private const string S3_BASE_PATH = "s3://overturemaps-us-west-2/release";

        private Dictionary<string, string> ThemeTypes = new Dictionary<string, string>
        {
            { "addresses", "address" },
            { "base", "land,water,land_use,land_cover,bathymetry,infrastructure" }, // base has multiple types
            { "buildings", "building,building_part" },
            { "divisions", "division,division_boundary,division_area" },
            { "places", "place" },
            { "transportation", "connector,segment" }
        };

        protected WizardDockpaneViewModel()
        {
            System.Diagnostics.Debug.WriteLine("Initializing WizardDockpaneViewModel");

            _dataProcessor = new DataProcessor();

            LoadDataCommand = new RelayCommand(
                async () => await LoadOvertureDataAsync(),
                () => !string.IsNullOrEmpty(SelectedTheme)
            );

            // Initialize properties
            Themes = new List<string>
            {
                "addresses",
                "base",
                "buildings",
                "divisions",
                "places",
                "transportation"
            };

            System.Diagnostics.Debug.WriteLine("Starting async initialization");
            _ = InitializeAsync();
        }

        private async Task InitializeAsync()
        {
            try
            {
                System.Diagnostics.Debug.WriteLine("Initializing DuckDB");
                await _dataProcessor.InitializeDuckDBAsync();

                System.Diagnostics.Debug.WriteLine("Fetching latest release");
                LatestRelease = await GetLatestRelease();

                System.Diagnostics.Debug.WriteLine($"Latest release set to: {LatestRelease}");
                NotifyPropertyChanged("LatestRelease");

                StatusText = "Ready to load Overture Maps data";
            }
            catch (Exception ex)
            {
                var error = $"Initialization error: {ex.Message}";
                System.Diagnostics.Debug.WriteLine($"Error during initialization: {ex}");
                StatusText = error;
            }
        }

        #region Properties
        private string _latestRelease;
        public string LatestRelease
        {
            get => _latestRelease;
            set => SetProperty(ref _latestRelease, value);
        }

        private List<string> _themes;
        public List<string> Themes
        {
            get => _themes;
            private set => SetProperty(ref _themes, value);
        }

        private string _selectedTheme;
        public string SelectedTheme
        {
            get => _selectedTheme;
            set
            {
                SetProperty(ref _selectedTheme, value);
                (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private string _statusText = "Initializing...";
        public string StatusText
        {
            get => _statusText;
            set => SetProperty(ref _statusText, value);
        }

        private double _progressValue;
        public double ProgressValue
        {
            get => _progressValue;
            set => SetProperty(ref _progressValue, value);
        }
        #endregion

        #region Commands
        public ICommand LoadDataCommand { get; private set; }
        #endregion

        private async Task<string> GetLatestRelease()
        {
            try
            {
                using (var client = new HttpClient())
                {
                    StatusText = "Fetching latest release info...";
                    var json = await client.GetStringAsync(RELEASE_URL);

                    // Log the raw JSON response
                    System.Diagnostics.Debug.WriteLine($"Release API Response: {json}");

                    var releaseInfo = JsonSerializer.Deserialize<ReleaseInfo>(json, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                    System.Diagnostics.Debug.WriteLine($"Deserialized Latest Release: {releaseInfo?.Latest}");

                    if (string.IsNullOrEmpty(releaseInfo?.Latest))
                    {
                        throw new Exception($"No release version found in response. Raw JSON: {json}");
                    }

                    StatusText = $"Found release: {releaseInfo.Latest}";
                    return releaseInfo.Latest;
                }
            }
            catch (HttpRequestException ex)
            {
                var error = $"Network error fetching release info: {ex.Message}";
                System.Diagnostics.Debug.WriteLine(error);
                StatusText = error;
                throw;
            }
            catch (JsonException ex)
            {
                var error = $"Error parsing release info: {ex.Message}";
                System.Diagnostics.Debug.WriteLine(error);
                StatusText = error;
                throw;
            }
            catch (Exception ex)
            {
                var error = $"Error fetching release info: {ex.Message}";
                System.Diagnostics.Debug.WriteLine(error);
                StatusText = error;
                throw;
            }
        }

        private async Task LoadOvertureDataAsync()
        {
            try
            {
                if (string.IsNullOrEmpty(LatestRelease))
                {
                    StatusText = "No release version available. Retrying...";
                    LatestRelease = await GetLatestRelease();
                }

                StatusText = $"Loading {SelectedTheme} data from Overture Maps release {LatestRelease}...";
                ProgressValue = 10;

                // Get the current active map view's extent
                Envelope extent = null;
                await QueuedTask.Run(() =>
                {
                    var mapView = MapView.Active;
                    if (mapView != null)
                    {
                        extent = mapView.Extent;
                        System.Diagnostics.Debug.WriteLine($"Map extent: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                    }
                    return true;
                });

                // Get the correct type for the selected theme
                string themeType = ThemeTypes[SelectedTheme];
                System.Diagnostics.Debug.WriteLine($"Theme type: {themeType}");

                // Handle multiple types if necessary
                var types = themeType.Split(',');
                foreach (var type in types)
                {
                    string s3Path = $"{S3_BASE_PATH}/{LatestRelease}/theme={SelectedTheme}/type={type.Trim()}/*.parquet";
                    StatusText = $"Attempting to load from: {s3Path}";
                    System.Diagnostics.Debug.WriteLine($"Loading from path: {s3Path}");

                    await _dataProcessor.IngestFileAsync(s3Path, extent);

                    // Create a feature layer for the loaded data
                    string layerName = $"{SelectedTheme} - {type.Trim()}";
                    var progress = new Progress<string>(status =>
                    {
                        StatusText = status;
                        if (status.StartsWith("Processing: "))
                        {
                            var percent = status.Split('%')[0].Split(':')[1].Trim();
                            if (double.TryParse(percent, out double value))
                            {
                                ProgressValue = value;
                            }
                        }
                    });

                    await _dataProcessor.CreateFeatureLayerAsync(layerName, LatestRelease,progress);

                    StatusText = $"Successfully loaded {SelectedTheme}/{type} data from release {LatestRelease}";
                    ProgressValue += (100.0 / types.Length);
                }

                StatusText = $"Successfully loaded all {SelectedTheme} data from release {LatestRelease}";
                ProgressValue = 100;
            }
            catch (Exception ex)
            {
                StatusText = $"Error loading data: {ex.Message}";
                ProgressValue = 0;
                System.Diagnostics.Debug.WriteLine($"Load error: {ex}");
            }
        }

        private class ReleaseInfo
        {
            public string Latest { get; set; }
            public List<string> Releases { get; set; }
        }

        /// <summary>
        /// Show the DockPane.
        /// </summary>
        internal static void Show()
        {
            var pane = FrameworkApplication.DockPaneManager.Find(_dockPaneID);
            if (pane == null)
                return;

            pane.Activate();
        }
    }
}

