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
using System.Text;

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

        private Dictionary<string, string> ThemeDescriptions = new Dictionary<string, string>
        {
            { "addresses", "Address points including street names, house numbers, and postal codes." },
            { "base", "Base layers including land, water, land use, land cover, and infrastructure boundaries." },
            { "buildings", "Building footprints with height information where available." },
            { "divisions", "Administrative boundaries including countries, states, cities, and other divisions." },
            { "places", "Points of interest and places including businesses, landmarks, and amenities." },
            { "transportation", "Transportation networks including roads, rail, paths, and other ways." }
        };

        private Dictionary<string, int> ThemeFeatureEstimates = new Dictionary<string, int>
        {
            { "addresses", 500 },
            { "base", 300 },
            { "buildings", 800 },
            { "divisions", 100 },
            { "places", 250 },
            { "transportation", 750 }
        };

        protected WizardDockpaneViewModel()
        {
            System.Diagnostics.Debug.WriteLine("Initializing WizardDockpaneViewModel");

            _dataProcessor = new DataProcessor();

            LoadDataCommand = new RelayCommand(
                async () => await LoadOvertureDataAsync(),
                () => !string.IsNullOrEmpty(SelectedTheme)
            );

            ShowThemeInfoCommand = new RelayCommand(
                () => ShowThemeInfo(),
                () => !string.IsNullOrEmpty(SelectedTheme)
            );

            SetCustomExtentCommand = new RelayCommand(
                () => SetCustomExtent(),
                () => UseCustomExtent
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

            LogOutput = new StringBuilder();
            LogOutput.AppendLine("Initializing...");

            System.Diagnostics.Debug.WriteLine("Starting async initialization");
            _ = InitializeAsync();
        }

        private async Task InitializeAsync()
        {
            try
            {
                AddToLog("Initializing DuckDB");
                System.Diagnostics.Debug.WriteLine("Initializing DuckDB");
                await _dataProcessor.InitializeDuckDBAsync();

                AddToLog("Fetching latest release information");
                System.Diagnostics.Debug.WriteLine("Fetching latest release");
                LatestRelease = await GetLatestRelease();

                System.Diagnostics.Debug.WriteLine($"Latest release set to: {LatestRelease}");
                NotifyPropertyChanged("LatestRelease");

                StatusText = "Ready to load Overture Maps data";
                AddToLog("Ready to load Overture Maps data");
            }
            catch (Exception ex)
            {
                var error = $"Initialization error: {ex.Message}";
                System.Diagnostics.Debug.WriteLine($"Error during initialization: {ex}");
                StatusText = error;
                AddToLog($"ERROR: {error}");
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
                (ShowThemeInfoCommand as RelayCommand)?.RaiseCanExecuteChanged();
                UpdateThemePreview();
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

        private StringBuilder _logOutput;
        public StringBuilder LogOutput
        {
            get => _logOutput;
            set => SetProperty(ref _logOutput, value);
        }

        private string _logOutputText;
        public string LogOutputText
        {
            get => _logOutputText;
            set => SetProperty(ref _logOutputText, value);
        }

        private bool _useCurrentMapExtent = true;
        public bool UseCurrentMapExtent
        {
            get => _useCurrentMapExtent;
            set
            {
                SetProperty(ref _useCurrentMapExtent, value);
                UseCustomExtent = !value;
                (SetCustomExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private bool _useCustomExtent;
        public bool UseCustomExtent
        {
            get => _useCustomExtent;
            set
            {
                SetProperty(ref _useCustomExtent, value);
                UseCurrentMapExtent = !value;
                (SetCustomExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private Envelope _customExtent;
        public Envelope CustomExtent
        {
            get => _customExtent;
            set => SetProperty(ref _customExtent, value);
        }

        private string _themeDescription = "Select a theme to see description";
        public string ThemeDescription
        {
            get => _themeDescription;
            set => SetProperty(ref _themeDescription, value);
        }

        private string _estimatedFeatures = "--";
        public string EstimatedFeatures
        {
            get => _estimatedFeatures;
            set => SetProperty(ref _estimatedFeatures, value);
        }

        private string _estimatedSize = "--";
        public string EstimatedSize
        {
            get => _estimatedSize;
            set => SetProperty(ref _estimatedSize, value);
        }
        #endregion

        #region Commands
        public ICommand LoadDataCommand { get; private set; }
        public ICommand ShowThemeInfoCommand { get; private set; }
        public ICommand SetCustomExtentCommand { get; private set; }
        #endregion

        #region Helper Methods
        private void AddToLog(string message)
        {
            LogOutput.AppendLine($"[{DateTime.Now:HH:mm:ss}] {message}");
            LogOutputText = LogOutput.ToString();
            NotifyPropertyChanged("LogOutputText");
        }

        private void UpdateThemePreview()
        {
            if (string.IsNullOrEmpty(SelectedTheme))
            {
                ThemeDescription = "Select a theme to see description";
                EstimatedFeatures = "--";
                EstimatedSize = "--";
                return;
            }

            ThemeDescription = ThemeDescriptions.ContainsKey(SelectedTheme)
                ? ThemeDescriptions[SelectedTheme]
                : "No description available";

            if (ThemeFeatureEstimates.ContainsKey(SelectedTheme))
            {
                var estimate = ThemeFeatureEstimates[SelectedTheme];
                EstimatedFeatures = $"{estimate} per sq km (approx.)";
                // Rough estimate of size based on feature count
                double sizeInKb = estimate * 2.5; // Assuming each feature is about 2.5KB on average
                EstimatedSize = sizeInKb > 1024
                    ? $"{sizeInKb / 1024:F1} MB per sq km (approx.)"
                    : $"{sizeInKb:F0} KB per sq km (approx.)";
            }
            else
            {
                EstimatedFeatures = "--";
                EstimatedSize = "--";
            }
        }

        private void ShowThemeInfo()
        {
            if (string.IsNullOrEmpty(SelectedTheme)) return;

            string description = ThemeDescriptions.ContainsKey(SelectedTheme)
                ? ThemeDescriptions[SelectedTheme]
                : "No detailed information available.";

            string types = ThemeTypes.ContainsKey(SelectedTheme)
                ? $"Type(s): {ThemeTypes[SelectedTheme]}"
                : "";

            ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                $"{description}\n\n{types}",
                $"About {SelectedTheme} theme",
                System.Windows.MessageBoxButton.OK,
                System.Windows.MessageBoxImage.Information);
        }

        private void SetCustomExtent()
        {
            // This would typically use the current map to let the user draw an extent
            // For now, we'll just get the current map extent as a placeholder
            QueuedTask.Run(() =>
            {
                try
                {
                    var mapView = MapView.Active;
                    if (mapView != null && mapView.Extent != null)
                    {
                        CustomExtent = mapView.Extent;
                        AddToLog($"Custom extent set: {CustomExtent.XMin}, {CustomExtent.YMin}, {CustomExtent.XMax}, {CustomExtent.YMax}");
                    }
                    else
                    {
                        AddToLog("Unable to set custom extent: No active map view");
                    }
                }
                catch (Exception ex)
                {
                    AddToLog($"Error setting custom extent: {ex.Message}");
                }
            });
        }
        #endregion

        private async Task<string> GetLatestRelease()
        {
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    var response = await client.GetStringAsync(RELEASE_URL);
                    System.Diagnostics.Debug.WriteLine($"Release API Response: {response}");
                    AddToLog("Received release information from Overture Maps API");

                    var options = new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    };

                    var releaseInfo = JsonSerializer.Deserialize<ReleaseInfo>(response, options);

                    if (releaseInfo == null)
                    {
                        throw new Exception("Failed to deserialize release info");
                    }

                    System.Diagnostics.Debug.WriteLine($"Deserialized Latest Release: {releaseInfo.Latest}");
                    AddToLog($"Latest release available: {releaseInfo.Latest}");
                    return releaseInfo.Latest;
                }
            }
            catch (Exception ex)
            {
                AddToLog($"Failed to get latest release: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Error getting latest release: {ex}");
                throw;
            }
        }

        private async Task LoadOvertureDataAsync()
        {
            try
            {
                ProgressValue = 0;
                StatusText = $"Loading {SelectedTheme} data...";
                AddToLog($"Starting to load {SelectedTheme} data from release {LatestRelease}");

                // Get map extent
                Envelope extent = null;
                await QueuedTask.Run(() =>
                {
                    if (UseCurrentMapExtent && MapView.Active != null)
                    {
                        extent = MapView.Active.Extent;
                        AddToLog($"Map extent: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                        System.Diagnostics.Debug.WriteLine($"Map extent: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                    }
                    else if (UseCustomExtent && CustomExtent != null)
                    {
                        extent = CustomExtent;
                        AddToLog($"Using custom extent: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                    }
                });

                // Process each type within the selected theme
                if (ThemeTypes.ContainsKey(SelectedTheme))
                {
                    string[] types = ThemeTypes[SelectedTheme].Split(',');
                    ProgressValue = 0;
                    double increment = 100.0 / types.Length;

                    foreach (var type in types)
                    {
                        AddToLog($"Processing theme type: {type.Trim()}");
                        System.Diagnostics.Debug.WriteLine($"Theme type: {type.Trim()}");

                        // Ensure proper path construction with trimmed release
                        string trimmedRelease = LatestRelease?.Trim() ?? "";
                        string s3Path = trimmedRelease.Length > 0
                            ? $"{S3_BASE_PATH}/{trimmedRelease}/theme={SelectedTheme}/type={type.Trim()}/*.parquet"
                            : $"{S3_BASE_PATH}/theme={SelectedTheme}/type={type.Trim()}/*.parquet";

                        StatusText = $"Attempting to load from: {s3Path}";
                        AddToLog($"Loading from path: {s3Path}");
                        System.Diagnostics.Debug.WriteLine($"Loading from path: {s3Path}");

                        await _dataProcessor.IngestFileAsync(s3Path, extent);

                        // Create a feature layer for the loaded data
                        string layerName = $"{SelectedTheme} - {type.Trim()}";
                        var progress = new Progress<string>(status =>
                        {
                            StatusText = status;
                            AddToLog(status);
                            if (status.StartsWith("Processing: "))
                            {
                                var percent = status.Split('%')[0].Split(':')[1].Trim();
                                if (double.TryParse(percent, out double value))
                                {
                                    ProgressValue = value;
                                }
                            }
                        });

                        await _dataProcessor.CreateFeatureLayerAsync(layerName, LatestRelease, progress);

                        StatusText = $"Successfully loaded {SelectedTheme}/{type} data from release {LatestRelease}";
                        AddToLog($"Successfully loaded {SelectedTheme}/{type} data");
                        ProgressValue += increment;
                    }

                    StatusText = $"Successfully loaded all {SelectedTheme} data from release {LatestRelease}";
                    AddToLog($"All {SelectedTheme} data loaded successfully");
                    ProgressValue = 100;
                }
            }
            catch (Exception ex)
            {
                StatusText = $"Error loading data: {ex.Message}";
                AddToLog($"ERROR: {ex.Message}");
                AddToLog($"Stack trace: {ex.StackTrace}");
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

