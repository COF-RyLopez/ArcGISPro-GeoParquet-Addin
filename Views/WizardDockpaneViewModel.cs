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
using System.IO;
using System.Windows.Forms;

namespace DuckDBGeoparquet.Views
{
    internal class WizardDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_WizardDockpane";
        private readonly DataProcessor _dataProcessor;
        private const string RELEASE_URL = "https://labs.overturemaps.org/data/releases.json";
        private const string S3_BASE_PATH = "s3://overturemaps-us-west-2/release";

        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        private readonly Dictionary<string, string> ThemeTypes = new()
        {
            { "addresses", "address" },
            { "base", "land,water,land_use,land_cover,bathymetry,infrastructure" }, // base has multiple types
            { "buildings", "building,building_part" },
            { "divisions", "division,division_boundary,division_area" },
            { "places", "place" },
            { "transportation", "connector,segment" }
        };

        private readonly Dictionary<string, string> ThemeIcons = new()
        {
            { "addresses", "GeocodeAddressesIcon" },
            { "base", "GlobeIcon" },
            { "buildings", "BuildingLayerIcon" },
            { "divisions", "BoundaryIcon" },
            { "places", "PointOfInterestIcon" },
            { "transportation", "TransportationNetworkIcon" }
        };

        private readonly Dictionary<string, string> ThemeDescriptions = new()
        {
            { "addresses", "Address points including street names, house numbers, and postal codes." },
            { "base", "Base layers including land, water, land use, land cover, and infrastructure boundaries." },
            { "buildings", "Building footprints with height information where available." },
            { "divisions", "Administrative boundaries including countries, states, cities, and other divisions." },
            { "places", "Points of interest and places including businesses, landmarks, and amenities." },
            { "transportation", "Transportation networks including roads, rail, paths, and other ways." }
        };

        private readonly Dictionary<string, int> ThemeFeatureEstimates = new()
        {
            { "addresses", 500 },
            { "base", 300 },
            { "buildings", 800 },
            { "divisions", 100 },
            { "places", 250 },
            { "transportation", 750 }
        };

        private CustomExtentTool _customExtentTool;

        protected WizardDockpaneViewModel()
        {
            System.Diagnostics.Debug.WriteLine("Initializing WizardDockpaneViewModel");

            _dataProcessor = new();

            LoadDataCommand = new RelayCommand(
                async () => await LoadOvertureDataAsync(),
                () => SelectedThemes.Count > 0
            );

            ShowThemeInfoCommand = new RelayCommand(
                () => ShowThemeInfo(),
                () => !string.IsNullOrEmpty(SelectedTheme)
            );

            SetCustomExtentCommand = new RelayCommand(
                () => SetCustomExtent(),
                () => UseCustomExtent
            );

            BrowseMfcLocationCommand = new RelayCommand(
                () => BrowseMfcLocation()
            );

            // Subscribe to static events
            CustomExtentTool.ExtentCreatedStatic += OnExtentCreated;

            // Register for DockPane closing
            try
            {
                System.Diagnostics.Debug.WriteLine("Registering dockpane event handlers for cleanup");
                // The ArcGIS Pro framework will handle the lifecycle and cleanup
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error setting up event handlers: {ex.Message}");
            }

            // Initialize properties
            Themes = new()
            {
                "addresses",
                "base",
                "buildings",
                "divisions",
                "places",
                "transportation"
            };

            LogOutput = new();
            LogOutput.AppendLine("Initializing...");

            System.Diagnostics.Debug.WriteLine("Starting async initialization");
            _ = InitializeAsync();

            // Set default MFC output path to just the Connections folder
            MfcOutputPath = Path.Combine(
                Services.MfcUtility.DefaultMfcBasePath,
                "Connections"
            );
        }

        private async new Task InitializeAsync()
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
            finally
            {
                // Set loading state to false when initialization is complete
                IsLoading = false;
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

        private List<string> _selectedThemes = new();
        public List<string> SelectedThemes
        {
            get => _selectedThemes;
            set => SetProperty(ref _selectedThemes, value);
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

                if (value != null && ThemeIcons.TryGetValue(value, out string iconText))
                {
                    ThemeIconText = iconText;
                }
                else
                {
                    ThemeIconText = "GlobeIcon"; // Default icon (globe)
                }

                UpdateThemePreview();
            }
        }

        private int _selectedTabIndex = 0;
        public int SelectedTabIndex
        {
            get => _selectedTabIndex;
            set => SetProperty(ref _selectedTabIndex, value);
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
                // Only update UseCustomExtent if setting UseCurrentMapExtent to true
                if (value)
                {
                    UseCustomExtent = false;
                }
                // Always raise can execute changed for the command
                (SetCustomExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();

                // Add debug info
                System.Diagnostics.Debug.WriteLine($"UseCurrentMapExtent set to {value}, UseCustomExtent is now {_useCustomExtent}");
            }
        }

        private bool _useCustomExtent = false;
        public bool UseCustomExtent
        {
            get => _useCustomExtent;
            set
            {
                SetProperty(ref _useCustomExtent, value);
                // Only update UseCurrentMapExtent if setting UseCustomExtent to true
                if (value)
                {
                    UseCurrentMapExtent = false;
                }
                // Always raise can execute changed for the command
                (SetCustomExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();

                // Add debug info
                System.Diagnostics.Debug.WriteLine($"UseCustomExtent set to {value}, UseCurrentMapExtent is now {_useCurrentMapExtent}");
            }
        }

        private bool _isLoading = true;
        public bool IsLoading
        {
            get => _isLoading;
            set => SetProperty(ref _isLoading, value);
        }

        private Envelope _customExtent;
        public Envelope CustomExtent
        {
            get => _customExtent;
            set
            {
                SetProperty(ref _customExtent, value);
                // Update the display string and has-extent flag when extent changes
                HasCustomExtent = value != null;
                UpdateCustomExtentDisplay();
            }
        }

        private bool _hasCustomExtent;
        public bool HasCustomExtent
        {
            get => _hasCustomExtent;
            set => SetProperty(ref _hasCustomExtent, value);
        }

        private string _customExtentDisplay = "No custom extent set";
        public string CustomExtentDisplay
        {
            get => _customExtentDisplay;
            set => SetProperty(ref _customExtentDisplay, value);
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

        private string _themeIconText = "GlobeIcon"; // Default icon (globe)
        public string ThemeIconText
        {
            get => _themeIconText;
            set => SetProperty(ref _themeIconText, value);
        }

        private bool _createMfc = true;
        public bool CreateMfc
        {
            get => _createMfc;
            set => SetProperty(ref _createMfc, value);
        }

        private bool _useSpatialIndex = true;
        public bool UseSpatialIndex
        {
            get => _useSpatialIndex;
            set => SetProperty(ref _useSpatialIndex, value);
        }

        private string _mfcOutputPath;
        public string MfcOutputPath
        {
            get => _mfcOutputPath;
            set => SetProperty(ref _mfcOutputPath, value);
        }

        private bool _isSharedMfc = true;
        public bool IsSharedMfc
        {
            get => _isSharedMfc;
            set => SetProperty(ref _isSharedMfc, value);
        }
        #endregion

        #region Commands
        public ICommand LoadDataCommand { get; private set; }
        public ICommand ShowThemeInfoCommand { get; private set; }
        public ICommand SetCustomExtentCommand { get; private set; }
        public ICommand BrowseMfcLocationCommand { get; private set; }
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
            // Use the single selection for preview purposes for the description
            if (string.IsNullOrEmpty(SelectedTheme))
            {
                ThemeDescription = "Select one or more themes to load";
                EstimatedFeatures = "--";
                EstimatedSize = "--";
                return;
            }

            ThemeDescription = ThemeDescriptions.TryGetValue(SelectedTheme, out string description)
                ? description
                : "No description available";

            // Calculate combined estimates for all selected themes
            if (SelectedThemes.Count > 0)
            {
                int totalEstimatedFeatures = 0;
                double totalSizeInKb = 0;

                foreach (var theme in SelectedThemes)
                {
                    if (ThemeFeatureEstimates.TryGetValue(theme, out int estimate))
                    {
                        totalEstimatedFeatures += estimate;
                        totalSizeInKb += estimate * 2.5; // Assuming each feature is about 2.5KB on average
                    }
                }

                // Format the estimates
                if (SelectedThemes.Count > 1)
                {
                    EstimatedFeatures = $"{totalEstimatedFeatures} total per sq km (approx.)";
                    EstimatedSize = totalSizeInKb > 1024
                        ? $"{totalSizeInKb / 1024:F1} MB total per sq km (approx.)"
                        : $"{totalSizeInKb:F0} KB total per sq km (approx.)";
                }
                else
                {
                    // For single selection, use the original format
                    var estimate = ThemeFeatureEstimates[SelectedTheme];
                    EstimatedFeatures = $"{estimate} per sq km (approx.)";
                    double sizeInKb = estimate * 2.5;
                    EstimatedSize = sizeInKb > 1024
                        ? $"{sizeInKb / 1024:F1} MB per sq km (approx.)"
                        : $"{sizeInKb:F0} KB per sq km (approx.)";
                }
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

            string description = ThemeDescriptions.TryGetValue(SelectedTheme, out string themeDesc)
                ? themeDesc
                : "No detailed information available.";

            string types = ThemeTypes.TryGetValue(SelectedTheme, out string themeTypes)
                ? $"Type(s): {themeTypes}"
                : "";

            string selectedCount = _selectedThemes.Count > 0 ?
                $"\n\nYou have selected {_selectedThemes.Count} theme(s) in total." : "";

            ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                $"{description}\n\n{types}{selectedCount}",
                $"About {SelectedTheme} theme",
                System.Windows.MessageBoxButton.OK,
                System.Windows.MessageBoxImage.Information);
        }

        private void SetCustomExtent()
        {
            try
            {
                // Add diagnostic logging
                System.Diagnostics.Debug.WriteLine("SetCustomExtent method called");
                AddToLog("SetCustomExtent method called - attempting to activate drawing tool");

                // Ensure the custom extent radio button is selected
                UseCustomExtent = true;

                // Make sure we're subscribed to the static event
                // We already subscribed in the constructor, but ensure it's still active
                try
                {
                    // Remove any existing subscription and add it again to be safe
                    // This prevents multiple handlers if called multiple times
                    CustomExtentTool.ExtentCreatedStatic -= OnExtentCreated;
                    CustomExtentTool.ExtentCreatedStatic += OnExtentCreated;
                    System.Diagnostics.Debug.WriteLine("Re-established event subscription for CustomExtentTool");
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error managing event subscriptions: {ex.Message}");
                }

                // Create the instance tool as well for backward compatibility
                if (_customExtentTool == null)
                {
                    _customExtentTool = new CustomExtentTool();
                    _customExtentTool.ExtentCreated += OnExtentCreated;
                }

                // Use ArcGIS Pro's drawing tool to select an extent
                QueuedTask.Run(async () =>
                {
                    System.Diagnostics.Debug.WriteLine("Inside QueuedTask.Run");
                    AddToLog("Starting custom extent drawing operation...");

                    // Get a reference to the active map and make sure one exists
                    var mapView = MapView.Active;
                    if (mapView == null)
                    {
                        AddToLog("Unable to set custom extent: No active map view");
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                            "Please open a map before setting a custom extent.",
                            "No Active Map");
                        return;
                    }

                    AddToLog($"Active map view found: {mapView.Map.Name}");

                    try
                    {
                        // Activate our custom tool
                        AddToLog("Activating custom drawing tool...");
                        System.Diagnostics.Debug.WriteLine("Activating custom extent tool");

                        // Use our custom tool by ID as defined in the Config.daml
                        await FrameworkApplication.SetCurrentToolAsync("DuckDBGeoparquet_CustomExtentTool");
                        AddToLog("Draw a rectangle on the map to set the custom extent...");
                        System.Diagnostics.Debug.WriteLine("Custom tool activated successfully");
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"Error in tool activation: {ex.Message}");
                        System.Diagnostics.Debug.WriteLine($"Exception in tool activation: {ex}");
                    }
                });
            }
            catch (Exception ex)
            {
                AddToLog($"Error setting custom extent: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Exception in SetCustomExtent: {ex}");
            }
        }

        // Handler for when our custom tool creates an extent
        private void OnExtentCreated(Envelope extent)
        {
            // Add more detailed logging
            System.Diagnostics.Debug.WriteLine($"Custom extent created: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
            AddToLog($"CUSTOM EXTENT SET SUCCESSFULLY: {extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}");

            // Store the extent - this will trigger the property change handlers
            CustomExtent = extent;

            // Explicitly set these properties to ensure UI updates
            HasCustomExtent = true;
            UpdateCustomExtentDisplay();
            NotifyPropertyChanged("CustomExtentDisplay");
            NotifyPropertyChanged("HasCustomExtent");

            // Make sure custom extent radio is selected
            UseCustomExtent = true;
            UseCurrentMapExtent = false;

            // First ensure we're completely back to the default tool
            QueuedTask.Run(async () => {
                try
                {
                    // Make absolutely sure we return to the default explore tool first
                    await FrameworkApplication.SetCurrentToolAsync("esri_mapping_exploreTool");
                    System.Diagnostics.Debug.WriteLine("Ensuring return to default tool before showing feedback");

                    // Give the UI thread a moment to update and remove the sketch cursor
                    await Task.Delay(200);

                    // Then show the message box
                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                        $"Custom extent set successfully:\nMin X,Y: {extent.XMin:F4}, {extent.YMin:F4}\nMax X,Y: {extent.XMax:F4}, {extent.YMax:F4}",
                        "Custom Extent Set",
                        System.Windows.MessageBoxButton.OK,
                        System.Windows.MessageBoxImage.Information);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error showing custom extent feedback: {ex.Message}");
                }
            });

            // Update the UI to reflect the custom extent is set
            AddToLog("Custom extent will be used for data loading");
            AddToLog("You may now select data themes and click 'Load Data'");

            // Force property change notifications
            NotifyPropertyChanged("UseCustomExtent");
            NotifyPropertyChanged("UseCurrentMapExtent");
        }

        private void BrowseMfcLocation()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select folder for MFC output",
                UseDescriptionForTitle = true,
                SelectedPath = MfcOutputPath ?? Services.MfcUtility.DefaultMfcBasePath
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                MfcOutputPath = dialog.SelectedPath;
            }
        }

        private void UpdateCustomExtentDisplay()
        {
            if (_customExtent != null)
            {
                CustomExtentDisplay = $"Min X: {_customExtent.XMin:F4}\nMin Y: {_customExtent.YMin:F4}\nMax X: {_customExtent.XMax:F4}\nMax Y: {_customExtent.YMax:F4}";
            }
            else
            {
                CustomExtentDisplay = "No custom extent set";
            }
        }
        #endregion

        private async Task<string> GetLatestRelease()
        {
            try
            {
                using var client = new HttpClient();
                var response = await client.GetStringAsync(RELEASE_URL);
                System.Diagnostics.Debug.WriteLine($"Release API Response: {response}");
                AddToLog("Received release information from Overture Maps API");

                var releaseInfo = JsonSerializer.Deserialize<ReleaseInfo>(response, _jsonOptions)
                    ?? throw new Exception("Failed to deserialize release info");

                System.Diagnostics.Debug.WriteLine($"Deserialized Latest Release: {releaseInfo.Latest}");
                AddToLog($"Latest release available: {releaseInfo.Latest}");
                return releaseInfo.Latest;
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
                if (SelectedThemes.Count == 0)
                {
                    AddToLog("No themes selected.");
                    return;
                }

                // Switch to status tab
                SelectedTabIndex = 1;

                StatusText = $"Loading {SelectedThemes.Count} themes...";
                AddToLog($"Starting to load multiple themes from release {LatestRelease}");

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

                int totalThemeTypes = 0;
                // Calculate total number of theme types to process for progress reporting
                foreach (var theme in SelectedThemes)
                {
                    if (ThemeTypes.TryGetValue(theme, out string themeTypes))
                    {
                        string[] types = themeTypes.Split(',');
                        totalThemeTypes += types.Length;
                    }
                }

                int processedTypes = 0;
                // Process each selected theme
                foreach (var theme in SelectedThemes)
                {
                    StatusText = $"Processing theme: {theme}";
                    AddToLog($"Processing theme: {theme}");

                    // Process each type within the current theme
                    if (ThemeTypes.TryGetValue(theme, out string themeTypes))
                    {
                        string[] types = themeTypes.Split(',');

                        foreach (var type in types)
                        {
                            AddToLog($"Processing theme type: {type.Trim()}");
                            System.Diagnostics.Debug.WriteLine($"Theme type: {type.Trim()}");

                            // Ensure proper path construction with trimmed release
                            string trimmedRelease = LatestRelease?.Trim() ?? "";
                            string s3Path = trimmedRelease.Length > 0
                                ? $"{S3_BASE_PATH}/{trimmedRelease}/theme={theme}/type={type.Trim()}/*.parquet"
                                : $"{S3_BASE_PATH}/theme={theme}/type={type.Trim()}/*.parquet";

                            StatusText = $"Loading from path: {s3Path}";
                            AddToLog($"Loading from path: {s3Path}");
                            System.Diagnostics.Debug.WriteLine($"Loading from path: {s3Path}");

                            await _dataProcessor.IngestFileAsync(s3Path, extent);

                            // Create a feature layer for the loaded data
                            string layerName = $"{theme} - {type.Trim()}";
                            var progress = new Progress<string>(status =>
                            {
                                StatusText = status;
                                AddToLog(status);
                                if (status.StartsWith("Processing: "))
                                {
                                    var percent = status.Split('%')[0].Split(':')[1].Trim();
                                    if (double.TryParse(percent, out double value))
                                    {
                                        // Scale the progress to the overall progress across all themes
                                        processedTypes++;
                                        ProgressValue = (processedTypes * 100.0) / totalThemeTypes;
                                    }
                                }
                            });

                            await _dataProcessor.CreateFeatureLayerAsync(layerName, LatestRelease, progress);

                            StatusText = $"Successfully loaded {theme}/{type} data from release {LatestRelease}";
                            AddToLog($"Successfully loaded {theme}/{type} data");
                        }
                    }
                }

                // After successfully loading all the data, create MFC if requested
                if (CreateMfc && !string.IsNullOrEmpty(MfcOutputPath))
                {
                    StatusText = "Creating Multifile Feature Connection...";
                    AddToLog("Setting up Multifile Feature Connection for loaded data");

                    // Use the data files already stored in the MfcUtility.DefaultMfcBasePath/Data folder
                    // No need to append "Data" to MfcOutputPath since we want to store the MFC file directly in the output path
                    string dataFolder = Path.Combine(Services.MfcUtility.DefaultMfcBasePath, "Data");
                    string mfcFilePath = Path.Combine(
                        MfcOutputPath,  // This should be the parent Connections folder
                        $"OvertureRelease_{LatestRelease.Replace("-", "")}.mfc"
                    );

                    bool success = await Services.MfcUtility.CreateMfcAsync(
                        dataFolder,     // Point to where the actual data files are
                        mfcFilePath,    // Where to create the MFC file
                        IsSharedMfc
                    );

                    if (success)
                    {
                        StatusText = "Successfully created Multifile Feature Connection";
                        AddToLog($"MFC created at: {mfcFilePath}");
                    }
                    else
                    {
                        StatusText = "Error creating Multifile Feature Connection";
                        AddToLog("Failed to create MFC. See ArcGIS Pro logs for details.");
                    }
                }

                StatusText = $"Successfully loaded all selected themes from release {LatestRelease}";
                AddToLog($"All selected themes loaded successfully");
                ProgressValue = 100;
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

        public bool IsThemeSelected(string theme)
        {
            return _selectedThemes.Contains(theme);
        }

        public void ToggleThemeSelection(string theme)
        {
            if (!_selectedThemes.Remove(theme))
                _selectedThemes.Add(theme);

            NotifyPropertyChanged("SelectedThemes");
            UpdateThemePreview();
            (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        // Add a method to check the selected status in the ViewModel
        private void CheckInitialThemeSelection()
        {
            // Update the preview based on the first selected theme (if any)
            if (_selectedThemes.Count > 0)
            {
                SelectedTheme = _selectedThemes[0];
            }
        }

        // Cleanup method that will be called when the add-in is unloaded
        // No override needed - this will be called by the framework
        private void CleanupResources()
        {
            // Cleanup by unsubscribing from static events
            System.Diagnostics.Debug.WriteLine("WizardDockpaneViewModel cleaning up, unsubscribing from static events");
            CustomExtentTool.ExtentCreatedStatic -= OnExtentCreated;
        }

        ~WizardDockpaneViewModel()
        {
            CleanupResources();
        }
    }
}

