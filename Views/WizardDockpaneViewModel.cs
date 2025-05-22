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
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Linq;
using System.Data;
using DuckDBGeoparquet.Services;
using Microsoft.Win32;
using ArcGIS.Desktop.Catalog;
using System.Net.Http;
using System.Text.Json;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Threading;

namespace DuckDBGeoparquet.Views
{
    // Selectable theme item class for binding
    public class SelectableThemeItem : INotifyPropertyChanged
    {
        private string _name;
        private bool _isSelected;

        public string Name
        {
            get => _name;
            set
            {
                if (_name != value)
                {
                    _name = value;
                    OnPropertyChanged();
                }
            }
        }

        public bool IsSelected
        {
            get => _isSelected;
            set
            {
                if (_isSelected != value)
                {
                    _isSelected = value;
                    OnPropertyChanged();
                    SelectionChanged?.Invoke(this, EventArgs.Empty);
                }
            }
        }

        public event EventHandler SelectionChanged;
        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        public SelectableThemeItem(string name, bool isSelected = false)
        {
            _name = name;
            _isSelected = isSelected;
        }
    }

    internal class WizardDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_WizardDockpane";
        private readonly DataProcessor _dataProcessor;
        private const string RELEASE_URL = "https://labs.overturemaps.org/data/releases.json";
        private const string S3_BASE_PATH = "s3://overturemaps-us-west-2/release";

        // Add CancellationTokenSource for cancelling operations
        private CancellationTokenSource _cts;

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

            BrowseDataLocationCommand = new RelayCommand(
                () => BrowseDataLocation()
            );

            BrowseCustomDataFolderCommand = new RelayCommand(
                () => BrowseCustomDataFolder()
            );

            CreateMfcCommand = new RelayCommand(
                async () => await CreateMfcAsync(),
                () => (UsePreviouslyLoadedData && !string.IsNullOrEmpty(_lastLoadedDataPath)) ||
                      (UseCustomDataFolder && !string.IsNullOrEmpty(CustomDataFolderPath))
            );

            GoToCreateMfcTabCommand = new RelayCommand(
                () => ShowCreateMfcTab(),
                () => true // Always enabled
            );

            CancelCommand = new RelayCommand(
                () =>
                {
                    // Cancel any ongoing operations
                    if (_cts != null && !_cts.IsCancellationRequested)
                    {
                        AddToLog("Operation cancelled by user");
                        System.Diagnostics.Debug.WriteLine("Operation cancelled by user");
                        _cts.Cancel();
                    }

                    // Reset the state of the add-in
                    ResetState();
                    AddToLog("Add-in state has been reset");

                    // Close the dockpane - use the base class method
                    try
                    {
                        // Use the Hide method to close the dockpane
                        // In ArcGIS Pro 3.5, DockPane base class provides this method
                        this.Hide();
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"Error closing dockpane: {ex.Message}");
                    }
                }
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
            Themes = new ObservableCollection<SelectableThemeItem>
            {
                new SelectableThemeItem("addresses"),
                new SelectableThemeItem("base"),
                new SelectableThemeItem("buildings"),
                new SelectableThemeItem("divisions"),
                new SelectableThemeItem("places"),
                new SelectableThemeItem("transportation")
            };

            // Subscribe to selection changed events for each theme item
            foreach (var themeItem in Themes)
            {
                themeItem.SelectionChanged += OnThemeSelectionChanged;
            }

            LogOutput = new();
            LogOutput.AppendLine("Initializing...");

            System.Diagnostics.Debug.WriteLine("Starting async initialization");
            _ = InitializeAsync();

            // Set default paths
            var defaultBasePath = Services.MfcUtility.DefaultMfcBasePath;

            // Data output path is where the GeoParquet files will be stored
            DataOutputPath = Path.Combine(
                defaultBasePath,
                "Data",
                LatestRelease ?? "latest"
            );

            // MFC output path is where the MFC connection file will be stored
            MfcOutputPath = Path.Combine(
                defaultBasePath,
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

        private ObservableCollection<SelectableThemeItem> _themes;
        public ObservableCollection<SelectableThemeItem> Themes
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

        private string _dataOutputPath;
        public string DataOutputPath
        {
            get => _dataOutputPath;
            set => SetProperty(ref _dataOutputPath, value);
        }

        private bool _usePreviouslyLoadedData = true;
        public bool UsePreviouslyLoadedData
        {
            get => _usePreviouslyLoadedData;
            set
            {
                SetProperty(ref _usePreviouslyLoadedData, value);
                if (value)
                {
                    UseCustomDataFolder = false;
                }
            }
        }

        private bool _useCustomDataFolder = false;
        public bool UseCustomDataFolder
        {
            get => _useCustomDataFolder;
            set
            {
                SetProperty(ref _useCustomDataFolder, value);
                if (value)
                {
                    UsePreviouslyLoadedData = false;
                }
            }
        }

        private string _customDataFolderPath;
        public string CustomDataFolderPath
        {
            get => _customDataFolderPath;
            set => SetProperty(ref _customDataFolderPath, value);
        }

        // Track the last loaded data path for MFC creation
        private string _lastLoadedDataPath;
        #endregion

        #region Commands
        public ICommand LoadDataCommand { get; private set; }
        public ICommand ShowThemeInfoCommand { get; private set; }
        public ICommand SetCustomExtentCommand { get; private set; }
        public ICommand BrowseMfcLocationCommand { get; private set; }
        public ICommand BrowseDataLocationCommand { get; private set; }
        public ICommand BrowseCustomDataFolderCommand { get; private set; }
        public ICommand CreateMfcCommand { get; private set; }
        public ICommand GoToCreateMfcTabCommand { get; private set; }
        public ICommand CancelCommand { get; private set; }
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
                Description = "Select base folder for Overture MFC structure",
                UseDescriptionForTitle = true,
                SelectedPath = MfcOutputPath ?? Services.MfcUtility.DefaultMfcBasePath
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                // Set the MfcOutputPath to the selected base folder
                // The Connection and Data subfolders will be created automatically during processing
                MfcOutputPath = dialog.SelectedPath;

                // Display helpful information to the user
                AddToLog($"Selected base folder: {MfcOutputPath}");
                AddToLog("The following structure will be created:");
                AddToLog($"- {Path.Combine(MfcOutputPath, "Connection")} (for the MFC file)");
                AddToLog($"- {Path.Combine(MfcOutputPath, "Data", LatestRelease)} (for the data files)");
            }
        }

        private void BrowseDataLocation()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select folder for GeoParquet data files",
                UseDescriptionForTitle = true,
                SelectedPath = DataOutputPath ?? Path.Combine(Services.MfcUtility.DefaultMfcBasePath, "Data")
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                DataOutputPath = dialog.SelectedPath;
                AddToLog($"Data files will be saved to: {DataOutputPath}");
            }
        }

        private void BrowseCustomDataFolder()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select folder containing GeoParquet data files",
                UseDescriptionForTitle = true,
                SelectedPath = CustomDataFolderPath ?? DataOutputPath ?? Path.Combine(Services.MfcUtility.DefaultMfcBasePath, "Data")
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                CustomDataFolderPath = dialog.SelectedPath;
                AddToLog($"Custom data folder set to: {CustomDataFolderPath}");

                // Update command can execute
                (CreateMfcCommand as RelayCommand)?.RaiseCanExecuteChanged();
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

                // Initialize a new cancellation token source
                _cts?.Dispose();
                _cts = new CancellationTokenSource();
                var cancellationToken = _cts.Token;

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

                // Check for cancellation
                if (cancellationToken.IsCancellationRequested)
                {
                    StatusText = "Operation cancelled";
                    AddToLog("Operation was cancelled");
                    return;
                }

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
                    // Check for cancellation between themes
                    if (cancellationToken.IsCancellationRequested)
                    {
                        StatusText = "Operation cancelled";
                        AddToLog("Operation was cancelled");
                        return;
                    }

                    StatusText = $"Processing theme: {theme}";
                    AddToLog($"Processing theme: {theme}");

                    // Process each type within the current theme
                    if (ThemeTypes.TryGetValue(theme, out string themeTypes))
                    {
                        string[] types = themeTypes.Split(',');

                        foreach (var type in types)
                        {
                            // Check for cancellation between types
                            if (cancellationToken.IsCancellationRequested)
                            {
                                StatusText = "Operation cancelled";
                                AddToLog("Operation was cancelled");
                                return;
                            }

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

                            // Pass cancellation token to IngestFileAsync
                            bool ingestSuccess = await _dataProcessor.IngestFileAsync(s3Path, extent);
                            if (!ingestSuccess)
                            {
                                AddToLog($"Failed to ingest data from {s3Path}");
                                StatusText = $"Error loading data from {s3Path}";
                                continue;
                            }

                            // Check for cancellation
                            if (cancellationToken.IsCancellationRequested)
                            {
                                StatusText = "Operation cancelled";
                                AddToLog("Operation was cancelled");
                                return;
                            }

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

                // Check for cancellation
                if (cancellationToken.IsCancellationRequested)
                {
                    StatusText = "Operation cancelled";
                    AddToLog("Operation was cancelled");
                    return;
                }

                // Store the data path for potential MFC creation later
                _lastLoadedDataPath = DataOutputPath;

                // Now that the data is loaded, inform the user they can create an MFC if desired
                AddToLog("----------------");
                AddToLog("Data loading complete. You can now:");
                AddToLog("1. Work with the loaded GeoParquet data directly");
                AddToLog("2. Create a Multifile Feature Connection (MFC) from the 'Create MFC' tab");
                AddToLog("----------------");

                // Show a message box offering to go to the Create MFC tab
                var result = ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    "Data loading complete. Would you like to create a Multifile Feature Connection (MFC) now?",
                    "Create MFC?",
                    System.Windows.MessageBoxButton.YesNo,
                    System.Windows.MessageBoxImage.Question);

                if (result == System.Windows.MessageBoxResult.Yes)
                {
                    // Navigate to the Create MFC tab
                    ShowCreateMfcTab();
                }

                // Update the Create MFC command can-execute state
                (CreateMfcCommand as RelayCommand)?.RaiseCanExecuteChanged();

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
            finally
            {
                // Clean up the cancellation token source
                if (_cts != null)
                {
                    _cts.Dispose();
                    _cts = null;
                }
            }
        }

        private async Task CreateMfcAsync()
        {
            try
            {
                // Initialize a new cancellation token source
                _cts?.Dispose();
                _cts = new CancellationTokenSource();
                var cancellationToken = _cts.Token;

                // Switch to status tab
                SelectedTabIndex = 1; // Status tab is now index 1

                StatusText = "Creating Multifile Feature Connection...";
                AddToLog("Setting up Multifile Feature Connection for data");
                ProgressValue = 0;

                // Determine the data source folder
                string dataFolder;

                if (UsePreviouslyLoadedData && !string.IsNullOrEmpty(_lastLoadedDataPath))
                {
                    dataFolder = _lastLoadedDataPath;
                    AddToLog($"Using previously loaded data from: {dataFolder}");
                }
                else if (UseCustomDataFolder && !string.IsNullOrEmpty(CustomDataFolderPath))
                {
                    dataFolder = CustomDataFolderPath;
                    AddToLog($"Using custom data folder: {dataFolder}");
                }
                else
                {
                    StatusText = "Error: No valid data folder specified";
                    AddToLog("ERROR: No valid data folder specified for MFC creation");
                    return;
                }

                // Ensure connection folder exists
                string connectionFolder = MfcOutputPath;
                if (!Directory.Exists(connectionFolder))
                {
                    AddToLog($"Creating connection folder: {connectionFolder}");
                    Directory.CreateDirectory(connectionFolder);
                }

                // Check if data folder exists and has content
                if (!Directory.Exists(dataFolder))
                {
                    AddToLog($"ERROR: Data folder does not exist: {dataFolder}");
                    StatusText = "Error creating Multifile Feature Connection - data folder not found";
                    return;
                }

                // Check for cancellation
                if (cancellationToken.IsCancellationRequested)
                {
                    StatusText = "Operation cancelled";
                    AddToLog("Operation was cancelled during MFC preparation");
                    return;
                }

                // Do a sanity check on the data folder contents
                int fileCount = Directory.GetFiles(dataFolder, "*.parquet", SearchOption.AllDirectories).Length;
                AddToLog($"Found {fileCount} parquet files in data folder");

                if (fileCount == 0)
                {
                    // Check if theme folders were created
                    var themeFolders = Directory.GetDirectories(dataFolder);
                    AddToLog($"Found {themeFolders.Length} theme folders in {dataFolder}");

                    foreach (var folder in themeFolders)
                    {
                        AddToLog($"Theme folder: {Path.GetFileName(folder)}");
                        var typeFolders = Directory.GetDirectories(folder);
                        AddToLog($"  Contains {typeFolders.Length} type folders");

                        foreach (var typeFolder in typeFolders)
                        {
                            var filesInType = Directory.GetFiles(typeFolder, "*.parquet");
                            AddToLog($"  Type folder {Path.GetFileName(typeFolder)} contains {filesInType.Length} parquet files");
                        }
                    }

                    if (fileCount == 0)
                    {
                        AddToLog("No data files were found in the specified folder. Cannot create MFC.");
                        StatusText = "Error creating Multifile Feature Connection - no data files found";
                        return;
                    }
                }

                // Check for cancellation
                if (cancellationToken.IsCancellationRequested)
                {
                    StatusText = "Operation cancelled";
                    AddToLog("Operation was cancelled before MFC creation");
                    return;
                }

                // Create a nice MFC filename based on the release and sanitize it
                string releaseName = LatestRelease?.Replace("-", "") ?? "Latest";
                string mfcName = $"OvertureRelease_{releaseName}";
                string mfcFilePath = Path.Combine(connectionFolder, $"{mfcName}.mfc");

                AddToLog($"MFC source folder: {dataFolder}");
                AddToLog($"MFC output location: {connectionFolder}");
                AddToLog($"MFC name: {mfcName}");
                AddToLog($"Spatial indexing: {(UseSpatialIndex ? "Enabled" : "Disabled")}");
                AddToLog($"Connection type: {(IsSharedMfc ? "Shared" : "Standalone")}");

                ProgressValue = 30; // Show progress starting

                try
                {
                    bool success = await Services.MfcUtility.CreateMfcAsync(
                        dataFolder,         // Source folder with the properly structured datasets
                        mfcFilePath,        // Full path to the output MFC file
                        IsSharedMfc,        // Whether to create a shared connection
                        true,               // Always keep geometry fields visible
                        true                // Always keep time fields visible
                    );

                    // Check for cancellation
                    if (cancellationToken.IsCancellationRequested)
                    {
                        StatusText = "Operation cancelled";
                        AddToLog("Operation was cancelled during MFC creation");
                        return;
                    }

                    ProgressValue = 100; // Complete

                    if (success)
                    {
                        StatusText = "Successfully created Multifile Feature Connection";
                        AddToLog($"MFC created at: {mfcFilePath}");

                        // Provide simplified instructions for adding the MFC to the project
                        try
                        {
                            AddToLog("----------------");
                            AddToLog("To use the MFC in your project:");
                            AddToLog("1. In the Catalog pane, navigate to the location of the MFC file");
                            AddToLog($"2. Right-click on the file: {Path.GetFileName(mfcFilePath)}");
                            AddToLog("3. Select 'Add To Project'");
                            AddToLog("4. The MFC will appear in the 'Multifile Feature Connections' section");
                            AddToLog("----------------");

                            // Display a message box with instructions
                            ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                                $"Multifile Feature Connection created successfully!\n\n" +
                                $"Location: {mfcFilePath}\n\n" +
                                "To add it to your project:\n" +
                                "1. Navigate to the MFC file in the Catalog pane\n" +
                                $"2. Right-click on '{Path.GetFileName(mfcFilePath)}'\n" +
                                "3. Select 'Add To Project'",
                                "MFC Created Successfully",
                                System.Windows.MessageBoxButton.OK,
                                System.Windows.MessageBoxImage.Information);
                        }
                        catch (Exception ex)
                        {
                            AddToLog($"Warning: {ex.Message}");
                        }
                    }
                    else
                    {
                        StatusText = "Error creating Multifile Feature Connection";
                        AddToLog("Failed to create MFC. See ArcGIS Pro logs for details.");

                        // Show a message box with more details to help the user
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                            "The Multifile Feature Connection could not be created.\n\n" +
                            "Possible reasons:\n" +
                            "1. No data files were found in the expected folder structure\n" +
                            "2. The GeoParquet files don't have the correct structure\n" +
                            "3. ArcGIS Pro doesn't have permission to create the MFC file\n\n" +
                            "Check the log tab for more details.",
                            "MFC Creation Failed",
                            System.Windows.MessageBoxButton.OK,
                            System.Windows.MessageBoxImage.Warning);
                    }
                }
                catch (Exception ex)
                {
                    StatusText = "Error creating Multifile Feature Connection";
                    AddToLog($"ERROR: Exception creating MFC: {ex.Message}");
                    AddToLog($"Stack trace: {ex.StackTrace}");
                    ProgressValue = 0;
                }
            }
            catch (Exception ex)
            {
                StatusText = $"Error creating MFC: {ex.Message}";
                AddToLog($"ERROR: {ex.Message}");
                AddToLog($"Stack trace: {ex.StackTrace}");
                ProgressValue = 0;
                System.Diagnostics.Debug.WriteLine($"MFC creation error: {ex}");
            }
            finally
            {
                // Clean up the cancellation token source
                if (_cts != null)
                {
                    _cts.Dispose();
                    _cts = null;
                }
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

            // Reset the state when showing the dockpane
            if (pane is WizardDockpaneViewModel viewModel)
            {
                viewModel.ResetState();
            }

            pane.Activate();
        }

        public bool IsThemeSelected(string theme)
        {
            var themeItem = Themes.FirstOrDefault(t => t.Name == theme);
            return themeItem != null && themeItem.IsSelected;
        }

        public void ToggleThemeSelection(string theme)
        {
            var themeItem = Themes.FirstOrDefault(t => t.Name == theme);
            if (themeItem != null)
            {
                themeItem.IsSelected = !themeItem.IsSelected;
                // The OnThemeSelectionChanged event handler will update SelectedThemes
            }
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

            // Dispose of the cancellation token source if it exists
            if (_cts != null)
            {
                _cts.Dispose();
                _cts = null;
            }
        }

        ~WizardDockpaneViewModel()
        {
            CleanupResources();
        }

        private void OnThemeSelectionChanged(object sender, EventArgs e)
        {
            // Update the SelectedThemes list based on the currently selected theme items
            _selectedThemes.Clear();
            foreach (var themeItem in Themes)
            {
                if (themeItem.IsSelected)
                {
                    _selectedThemes.Add(themeItem.Name);
                }
            }

            NotifyPropertyChanged("SelectedThemes");
            UpdateThemePreview();
            (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();

            // If a theme was selected, set it as the current preview theme
            if (sender is SelectableThemeItem selectedItem && selectedItem.IsSelected)
            {
                SelectedTheme = selectedItem.Name;
            }
            else if (_selectedThemes.Count > 0)
            {
                // If we just deselected an item but others are still selected, show the first selected theme
                SelectedTheme = _selectedThemes[0];
            }
            else
            {
                // If no themes are selected, clear the selection
                SelectedTheme = null;
            }
        }

        private void ResetState()
        {
            // Clear theme selections
            foreach (var themeItem in Themes)
            {
                // Temporarily unsubscribe to avoid multiple event triggers
                themeItem.SelectionChanged -= OnThemeSelectionChanged;
                themeItem.IsSelected = false;
                themeItem.SelectionChanged += OnThemeSelectionChanged;
            }

            // Clear selected themes list
            _selectedThemes.Clear();
            NotifyPropertyChanged("SelectedThemes");

            // Reset other properties
            SelectedTheme = null;
            SelectedTabIndex = 0; // Switch back to the first tab

            // Reset extent options
            UseCurrentMapExtent = true;
            UseCustomExtent = false;
            CustomExtent = null;

            // Reset data and MFC options
            var defaultBasePath = Services.MfcUtility.DefaultMfcBasePath;

            // Reset data options
            DataOutputPath = Path.Combine(
                defaultBasePath,
                "Data",
                LatestRelease ?? "latest"
            );

            // Reset MFC options
            UseSpatialIndex = true;
            IsSharedMfc = true;
            MfcOutputPath = Path.Combine(
                defaultBasePath,
                "Connections"
            );

            // Reset data source options for MFC
            UsePreviouslyLoadedData = true;
            UseCustomDataFolder = false;
            CustomDataFolderPath = null;
            _lastLoadedDataPath = null;

            // Reset progress and status
            ProgressValue = 0;
            StatusText = "Ready to load Overture Maps data";

            // Clear log but keep initialization messages
            LogOutput = new();
            LogOutput.AppendLine("Initialization complete. Ready for a new query.");
            LogOutputText = LogOutput.ToString();
            NotifyPropertyChanged("LogOutputText");

            // Raise can execute changed on commands
            (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (ShowThemeInfoCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (SetCustomExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();

            System.Diagnostics.Debug.WriteLine("Add-in state has been reset");
        }

        private void ShowCreateMfcTab()
        {
            // Navigate to the Create MFC tab (index 2)
            SelectedTabIndex = 2;
            StatusText = "Ready to create Multifile Feature Connection";
            AddToLog("Create MFC tab activated");
        }
    }
}

