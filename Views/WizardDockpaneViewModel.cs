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
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Microsoft.Win32;
using ArcGIS.Desktop.Catalog;
using System.Net.Http;
using System.Text.Json;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Threading;
using ArcGIS.Desktop.Core;
using System.Net;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DuckDBGeoparquet.Views
{
    internal partial class WizardDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_WizardDockpane_V2";
        private DataProcessor _dataProcessor;
        private const string S3_BASE_PATH = "s3://overturemaps-us-west-2/release";

        // Add CancellationTokenSource for cancelling operations
        private CancellationTokenSource _cts;


        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        // Default MFC base path: <project>\OvertureProAddinData (MyDocuments fallback).
        // Shared with the geocoder and the MFC dockpane via ProjectDataLocator.
        private static string DeterminedDefaultMfcBasePath => ProjectDataLocator.GetAddinDataBase();

        // Store original Overture S3 theme structure
        private readonly Dictionary<string, string> _overtureS3ThemeTypes = new()
        {
            { "addresses", "address" },
            { "base", "land,water,land_use,land_cover,bathymetry,infrastructure" },
            { "buildings", "building,building_part" },
            { "divisions", "division,division_boundary,division_area" },
            { "places", "place" },
            { "transportation", "connector,segment" }
        };

        // Friendly display names for parent themes
        private readonly Dictionary<string, string> _parentThemeDisplayNames = new()
        {
            { "addresses", "Addresses" },
            { "base", "Base Layers" },
            { "buildings", "Buildings" },
            { "divisions", "Administrative Divisions" },
            { "places", "Places of Interest" },
            { "transportation", "Transportation Networks" }
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

        // IMPORTANT: Review and adjust these estimates for accuracy.
        // These are now based on specific "ActualType" rather than parent themes.
        private readonly Dictionary<string, int> ThemeFeatureEstimates = new()
        {
            // Addresses
            { "address", 500 }, // Previously under "addresses"

            // Base Sub-types
            { "land", 100 },
            { "water", 50 },
            { "land_use", 40 },
            { "land_cover", 40 },
            { "bathymetry", 20 },
            { "infrastructure", 50 },
            // Total for old "base" was 300, current sum is 300

            // Buildings Sub-types
            { "building", 700 },
            { "building_part", 100 },
            // Total for old "buildings" was 800, current sum is 800

            // Divisions Sub-types
            { "division", 30 },
            { "division_boundary", 40 },
            { "division_area", 30 },
            // Total for old "divisions" was 100, current sum is 100

            // Places
            { "place", 250 }, // Previously under "places"

            // Transportation Sub-types
            { "connector", 150 },
            { "segment", 600 }
            // Total for old "transportation" was 750, current sum is 750
        };

        private CustomExtentTool _customExtentTool;

        // Property for the TreeView to bind its selected item for preview
        private SelectableThemeItem _selectedItemForPreview;
        public SelectableThemeItem SelectedItemForPreview
        {
            get => _selectedItemForPreview;
            set
            {
                SetProperty(ref _selectedItemForPreview, value);
                // Update preview panel when the focused item in TreeView changes
                if (value != null)
                {
                    // Old SelectedTheme string is now derived from SelectedItemForPreview for compatibility
                    SelectedTheme = value.ParentThemeForS3; // Or value.DisplayName, depending on usage
                }
                UpdateThemePreview(); // Update description, estimates etc.
                (ShowThemeInfoCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        // Properties for TreeView Preview
        public int SelectedLeafItemCount => GetSelectedLeafItems().Count;
        
        public List<SelectableThemeItem> AllSelectedLeafItemsForPreview => GetSelectedLeafItems();

        // Public parameterless constructor for XAML Designer
        public WizardDockpaneViewModel()
        {
            // This constructor is ONLY for the XAML designer.
            // It should initialize properties to provide a design-time preview.
            // Do NOT call full runtime initialization logic (like InitializeAsync).
            if (System.ComponentModel.DesignerProperties.GetIsInDesignMode(new System.Windows.DependencyObject()))
            {
                Themes = new ObservableCollection<SelectableThemeItem>
                {
                    new SelectableThemeItem("Addresses (Design)", "addresses", "addresses", true),
                    new SelectableThemeItem("Base (Design)", "base", "base", false)
                    {
                        SubItems =
                        {
                            new SelectableThemeItem("Land (Design)", "land", "base", true),
                            new SelectableThemeItem("Water (Design)", "water", "base", true)
                        }
                    }
                };
                LatestRelease = "202X-XX-XX (Design)";
                StatusText = "Design Mode Preview - Themes Loaded";
                IsLoading = false;
                DataOutputPath = "C:\\Design\\Path\\Data";
                MfcOutputPath = "C:\\Design\\Path\\Connections";
                CustomExtentDisplay = "No custom extent set (Design)";
                ThemeDescription = "Select a theme (Design)";
                EstimatedFeatures = "-- (Design)";
                EstimatedSize = "-- (Design)";
                LogOutput = new StringBuilder("Design mode log output.\nReady.");
                LogOutputText = LogOutput.ToString();
            }
            else
            {
                // This case (public ctor at runtime) should ideally not happen if Pro uses the protected one.
                // If it does, we must ensure full initialization.
                System.Diagnostics.Debug.WriteLine("WARNING: Public parameterless constructor called at runtime. Performing full initialization.");
                InitializeViewModelForRuntime();
            }
        }

        private void InitializeViewModelForRuntime()
        {
            System.Diagnostics.Debug.WriteLine("InitializeViewModelForRuntime executing...");
            _dataProcessor = new DataProcessor();

            LoadDataCommand = new RelayCommand(async () => await LoadOvertureDataAsync(), () => !IsWorking && !_isPreviewSampling && !IsLoading && GetSelectedLeafItems().Count > 0);
            RefreshReleaseCommand = new RelayCommand(async () =>
            {
                IsLoading = true;
                try { await RefreshLatestReleaseAsync(); StatusText = ReleaseStatusText; }
                finally { IsLoading = false; }
            }, () => !IsLoading && !IsWorking);
            RefreshCacheInfoCommand = new RelayCommand(() => RefreshCacheInfo());
            ClearCacheCommand = new RelayCommand(() => ClearCache(), () => ArcGISProVersionHelper.IsPro36OrLater);
            ShowThemeInfoCommand = new RelayCommand(() => ShowThemeInfo(), () => SelectedItemForPreview != null);
            SetCustomExtentCommand = new RelayCommand(() => SetCustomExtent(), () => UseCustomExtent);            BrowseDataLocationCommand = new RelayCommand(() => BrowseDataLocation());            ApplyManualReleaseCommand = new RelayCommand(() => ApplyManualRelease(), () => !string.IsNullOrWhiteSpace(ManualReleaseText));
            NextCommand = new RelayCommand(async () => {
                if (SelectedTabIndex == (int)WizardTab.SelectData) {
                    if (GetSelectedLeafItems().Count == 0) {
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show("Please select at least one data theme to download before continuing.", "No Themes Selected", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Warning);
                        return;
                    }
                    SelectedTabIndex = (int)WizardTab.Preview;
                } else if (SelectedTabIndex == (int)WizardTab.Preview) {
                    SelectedTabIndex = (int)WizardTab.Status;
                    await LoadOvertureDataAsync();
                } else if (SelectedTabIndex == (int)WizardTab.Status || SelectedTabIndex == (int)WizardTab.CacheManagement) {
                    ResetState();
                    SelectedTabIndex = (int)WizardTab.SelectData;
                }
            }, () => !IsWorking && !_isPreviewSampling && !IsLoading);
            BackCommand = new RelayCommand(() => {
                if (SelectedTabIndex == (int)WizardTab.Preview) {
                    SelectedTabIndex = (int)WizardTab.SelectData;
                }
            }, () => !IsWorking && !_isPreviewSampling);
            CancelCommand = new RelayCommand(() =>
            {
                if (IsWorking || _isPreviewSampling)
                {
                    _cts?.Cancel();
                    StatusText = "Cancelling; waiting for the data engine to stop...";
                    return;
                }
                ResetState();
            });
            SelectAllCommand = new RelayCommand(
                () => IsSelectAllChecked = !IsSelectAllChecked,
                () => Themes != null && Themes.Any(t => t.IsSelectable || t.SubItems.Any())
            );
            ClearMapStyleCommand = new RelayCommand(() => SelectedCartographicProfile = null, () => SelectedCartographicProfile != null);
            ApplyMapStyleCommand = new RelayCommand(async () => await ApplyMapStyleToExistingLayersAsync(), () => SelectedCartographicProfile != null);
            RepairMapSymbologyCommand = new RelayCommand(async () => await RepairMapSymbologyAsync());
            InitializePreviewCommands();

            CustomExtentTool.ExtentCreatedStatic += OnExtentCreated;

            Themes = new ObservableCollection<SelectableThemeItem>();
            LogOutput = new StringBuilder();
            LogOutput.AppendLine("Initializing WizardDockpaneViewModel...");
            LogOutputText = LogOutput.ToString(); // Initialize LogOutputText

            // Set default paths immediately, they might be updated by InitializeAsync if LatestRelease changes
            var defaultBasePath = DeterminedDefaultMfcBasePath;
            DataOutputPath = Path.Combine(defaultBasePath, "Data", LatestRelease ?? "latest");            NotifyPropertyChanged(nameof(DataOutputPath)); // Notify for initial value
            IsLoading = true; // Set IsLoading to true before starting async init
            StatusText = "Initializing..."; // Initial status
            NotifyPropertyChanged(nameof(IsLoading));
            NotifyPropertyChanged(nameof(StatusText));
            _isSelectAllChecked = false; // Initialize Select All state

            // Do not call InitializeAsync() here. The base DockPane class
            // will invoke it automatically after construction. Calling it
            // explicitly would cause double initialization which leads to
            // errors such as attempting to open the DuckDB connection twice.
        }

        protected override async Task InitializeAsync()
        {
            try
            {
                // Populate UI elements first so themes are always visible,
                // even if the DuckDB connection fails to initialize.
                AddToLog("Async Initialization: Establishing release value (cached/default)");
                var cachedRelease = LoadCachedLatestRelease();
                LatestRelease = !string.IsNullOrWhiteSpace(cachedRelease) ? cachedRelease : (LatestRelease ?? "latest");
                NotifyPropertyChanged(nameof(LatestRelease));
                AddToLog($"Async Initialization: Latest release set to: {LatestRelease} (cached or default)");

                InitializeThemes();
                AddToLog("Async Initialization: Themes initialized");

                var defaultBasePath = DeterminedDefaultMfcBasePath;
                DataOutputPath = Path.Combine(defaultBasePath, "Data", LatestRelease ?? "latest");
                NotifyPropertyChanged(nameof(DataOutputPath));
                AddToLog($"Async Initialization: DataOutputPath updated to: {DataOutputPath}");

                // Initialize cache info if Pro 3.6+
                if (ArcGISProVersionHelper.IsPro36OrLater)
                {
                    RefreshCacheInfo();
                }

                // DuckDB init happens after UI is ready so a failure here
                // does not prevent the user from seeing/selecting themes.
                AddToLog("Async Initialization: Initializing DuckDB");
                await _dataProcessor.InitializeDuckDBAsync();

                // Complete discovery before enabling downloads of a cached release.
                await RefreshLatestReleaseAsync();

                StatusText = ReleaseStatusText;
                AddToLog("Async Initialization: Ready to load Overture Maps data");
            }
            catch (Exception ex)
            {
                var error = $"Async Initialization error: {ex.Message}";
                System.Diagnostics.Debug.WriteLine($"Error during async initialization: {ex}");
                StatusText = error;
                AddToLog($"ERROR: {error}");
            }
            finally
            {
                IsLoading = false;
                NotifyPropertyChanged(nameof(IsLoading));
            }
        }


        private string LoadCachedLatestRelease()
        {
            try
            {
                string cacheFile = GetLatestReleaseCacheFilePath();
                if (File.Exists(cacheFile))
                {
                    var value = File.ReadAllText(cacheFile).Trim();
                    if (!string.IsNullOrWhiteSpace(value)) return value;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"LoadCachedLatestRelease error: {ex.Message}");
            }
            return null;
        }

        private void SaveCachedLatestRelease(string release)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(release)) return;
                string cacheFile = GetLatestReleaseCacheFilePath();
                string dir = Path.GetDirectoryName(cacheFile);
                if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);
                File.WriteAllText(cacheFile, release);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"SaveCachedLatestRelease error: {ex.Message}");
            }
        }

        private string GetLatestReleaseCacheFilePath()
        {
            // Store under <Base>/config/latestRelease.txt
            var basePath = DeterminedDefaultMfcBasePath;
            return Path.Combine(basePath, "config", "latestRelease.txt");
        }

        private void UpdatePathsFromRelease()
        {
            var basePath = DeterminedDefaultMfcBasePath;
            DataOutputPath = Path.Combine(basePath, "Data", LatestRelease ?? "latest");
            NotifyPropertyChanged(nameof(DataOutputPath));
            AddToLog($"DataOutputPath updated to: {DataOutputPath}");
        }

        private async Task RefreshLatestReleaseAsync()
        {
            try
            {
                ReleaseStatusText = "Checking published releases...";
                var selection = await new OvertureReleaseDiscovery().GetLatestAsync(Environment.GetEnvironmentVariable("OVERTURE_RELEASE_URL"));
                LatestRelease = selection.Release;
                NotifyPropertyChanged(nameof(LatestRelease));
                AddToLog($"Release discovered from {selection.Source}: {LatestRelease}");
                ReleaseStatusText = $"Selected {LatestRelease} from {selection.Source}.";
                UpdatePathsFromRelease();
                SaveCachedLatestRelease(LatestRelease);
                IsManualReleaseEntryVisible = false;
            }
            catch (Exception ex)
            {
                ReleaseStatusText = "Release discovery unavailable. The displayed cached date is unverified; enter a published release and apply it.";
                AddToLog($"Failed to refresh release: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"RefreshLatestReleaseAsync error: {ex}");
                // Offer manual entry when fetch fails
                if (string.IsNullOrWhiteSpace(ManualReleaseText))
                    ManualReleaseText = LatestRelease ?? string.Empty;
                IsManualReleaseEntryVisible = true;
            }
        }

        private void ApplyManualRelease()
        {
            var input = ManualReleaseText?.Trim();
            if (string.IsNullOrWhiteSpace(input)) return;
            try { OvertureReleaseDiscovery.ValidateRelease(input); }
            catch (ArgumentException ex) { ReleaseStatusText = ex.Message; return; }
            LatestRelease = input;
            ReleaseStatusText = $"Manually selected {input}. Availability will be checked before reading data.";
            NotifyPropertyChanged(nameof(LatestRelease));
            UpdatePathsFromRelease();
            AddToLog($"Manual release applied: {LatestRelease}");
        }



        /// <summary>
        /// Provides periodic heartbeat feedback during long-running operations
        /// </summary>
        private async Task StartHeartbeatAsync(string itemName, CancellationToken cancellationToken)
        {
            int heartbeatCount = 0;
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(10000, cancellationToken); // Every 10 seconds
                    heartbeatCount++;

                    var timeElapsed = heartbeatCount * 10;
                    StatusText = $"Still loading {itemName}... ({timeElapsed}s elapsed)";
                    AddToLog($"⏱️ Still working on {itemName} ({timeElapsed} seconds elapsed)...");
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when operation completes
            }
        }

        /// <summary>
        /// Resolves the chosen area against the captured map view and projects it to WGS84.
        /// </summary>
        private async Task<Envelope> GetLoadExtentAsync(MapView targetView = null)
        {
            Envelope extent = null;
            _savedViewExtentForRestore = null;
            var view = targetView ?? MapView.Active;
            await QueuedTask.Run(() =>
            {
                SpatialReference wgs84 = SpatialReferenceBuilder.CreateSpatialReference(ExtentResolution.Wgs84Wkid);

                switch (ExtentResolution.ChooseSource(UseCurrentMapExtent, view != null, UseCustomExtent, CustomExtent != null))
                {
                    case ExtentSource.Map:
                    {
                        Envelope mapExtent = view.Extent;
                        if (mapExtent != null)
                        {
                            // Save view extent so we can restore it after adding layers (prevents zoom-out)
                            _savedViewExtentForRestore = EnvelopeBuilderEx.CreateEnvelope(mapExtent.XMin, mapExtent.YMin, mapExtent.XMax, mapExtent.YMax, mapExtent.SpatialReference);
                            if (ExtentResolution.NeedsProjectionToWgs84(mapExtent.SpatialReference?.Wkid))
                            {
                                AddToLog($"Map extent SR is {mapExtent.SpatialReference?.Wkid.ToString() ?? "null"}, projecting to WGS84 (4326).");
                                try
                                {
                                    extent = GeometryEngine.Instance.Project(mapExtent, wgs84) as Envelope;
                                    if (extent == null)
                                    {
                                        AddToLog("Warning: Map extent projection to WGS84 resulted in null. Original extent might be invalid or projection failed.");
                                        throw new InvalidOperationException("Could not project the map area to WGS84.");
                                    }
                                }
                                catch (Exception ex)
                                {
                                    AddToLog($"Error projecting map extent: {ex.Message}. Choose an area with a known coordinate system.");
                                    System.Diagnostics.Debug.WriteLine($"Error projecting map extent: {ex.Message}");
                                    throw new InvalidOperationException("Choose an area with a known coordinate system.", ex);
                                }
                            }
                            else
                            {
                                AddToLog("Map extent is already in WGS84.");
                                extent = mapExtent;
                            }
                            AddToLog($"Using WGS84 extent from map: {extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}");
                            System.Diagnostics.Debug.WriteLine($"[GetLoadExtentAsync] WGS84 extent from map (view.Extent): {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                        }
                        else
                        {
                            AddToLog("Map extent is null.");
                        }
                        break;
                    }
                    case ExtentSource.Custom:
                        extent = CustomExtent;
                        AddToLog($"Using custom WGS84 extent: {extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}");
                        System.Diagnostics.Debug.WriteLine($"Using custom WGS84 extent: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                        break;
                    default:
                        AddToLog("No extent specified or available for filtering.");
                        break;
                }
            });
            return extent;
        }

        private bool _isWorking;
        public bool IsWorking
        {
            get => _isWorking;
            private set
            {
                _isWorking = value;
                NotifyPropertyChanged(nameof(IsWorking));
                (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (NextCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (BackCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (RefreshReleaseCommand as RelayCommand)?.RaiseCanExecuteChanged();
                RaisePreviewCommandsCanExecuteChanged();
            }
        }

        private async Task LoadOvertureDataAsync()
        {
            if (IsWorking || _isPreviewSampling || IsLoading) return;
            var selected = GetSelectedLeafItems().ToList();
            if (selected.Count == 0) return;
            IsWorking = true;
            _cts = new CancellationTokenSource();
            var token = _cts.Token;
            DownloadSnapshot snapshot = null;
            try
            {
                // Capture mutable UI choices once. A release refresh or map switch cannot change this job.
                string release = LatestRelease?.Trim();
                string compression = SelectedCompression;
                bool provenance = AddSourceProvenanceFields;
                string dataDirectory = Path.GetFullPath(DataOutputPath);
                var targetView = MapView.Active;
                var targetMap = targetView?.Map;
                if (targetMap == null) throw new InvalidOperationException("Open a map before adding data.");
                var extent = await GetLoadExtentAsync(targetView);
                if (extent == null) throw new InvalidOperationException("Choose a map or custom area before downloading.");
                var bounds = new ExtentBounds(extent.XMin, extent.YMin, extent.XMax, extent.YMax);
                GeographicBounds.Validate(bounds);
                StatusText = $"Checking availability of release {release}...";
                await new OvertureReleaseDiscovery().EnsureAvailableAsync(release, token);
                snapshot = new DownloadSnapshot(dataDirectory, release, bounds, compression);
                _dataProcessor.ClearPendingLayers();
                _dataProcessor.BeginNewOutputSession();
                _dataProcessor.SelectedCartographicProfile = SelectedCartographicProfile;
                _dataProcessor.SelectedMapStyle = null;
                SelectedTabIndex = (int)WizardTab.Status;
                var progress = new Progress<string>(message => { StatusText = message; AddToLog(message); });
                await _dataProcessor.InitializeDuckDBAsync(token);
                var version = await _dataProcessor.ExecuteQueryAsync("SELECT version() AS version");
                snapshot.Manifest.EngineVersion = Convert.ToString(version[0]["version"]);
                for (int index = 0; index < selected.Count; index++)
                {
                    token.ThrowIfCancellationRequested();
                    var item = selected[index];
                    string uri = $"{S3_BASE_PATH}/{release}/theme={item.ParentThemeForS3}/type={item.ActualType}/*.parquet";
                    try
                    {
                        IReadOnlyList<string> files = null;
                        StatusText = $"Finding {item.DisplayName} files for this area...";
                        try
                        {
                            files = await new OvertureCatalog().FindFilesAsync(release, item.ParentThemeForS3,
                                item.ActualType, bounds, Path.Combine(dataDirectory, ".catalog"), token);
                            AddToLog($"Catalog selected {files.Count} files for {item.DisplayName}.");
                        }
                        catch (OperationCanceledException) { throw; }
                        catch (Exception ex)
                        {
                            AddToLog($"Catalog unavailable ({ex.Message}); using the full file listing with spatial filtering.");
                        }
                        var result = await _dataProcessor.PrepareDatasetAsync(uri, item.ParentThemeForS3,
                            item.ActualType, $"{MakeFriendlyName(item.ParentThemeForS3)} - {item.DisplayName}",
                            snapshot.StagingPath, bounds, compression, provenance, progress, token, files);
                        snapshot.Manifest.Datasets.Add(result);
                        AddToLog(result.Status == DatasetLoadStatus.Empty
                            ? $"{item.DisplayName}: no features in this area."
                            : $"{item.DisplayName}: {result.RowCount:N0} features prepared.");
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        snapshot.Manifest.Datasets.Add(new DatasetLoadResult
                        {
                            Theme = item.ParentThemeForS3, Type = item.ActualType,
                            SourceUri = uri, Status = DatasetLoadStatus.Failed, Error = ex.Message
                        });
                        AddToLog($"{item.DisplayName} failed: {ex.Message}");
                    }
                    ProgressValue = (index + 1) * 90.0 / selected.Count;
                }
                token.ThrowIfCancellationRequested();
                int failed = snapshot.Manifest.Datasets.Count(d => d.Status == DatasetLoadStatus.Failed);
                int empty = snapshot.Manifest.Datasets.Count(d => d.Status == DatasetLoadStatus.Empty);
                if (failed > 0)
                    throw new InvalidOperationException($"{failed} of {selected.Count} datasets failed. Previous data is preserved; retry the download.");
                // Commit is a short, non-cancellable publish boundary after all files validate.
                string adoptedPath = await Task.Run(() => snapshot.Commit());
                _dataProcessor.LayerManager.RelocatePendingLayers(snapshot.StagingPath, adoptedPath);
                _lastLoadedDataPath = adoptedPath;
                StatusText = "Preparing ArcGIS layers...";
                int expectedLayers = _dataProcessor.LayerManager.GetPendingLayers().Count;
                await _dataProcessor.AddAllLayersToMapAsync(progress, targetMap, adoptedPath);
                int added = _dataProcessor.LastAddedLayerCount;
                StatusText = added == expectedLayers
                    ? $"Added {added} layers from {release}; {empty} empty datasets."
                    : $"Saved data from {release}; added {added} of {expectedLayers} layers. See details to recover missing layers.";
                AddToLog(StatusText);
                AddToLog($"Snapshot: {adoptedPath}. Original data is in _source; previous snapshots remain available.");
                ProgressValue = 100;
            }
            catch (OperationCanceledException)
            {
                StatusText = "Download cancelled. Previous data is preserved.";
                AddToLog(StatusText);
            }
            catch (Exception ex)
            {
                StatusText = $"Download incomplete: {ex.Message}";
                AddToLog(StatusText);
            }
            finally
            {
                _dataProcessor.ClearPendingLayers();
                snapshot?.Dispose();
                _cts.Dispose();
                _cts = null;
                IsWorking = false;
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
            var pane = FrameworkApplication.DockPaneManager.Find(_dockPaneID) as WizardDockpaneViewModel;
            if (pane == null)
                return;

            if (!pane.IsWorking && !pane._isPreviewSampling) pane.ResetState();
            if (!pane.IsWorking) pane.SelectedTabIndex = (int)WizardTab.SelectData;
            pane.Activate();
        }

        // Cleanup method that will be called when the add-in is unloaded
        // No override needed - this will be called by the framework
        private void CleanupResources()
        {
            // Cleanup by unsubscribing from static events
            System.Diagnostics.Debug.WriteLine("WizardDockpaneViewModel cleaning up, unsubscribing from static events");
            CustomExtentTool.ExtentCreatedStatic -= OnExtentCreated;

            // Dispose of the DataProcessor (closes DuckDB connection)
            _dataProcessor?.Dispose();
            _dataProcessor = null;

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

        private void ResetState()
        {
            if (IsWorking || _isPreviewSampling) return;
            // Clear theme selections
            foreach (var themeItem in Themes)
            {
                // Temporarily unsubscribe to avoid multiple event triggers
                if (themeItem.IsSelectable) themeItem.SelectionChanged -= OnLeafThemeSelectionChanged; // Only if it's a leaf
                else foreach (var subItem in themeItem.SubItems) subItem.SelectionChanged -= OnLeafThemeSelectionChanged;

                themeItem.IsSelected = false;
                foreach (var subItem in themeItem.SubItems) subItem.IsSelected = false;

                if (themeItem.IsSelectable) themeItem.SelectionChanged += OnLeafThemeSelectionChanged; // Only if it's a leaf
                else foreach (var subItem in themeItem.SubItems) subItem.SelectionChanged += OnLeafThemeSelectionChanged;
            }

            // Clear selected themes list - no longer needed
            // NotifyPropertyChanged(nameof(SelectedThemes));

            // Reset other properties
            SelectedTheme = null;
            SelectedTabIndex = (int)WizardTab.SelectData;
            _isSelectAllChecked = false; // Explicitly reset, though UpdateIsSelectAllCheckedStatus will also do it.
            NotifyPropertyChanged(nameof(IsSelectAllChecked));

            // Reset extent options
            UseCurrentMapExtent = true;
            UseCustomExtent = false;
            CustomExtent = null;

            // Reset data and MFC options
            var defaultBasePath = DeterminedDefaultMfcBasePath;

            // Reset data options
            DataOutputPath = Path.Combine(
                defaultBasePath,
                "Data",
                LatestRelease ?? "latest"
            );
            AddSourceProvenanceFields = false;

            // Reset MFC options
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
            NotifyPropertyChanged(nameof(LogOutputText));

            // Raise can execute changed on commands
            (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (ShowThemeInfoCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (SetCustomExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (SelectAllCommand as RelayCommand)?.RaiseCanExecuteChanged();

            UpdateIsSelectAllCheckedStatus(); // Ensure Select All checkbox is correctly updated
            System.Diagnostics.Debug.WriteLine("Add-in state has been reset");
        }

        private static string MakeFriendlyName(string s3TypeName) // CA1822 Made static
        {
            if (string.IsNullOrEmpty(s3TypeName)) return s3TypeName;
            // Replace underscores with spaces and capitalize words
            var parts = s3TypeName.Split(['_'], StringSplitOptions.RemoveEmptyEntries); // IDE0300 / CA1861 Simplified array
            for (int i = 0; i < parts.Length; i++)
            {
                if (parts[i].Length > 0)
                    parts[i] = char.ToUpper(parts[i][0]) + (parts[i].Length > 1 ? parts[i][1..] : ""); // IDE0057 Substring simplified
            }
            return string.Join(" ", parts);
        }

        private void InitializeThemes()
        {
            var themesCollection = new ObservableCollection<SelectableThemeItem>();
            foreach (var kvp in _overtureS3ThemeTypes)
            {
                string s3ParentThemeKey = kvp.Key; // e.g., "base", "buildings"
                string s3SubTypesString = kvp.Value;
                string[] s3SubTypes = s3SubTypesString.Split(',');

                string parentDisplayName = _parentThemeDisplayNames.TryGetValue(s3ParentThemeKey, out var dn) ? dn : MakeFriendlyName(s3ParentThemeKey);

                // Parent item: DisplayName, ActualType (itself, for grouping), ParentS3Theme (itself)
                // A parent is a leaf (and thus selectable) if it has no distinct sub-types.
                bool parentIsLeaf = s3SubTypes.Length == 1 && s3SubTypes[0] == s3ParentThemeKey;
                // or s3SubTypes.Length == 0 (though current data always has types)

                var parentItem = new SelectableThemeItem(parentDisplayName, s3ParentThemeKey, s3ParentThemeKey, parentIsLeaf);

                if (!parentIsLeaf && s3SubTypes.Length > 0)
                {
                    foreach (var s3SubType in s3SubTypes)
                    {
                        string subTypeTrimmed = s3SubType.Trim();
                        string subItemDisplayName = MakeFriendlyName(subTypeTrimmed);
                        // Sub-item: DisplayName, ActualType=s3SubType, ParentS3Theme=s3ParentThemeKey. Sub-items are always leaves.
                        var subItem = new SelectableThemeItem(subItemDisplayName, subTypeTrimmed, s3ParentThemeKey, true);
                        subItem.Parent = parentItem; // Set the parent property for the sub-item
                        subItem.SelectionChanged += OnLeafThemeSelectionChanged; // ViewModel listens to leaves
                        parentItem.SubItems.Add(subItem);
                    }
                }
                else // Parent is a leaf node
                {
                    // Ensure its ActualType is correctly set if it was determined to be a leaf
                    if (s3SubTypes.Any()) parentItem.ActualType = s3SubTypes[0].Trim();
                    parentItem.SelectionChanged += OnLeafThemeSelectionChanged; // ViewModel listens to leaves
                }
                themesCollection.Add(parentItem);
            }
            Themes = themesCollection; // Assign to the public property
            NotifyPropertyChanged(nameof(Themes));
            UpdateIsSelectAllCheckedStatus(); // Set initial state of SelectAll checkbox
            (SelectAllCommand as RelayCommand)?.RaiseCanExecuteChanged(); // Update command state
        }

        private List<SelectableThemeItem> GetSelectedLeafItems()
        {
            var selectedLeaves = new List<SelectableThemeItem>();
            if (Themes == null) return selectedLeaves;

            Action<SelectableThemeItem> collectSelectedLeaves = null;
            collectSelectedLeaves = (item) =>
            {
                if (item.IsSelectable && item.IsSelected == true) // Only add if explicitly true
                {
                    selectedLeaves.Add(item);
                }
                foreach (var subItem in item.SubItems)
                {
                    collectSelectedLeaves(subItem); // Recurse for sub-items (though current structure is one level deep)
                }
            };

            foreach (var parentItem in Themes)
            {
                // If the parent item itself is selectable (a leaf parent like "Places")
                if (parentItem.IsSelectable && parentItem.IsSelected == true)
                {
                    selectedLeaves.Add(parentItem);
                }
                // Otherwise, or in addition if it has sub-items (which it shouldn't if it's IsSelectable=true as a leaf)
                // Check its sub-items (which are always leaves and IsSelectable=true)
                else if (parentItem.SubItems.Count > 0)
                {
                    foreach (var subItem in parentItem.SubItems)
                    {
                        if (subItem.IsSelected == true) // SubItems are leaves
                        {
                            selectedLeaves.Add(subItem);
                        }
                    }
                }
            }
            return selectedLeaves.Distinct().ToList(); // Ensure distinct items if logic paths overlap
        }

        // This method is now the primary handler for selection changes on leaf items
        private void OnLeafThemeSelectionChanged(object sender, EventArgs e)
        {
            if (_isUpdatingSelectionInternally) return; // Skip if a bulk update is in progress

            if (sender is SelectableThemeItem selectedLeafItem)
            {
                // Set this item for preview purposes, even if it's being deselected
                // The preview panel will update based on this item's state and overall selections
                SelectedItemForPreview = selectedLeafItem;
            }
            // Update combined estimates and other UI elements that depend on the full selection set
            UpdateThemePreview(); // This eventually calls UpdateIsSelectAllCheckedStatus
            (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
            NotifyPropertyChanged(nameof(SelectedLeafItemCount));
            NotifyPropertyChanged(nameof(AllSelectedLeafItemsForPreview));
            // UpdateIsSelectAllCheckedStatus(); // Explicitly call to ensure status is current
        }

        private void ExecuteSelectAllInternal(bool select)
        {
            if (Themes == null) return;

            _isUpdatingSelectionInternally = true;
            try
            {
                foreach (var themeItem in Themes)
                {
                    if (themeItem.IsSelectable) // Parent is a leaf
                    {
                        themeItem.IsSelected = select;
                    }
                    else if (themeItem.SubItems.Any()) // Parent has sub-items, set its state (will propagate)
                    {
                        themeItem.IsSelected = select; // This will trigger propagation to children
                    }
                    // No need to iterate sub-items here anymore, parent IsSelected setter handles it.

                    // Expand/Collapse parent themes based on 'select' state
                    if (themeItem.IsExpandable)
                    {
                        themeItem.IsExpanded = select; // Set to true if select is true, false if select is false
                    }
                }
            }
            finally
            {
                _isUpdatingSelectionInternally = false;
            }

            // After bulk update, the individual OnLeafThemeSelectionChanged handlers were skipped.
            // We need to manually trigger updates for dependent properties and the overall "Select All" state.
            UpdateThemePreview(); // Refreshes previews, and calls UpdateIsSelectAllCheckedStatus
            (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
            NotifyPropertyChanged(nameof(SelectedLeafItemCount));
            NotifyPropertyChanged(nameof(AllSelectedLeafItemsForPreview));
            // UpdateIsSelectAllCheckedStatus(); // Called by UpdateThemePreview indirectly, but call directly for safety
        }

        private void UpdateIsSelectAllCheckedStatus()
        {
            if (Themes == null || !Themes.Any())
            {
                // Use SetProperty to ensure UI is notified if it changes.
                SetProperty(ref _isSelectAllChecked, false, nameof(IsSelectAllChecked));
                return;
            }

            bool allDataTypesSelected = true;
            bool anySelectableLeafExists = false;

            // We need to check all actual data types (leaf nodes)
            List<SelectableThemeItem> allLeafItems = GetAllLeafDataItems();

            if (!allLeafItems.Any())
            {
                SetProperty(ref _isSelectAllChecked, false, nameof(IsSelectAllChecked));
                return;
            }

            foreach (var leafItem in allLeafItems)
            {
                anySelectableLeafExists = true; // We know it exists if allLeafItems is not empty
                if (leafItem.IsSelected != true) // Check for explicitly true
                {
                    allDataTypesSelected = false;
                    break;
                }
            }

            SetProperty(ref _isSelectAllChecked, anySelectableLeafExists && allDataTypesSelected, nameof(IsSelectAllChecked));
        }

        // Helper to get all actual data type items (leaves)
        private List<SelectableThemeItem> GetAllLeafDataItems()
        {
            var leafItems = new List<SelectableThemeItem>();
            if (Themes == null) return leafItems;

            foreach (var themeItem in Themes)
            {
                if (themeItem.IsSelectable) // It's a leaf parent (e.g., Places)
                {
                    leafItems.Add(themeItem);
                }
                // Add all sub-items, as they are always leaves/actual data types
                leafItems.AddRange(themeItem.SubItems);
            }
            return leafItems.Distinct().ToList(); // Ensure distinct if structure could somehow allow duplicates
        }

        /// <summary>
        /// Refreshes cache information display
        /// </summary>
        private void RefreshCacheInfo()
        {
            try
            {
                CacheLocation = CacheManager.GetCacheDirectory();
                CacheSize = CacheManager.GetCacheSizeString();
                CacheFileCount = CacheManager.GetCacheFileCount();
                System.Diagnostics.Debug.WriteLine($"Cache info refreshed: {CacheLocation}, {CacheSize}, {CacheFileCount} files");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error refreshing cache info: {ex.Message}");
                CacheLocation = "Error retrieving cache location";
                CacheSize = "Unknown";
                CacheFileCount = 0;
            }
        }

        /// <summary>
        /// Clears the Parquet cache
        /// </summary>
        private void ClearCache()
        {
            try
            {
                var result = ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    $"This will delete all cached Parquet files ({CacheSize}).\n\nThis action cannot be undone. Continue?",
                    "Clear Parquet Cache",
                    System.Windows.MessageBoxButton.YesNo,
                    System.Windows.MessageBoxImage.Warning);

                if (result == System.Windows.MessageBoxResult.Yes)
                {
                    bool success = CacheManager.ClearCache();
                    if (success)
                    {
                        RefreshCacheInfo();
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                            "Cache cleared successfully.",
                            "Cache Cleared",
                            System.Windows.MessageBoxButton.OK,
                            System.Windows.MessageBoxImage.Information);
                    }
                    else
                    {
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                            "Failed to clear cache. Some files may be in use by ArcGIS Pro.",
                            "Clear Cache Failed",
                            System.Windows.MessageBoxButton.OK,
                            System.Windows.MessageBoxImage.Warning);
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error clearing cache: {ex.Message}");
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    $"Error clearing cache: {ex.Message}",
                    "Error",
                    System.Windows.MessageBoxButton.OK,
                    System.Windows.MessageBoxImage.Error);
            }
        }
    }
}

