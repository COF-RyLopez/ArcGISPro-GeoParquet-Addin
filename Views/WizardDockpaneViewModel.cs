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
        private const string _dockPaneID = "DuckDBGeoparquet_Views_WizardDockpane";
        private DataProcessor _dataProcessor;
        private const string RELEASE_URL = "https://labs.overturemaps.org/data/releases.json";
        private const string S3_BASE_PATH = "s3://overturemaps-us-west-2/release";

        private const uint FILE_DELETE_ACCESS = 0x00010000;
        private const int ERROR_SHARING_VIOLATION = 32;
        private const int ERROR_LOCK_VIOLATION = 33;
        private const int ERROR_ACCESS_DENIED = 5;

        // Add CancellationTokenSource for cancelling operations
        private CancellationTokenSource _cts;
        private bool _skipExistingData;

        public PreviewBridge PreviewBridge { get; private set; }

        public void AttachPreview(Action<string> postMessage)
        {
            if (PreviewBridge == null)
            {
                PreviewBridge = new PreviewBridge(postMessage);
                PreviewBridge.LayerLoaded += (name, count) => StatusText = $"Loaded {name} in preview";
                PreviewBridge.LayerError += (name, err) => {
                    StatusText = $"Preview Error ({name}): {err}";
                    LogOutputText += $"Preview Error ({name}): {err}\\n";
                };
                PreviewBridge.PreviewUnavailable += (err) => {
                    StatusText = $"Preview Unavailable: {err}";
                    LogOutputText += $"Preview Unavailable: {err}\\n";
                };
                PreviewBridge.MapReady += () => {
                    StatusText = "Preview map ready";
                    if (PreviewShowExtentCommand is RelayCommand extCmd) extCmd.RaiseCanExecuteChanged();
                    if (PreviewSampleCommand is RelayCommand smpCmd) smpCmd.RaiseCanExecuteChanged();
                    if (PreviewClearCommand is RelayCommand clrCmd) clrCmd.RaiseCanExecuteChanged();
                };
            }
        }

        private void OnPreviewShowExtent()
        {
            if (_customExtentTool?.CurrentExtent != null)
            {
                PreviewBridge?.ShowExtent(
                    _customExtentTool.CurrentExtent.XMin,
                    _customExtentTool.CurrentExtent.YMin,
                    _customExtentTool.CurrentExtent.XMax,
                    _customExtentTool.CurrentExtent.YMax);
            }
        }

        private async Task OnPreviewSampleAsync()
        {
            if (PreviewBridge == null || !PreviewBridge.IsMapReady)
            {
                StatusText = "Preview map not ready.";
                return;
            }

            try
            {
                StatusText = "Generating preview sample...";
                ProgressValue = 10;
                
                await _dataProcessor.InitializeDuckDBAsync(_cts.Token);
                
                ExtentBounds extent = null;
                if (UseCustomExtent && _customExtentTool?.CurrentExtent != null)
                {
                    extent = new ExtentBounds
                    {
                        XMin = _customExtentTool.CurrentExtent.XMin,
                        YMin = _customExtentTool.CurrentExtent.YMin,
                        XMax = _customExtentTool.CurrentExtent.XMax,
                        YMax = _customExtentTool.CurrentExtent.YMax
                    };
                }

                var sampleItem = GetSelectedLeafItems().FirstOrDefault();
                if (sampleItem != null)
                {
                    string s3Path = $"{S3_BASE_PATH}/{LatestRelease}/theme={sampleItem.ParentThemeForS3}/type={sampleItem.ActualType}/*";
                    string tempGeoJson = Path.Combine(Path.GetTempPath(), $"preview_{Guid.NewGuid():N}.geojson");
                    
                    await _dataProcessor.ExportPreviewSampleAsync(s3Path, tempGeoJson, extent, 2000, _cts.Token);
                    
                    if (File.Exists(tempGeoJson))
                    {
                        string geojson = await File.ReadAllTextAsync(tempGeoJson);
                        PreviewBridge.AddGeoJsonLayer(sampleItem.DisplayName, geojson);
                        File.Delete(tempGeoJson);
                    }
                }
                
                StatusText = "Preview sample loaded.";
                ProgressValue = 100;
            }
            catch (Exception ex)
            {
                StatusText = $"Preview failed: {ex.Message}";
                LogOutputText += $"[Error] {ex}\\n";
            }
        }

        public void NotifyPreviewInitFailed(string message)
        {
            StatusText = $"Preview failed to initialize: {message}";
            LogOutputText += $"WebView2 Error: {message}\\n";
        }

        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern SafeFileHandle CreateFileW(
            string lpFileName,
            uint dwDesiredAccess,
            FileShare dwShareMode,
            IntPtr lpSecurityAttributes,
            FileMode dwCreationDisposition,
            FileAttributes dwFlagsAndAttributes,
            IntPtr hTemplateFile);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool DeleteFileW(string lpFileName);

        // Centralized logic for Default MFC Base Path
        private static string DeterminedDefaultMfcBasePath
        {
            get
            {
                try
                {
                    var project = Project.Current;
                    if (project != null && !string.IsNullOrEmpty(project.HomeFolderPath))
                    {
                        // Use the project's Home Folder Path
                        return Path.Combine(project.HomeFolderPath, AddinConstants.DataSubfolder);
                    }
                    // Fallback if HomeFolderPath is not available but project path is (less ideal)
                    else if (project != null && !string.IsNullOrEmpty(project.Path))
                    {
                        string projectDir = Path.GetDirectoryName(project.Path);
                        if (!string.IsNullOrEmpty(projectDir))
                            return Path.Combine(projectDir, AddinConstants.DataSubfolder);
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error getting project home/path for DefaultMfcBasePath: {ex.Message}");
                }
                // Fallback to MyDocuments if project path cannot be determined
                return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), AddinConstants.DataSubfolder);
            }
        }

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

            LoadDataCommand = new RelayCommand(async () => await LoadOvertureDataAsync(), () => GetSelectedLeafItems().Count > 0);
            RefreshCacheInfoCommand = new RelayCommand(() => RefreshCacheInfo());
            ClearCacheCommand = new RelayCommand(() => ClearCache(), () => ArcGISProVersionHelper.IsPro36OrLater);
            ShowThemeInfoCommand = new RelayCommand(() => ShowThemeInfo(), () => SelectedItemForPreview != null);
            SetCustomExtentCommand = new RelayCommand(() => SetCustomExtent(), () => UseCustomExtent);            BrowseDataLocationCommand = new RelayCommand(() => BrowseDataLocation());            ApplyManualReleaseCommand = new RelayCommand(() => ApplyManualRelease(), () => !string.IsNullOrWhiteSpace(ManualReleaseText));
            CancelCommand = new RelayCommand(() =>
            {
                if (_cts != null && !_cts.IsCancellationRequested) { _cts.Cancel(); AddToLog("Operation cancelled by user."); }
                ResetState(); AddToLog("Add-in state has been reset.");
                try { this.Hide(); } catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"Error closing dockpane: {ex.Message}"); }
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

                // Refresh latest release in background without blocking UI
                _ = RefreshLatestReleaseAsync();

                StatusText = "Ready to load Overture Maps data";
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

        // Shared client for release checks: system proxy, default credentials, short timeout.
        private static readonly HttpClient _releaseHttpClient = new(new HttpClientHandler
        {
            UseProxy = true,
            Proxy = WebRequest.DefaultWebProxy,
            DefaultProxyCredentials = CredentialCache.DefaultCredentials
        })
        {
            Timeout = TimeSpan.FromSeconds(3)
        };

        private async Task RefreshLatestReleaseAsync()
        {
            try
            {
                var url = Environment.GetEnvironmentVariable("OVERTURE_RELEASE_URL") ?? RELEASE_URL;
                var response = await _releaseHttpClient.GetStringAsync(url);
                var releaseInfo = JsonSerializer.Deserialize<ReleaseInfo>(response, _jsonOptions);
                var latest = releaseInfo?.Latest;
                if (!string.IsNullOrWhiteSpace(latest) && latest != LatestRelease)
                {
                    LatestRelease = latest;
                    NotifyPropertyChanged(nameof(LatestRelease));
                    AddToLog($"Latest release refreshed: {LatestRelease}");
                    UpdatePathsFromRelease();
                    SaveCachedLatestRelease(LatestRelease);
                    IsManualReleaseEntryVisible = false; // hide if previously shown
                }
            }
            catch (Exception ex)
            {
                // Non-blocking: log and continue
                AddToLog($"Failed to refresh latest release (non-blocking): {ex.Message}");
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
            LatestRelease = input;
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
        /// Performs bulk folder deletion and layer removal asynchronously to prevent UI blocking
        /// </summary>
        private async Task PerformBulkDataReplacementAsync(List<SelectableThemeItem> selectedItems, string dataPath)
        {
            try
            {
                StatusText = "Removing existing layers from map...";

                // First, collect all unique actualS3Type folders that need to be deleted
                var typeFoldersToDelete = selectedItems
                    .Select(item => Path.Combine(dataPath, item.ActualType))
                    .Where(Directory.Exists)
                    .Distinct()
                    .ToList();

                AddToLog($"Found {typeFoldersToDelete.Count} theme folders to clean up");

                if (typeFoldersToDelete.Count == 0)
                {
                    return; // Nothing to delete
                }

                // Remove layers that use files from these folders asynchronously
                for (int i = 0; i < typeFoldersToDelete.Count; i++)
                {
                    var folderPath = typeFoldersToDelete[i];
                    var folderName = Path.GetFileName(folderPath);

                    StatusText = $"Removing layers for {folderName} ({i + 1}/{typeFoldersToDelete.Count})...";
                    AddToLog($"Removing layers using data from folder: {folderName}");

                    // Remove layers asynchronously with proper UI thread handling
                    await RemoveLayersUsingFolderAsync(folderPath);

                    // Small delay to allow UI updates
                    await Task.Delay(100);
                }

                // ArcGIS may still hold file handles briefly after layer removal/disposal.
                // Force a quick GC cycle and short pause before deleting files/folders.
                await Task.Run(() =>
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                });
                await Task.Delay(1000);

                StatusText = "Deleting existing data folders...";
                AddToLog("All layers removed. Now deleting data folders...");

                // Best-effort cleanup of old parquet files.
                // With per-load unique output filenames, full recursive deletion is no longer required.
                for (int i = 0; i < typeFoldersToDelete.Count; i++)
                {
                    var folderPath = typeFoldersToDelete[i];
                    var folderName = Path.GetFileName(folderPath);

                    StatusText = $"Cleaning old files in {folderName} ({i + 1}/{typeFoldersToDelete.Count})...";
                    AddToLog($"Cleaning old files in folder: {folderName}");

                    // Delete unlocked files quickly; skip locked files and continue.
                    await Task.Run(() =>
                    {
                        try
                        {
                            if (!Directory.Exists(folderPath))
                                return;

                            int deletedFiles = 0;
                            int lockedFiles = 0;
                            int failedFiles = 0;

                            foreach (var file in Directory.EnumerateFiles(folderPath, "*.parquet", SearchOption.AllDirectories))
                            {
                                var deleteResult = TryDeleteParquetFileQuiet(file);
                                switch (deleteResult)
                                {
                                    case FileDeleteResult.Deleted:
                                        deletedFiles++;
                                        break;
                                    case FileDeleteResult.Locked:
                                        lockedFiles++;
                                        break;
                                    default:
                                        failedFiles++;
                                        break;
                                }
                            }

                            // Remove now-empty subdirectories.
                            foreach (var dir in Directory.GetDirectories(folderPath, "*", SearchOption.AllDirectories)
                                .OrderByDescending(d => d.Length))
                            {
                                try
                                {
                                    if (!Directory.EnumerateFileSystemEntries(dir).Any())
                                        Directory.Delete(dir);
                                }
                                catch
                                {
                                    // Best effort only.
                                }
                            }

                            System.Diagnostics.Debug.WriteLine($"Cleanup {folderPath}: deleted {deletedFiles} parquet file(s), skipped {lockedFiles} locked file(s), failed {failedFiles} file(s).");
                            if (lockedFiles > 0)
                            {
                                AddToLog($"Skipped {lockedFiles} locked file(s) in {folderName}; they will be cleaned when released.");
                            }
                            if (failedFiles > 0)
                            {
                                AddToLog($"Could not delete {failedFiles} file(s) in {folderName} due to non-lock errors.");
                            }
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine($"Error cleaning folder {folderPath}: {ex.Message}");
                        }
                    });

                    // Small delay to allow UI updates
                    await Task.Delay(50);
                }

                // Clear ArcGIS Pro Parquet cache to prevent stale metadata/extents from reused file paths.
                StatusText = "Clearing ArcGIS Pro Parquet cache...";
                AddToLog("Clearing ArcGIS Pro Parquet cache to avoid stale layer metadata...");
                bool cacheCleared = await Task.Run(() => CacheManager.ClearCache());
                if (cacheCleared)
                {
                    AddToLog("ArcGIS Pro Parquet cache cleared.");
                }
                else
                {
                    AddToLog("Could not fully clear ArcGIS Pro Parquet cache (some files may be in use).");
                }
                RefreshCacheInfo();

                StatusText = "Data cleanup completed. Ready to load new data...";
                AddToLog("Bulk folder deletion completed successfully");
            }
            catch (Exception ex)
            {
                AddToLog($"Warning during bulk cleanup: {ex.Message}");
                StatusText = "Cleanup completed with warnings. Continuing with data load...";
                System.Diagnostics.Debug.WriteLine($"Error in PerformBulkDataReplacementAsync: {ex.Message}");
            }
        }

        private enum FileDeleteResult
        {
            Deleted,
            Locked,
            Failed
        }

        private static FileDeleteResult TryDeleteParquetFileQuiet(string filePath)
        {
            if (!File.Exists(filePath))
            {
                return FileDeleteResult.Deleted;
            }

            using (var handle = CreateFileW(
                filePath,
                FILE_DELETE_ACCESS,
                FileShare.None,
                IntPtr.Zero,
                FileMode.Open,
                FileAttributes.Normal,
                IntPtr.Zero))
            {
                if (handle.IsInvalid)
                {
                    int openError = Marshal.GetLastWin32Error();
                    return IsLockWin32Error(openError) ? FileDeleteResult.Locked : FileDeleteResult.Failed;
                }
            }

            if (DeleteFileW(filePath))
            {
                return FileDeleteResult.Deleted;
            }

            int deleteError = Marshal.GetLastWin32Error();
            return IsLockWin32Error(deleteError) ? FileDeleteResult.Locked : FileDeleteResult.Failed;
        }

        private static bool IsLockWin32Error(int errorCode)
        {
            return errorCode == ERROR_SHARING_VIOLATION
                   || errorCode == ERROR_LOCK_VIOLATION
                   || errorCode == ERROR_ACCESS_DENIED;
        }

        /// <summary>
        /// Removes all layers that use files from the specified folder
        /// </summary>
        private async Task RemoveLayersUsingFolderAsync(string folderPath)
        {
            await QueuedTask.Run(() =>
            {
                try
                {
                    var map = MapView.Active?.Map;
                    if (map == null) return;

                    var membersToRemove = new List<MapMember>();
                    var allLayers = map.GetLayersAsFlattenedList().ToList();
                    var allTables = map.GetStandaloneTablesAsFlattenedList().ToList();

                    string normalizedFolderPath = Path.GetFullPath(folderPath).ToLowerInvariant();
                    if (!normalizedFolderPath.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal))
                    {
                        normalizedFolderPath += Path.DirectorySeparatorChar;
                    }

                    // Normalize datasource paths to actual parquet file path when possible.
                    static string NormalizeToParquetFilePath(string rawPath)
                    {
                        if (string.IsNullOrWhiteSpace(rawPath)) return null;
                        try
                        {
                            string fullPath = Path.GetFullPath(rawPath);
                            string lowerPath = fullPath.ToLowerInvariant();
                            int parquetIndex = lowerPath.IndexOf(".parquet", StringComparison.OrdinalIgnoreCase);
                            if (parquetIndex >= 0)
                            {
                                return fullPath[..(parquetIndex + ".parquet".Length)].ToLowerInvariant();
                            }
                            return fullPath.ToLowerInvariant();
                        }
                        catch
                        {
                            return null;
                        }
                    }

                    // Helper to check if a file path is within the target folder
                    bool IsFileInTargetFolder(string filePath)
                    {
                        if (string.IsNullOrEmpty(filePath)) return false;
                        string normalizedFilePath = NormalizeToParquetFilePath(filePath);
                        return !string.IsNullOrEmpty(normalizedFilePath) &&
                               normalizedFilePath.StartsWith(normalizedFolderPath, StringComparison.OrdinalIgnoreCase);
                    }

                    // Check layers
                    foreach (var layer in allLayers)
                    {
                        if (layer is FeatureLayer featureLayer)
                        {
                            try
                            {
                                using var fc = featureLayer.GetFeatureClass();
                                if (fc != null)
                                {
                                    var fcPathUri = fc.GetPath();
                                    if (fcPathUri != null)
                                    {
                                        string rawPath = fcPathUri.IsFile ? fcPathUri.LocalPath : fcPathUri.OriginalString;
                                        if (IsFileInTargetFolder(rawPath))
                                        {
                                            membersToRemove.Add(featureLayer);
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                System.Diagnostics.Debug.WriteLine($"Error checking layer '{featureLayer.Name}': {ex.Message}");
                            }
                        }
                    }

                    // Check standalone tables
                    foreach (var tableMember in allTables)
                    {
                        if (tableMember is StandaloneTable standaloneTable)
                        {
                            try
                            {
                                using var tbl = standaloneTable.GetTable();
                                if (tbl != null)
                                {
                                    var tblPathUri = tbl.GetPath();
                                    if (tblPathUri != null)
                                    {
                                        string rawPath = tblPathUri.IsFile ? tblPathUri.LocalPath : tblPathUri.OriginalString;
                                        if (IsFileInTargetFolder(rawPath))
                                        {
                                            membersToRemove.Add(standaloneTable);
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                System.Diagnostics.Debug.WriteLine($"Error checking table '{standaloneTable.Name}': {ex.Message}");
                            }
                        }
                    }

                    // Remove the identified members
                    if (membersToRemove.Count > 0)
                    {
                        var distinctMembersToRemove = membersToRemove.Distinct().ToList();
                        System.Diagnostics.Debug.WriteLine($"Removing {distinctMembersToRemove.Count} map members for folder: {folderPath}");

                        foreach (var member in distinctMembersToRemove)
                        {
                            if (member is Layer layerToRemove)
                            {
                                map.RemoveLayer(layerToRemove);
                                (layerToRemove as IDisposable)?.Dispose();
                            }
                            else if (member is StandaloneTable tableToRemove)
                            {
                                map.RemoveStandaloneTable(tableToRemove);
                                (tableToRemove as IDisposable)?.Dispose();
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error in RemoveLayersUsingFolderAsync: {ex.Message}");
                }
            });
        }

        private async Task<Envelope> GetLoadExtentAsync()
        {
            Envelope extent = null;
            _savedViewExtentForRestore = null;
            await QueuedTask.Run(() =>
            {
                SpatialReference wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);

                if (UseCurrentMapExtent && MapView.Active != null)
                {
                    Envelope mapExtent = MapView.Active.Extent;
                    if (mapExtent != null)
                    {
                        // Save view extent so we can restore it after adding layers (prevents zoom-out)
                        _savedViewExtentForRestore = EnvelopeBuilderEx.CreateEnvelope(mapExtent.XMin, mapExtent.YMin, mapExtent.XMax, mapExtent.YMax, mapExtent.SpatialReference);
                        if (mapExtent.SpatialReference == null || mapExtent.SpatialReference.Wkid != 4326)
                        {
                            AddToLog($"Map extent SR is {mapExtent.SpatialReference?.Wkid.ToString() ?? "null"}, projecting to WGS84 (4326).");
                            try
                            {
                                extent = GeometryEngine.Instance.Project(mapExtent, wgs84) as Envelope;
                                if (extent == null)
                                {
                                    AddToLog("Warning: Map extent projection to WGS84 resulted in null. Original extent might be invalid or projection failed.");
                                    extent = mapExtent;
                                }
                            }
                            catch (Exception ex)
                            {
                                AddToLog($"Error projecting map extent: {ex.Message}. Using original extent values, which might be incorrect for filtering.");
                                System.Diagnostics.Debug.WriteLine($"Error projecting map extent: {ex.Message}");
                                extent = mapExtent;
                            }
                        }
                        else
                        {
                            AddToLog("Map extent is already in WGS84.");
                            extent = mapExtent;
                        }
                        AddToLog($"Using WGS84 extent from map: {extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}");
                        System.Diagnostics.Debug.WriteLine($"[GetLoadExtentAsync] WGS84 extent from map (MapView.Active.Extent): {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                    }
                    else
                    {
                        AddToLog("Map extent is null.");
                    }
                }
                else if (UseCustomExtent && CustomExtent != null)
                {
                    extent = CustomExtent;
                    AddToLog($"Using custom WGS84 extent: {extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}");
                    System.Diagnostics.Debug.WriteLine($"Using custom WGS84 extent: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");
                }
                else
                {
                    AddToLog("No extent specified or available for filtering.");
                }
            });
            return extent;
        }

        private async Task<bool> CheckAndReplaceExistingDataAsync(List<SelectableThemeItem> selectedItems, string dataPath)
        {
            _skipExistingData = false;
            bool existingDataFound = false;

            if (Directory.Exists(dataPath))
            {
                foreach (var selectedItem in selectedItems)
                {
                    var actualTypeSpecificDataPath = Path.Combine(dataPath, selectedItem.ActualType);
                    if (Directory.Exists(actualTypeSpecificDataPath) && Directory.EnumerateFiles(actualTypeSpecificDataPath, "*.parquet").Any())
                    {
                        existingDataFound = true;
                        break;
                    }
                }
            }

            if (existingDataFound)
            {
                var confirmResult = ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                    "Existing data found for one or more selected themes.\n\nYes = Replace existing files\nNo = Keep existing files and skip already-downloaded themes\nCancel = Abort",
                    "Replace or Skip Existing Data?",
                    System.Windows.MessageBoxButton.YesNoCancel,
                    System.Windows.MessageBoxImage.Warning);

                if (confirmResult == System.Windows.MessageBoxResult.Cancel)
                {
                    StatusText = "Operation cancelled by user";
                    AddToLog("Operation cancelled - user chose to cancel when existing data was detected");
                    return false;
                }

                if (confirmResult == System.Windows.MessageBoxResult.No)
                {
                    _skipExistingData = true;
                    AddToLog("User chose to keep existing data; will skip already-downloaded themes");
                    StatusText = "Keeping existing data; skipping already-downloaded themes";
                    return true;
                }

                // Yes = replace
                AddToLog("User confirmed replacing existing data");

                StatusText = "Preparing to replace existing data...";
                AddToLog("Removing existing layers and deleting theme folders...");
                await PerformBulkDataReplacementAsync(selectedItems, dataPath);
                AddToLog("Existing data cleanup completed. Beginning new data loading...");
            }

            return true;
        }

        private async Task<bool> LoadAndCreateLayersForItemAsync(
            SelectableThemeItem item, int itemIndex, int totalCount,
            int processedCount, Envelope extent, CancellationToken ct)
        {
            string parentS3Theme = item.ParentThemeForS3;
            string actualS3Type = item.ActualType;
            string itemDisplayName = item.DisplayName;

            if (_skipExistingData)
            {
                var existingFolder = Path.Combine(DataOutputPath, actualS3Type);
                if (Directory.Exists(existingFolder) && Directory.EnumerateFiles(existingFolder, "*.parquet").Any())
                {
                    AddToLog($"Skipping {itemDisplayName} (existing data kept per user choice)");
                    StatusText = $"Skipping {itemDisplayName} ({processedCount + 1}/{totalCount})";
                    ProgressValue = ((processedCount + 1) * 100.0) / totalCount;
                    return true;
                }
            }

            StatusText = $"Processing {itemIndex + 1} of {totalCount}: {MakeFriendlyName(parentS3Theme)} / {itemDisplayName}";
            AddToLog($"Processing: {MakeFriendlyName(parentS3Theme)} / {itemDisplayName}");
            AddToLog($"Data type for S3: theme='{parentS3Theme}', type='{actualS3Type}'");
            System.Diagnostics.Debug.WriteLine($"Data type for S3: theme='{parentS3Theme}', type='{actualS3Type}'");

            string trimmedRelease = LatestRelease?.Trim() ?? "";
            string s3Path = trimmedRelease.Length > 0
                ? $"{S3_BASE_PATH}/{trimmedRelease}/theme={parentS3Theme}/type={actualS3Type}/*.parquet"
                : $"{S3_BASE_PATH}/theme={parentS3Theme}/type={actualS3Type}/*.parquet";

            AddToLog($"Loading from S3 path: {s3Path}");
            System.Diagnostics.Debug.WriteLine($"Loading from S3 path: {s3Path}");

            await Task.Delay(50);

            var ingestProgressReporter = new Progress<string>(status =>
            {
                StatusText = status;
                AddToLog(status);
                var baseProgress = (processedCount * 100.0) / totalCount;
                var ingestProgress = 1.5;
                ProgressValue = Math.Min(baseProgress + ingestProgress, 98.0);
            });

            using var heartbeatCts = new CancellationTokenSource();
            var heartbeatTask = StartHeartbeatAsync(itemDisplayName, heartbeatCts.Token);

            StatusText = $"Loading {itemDisplayName} from S3 (this may take 30-60 seconds)...";
            AddToLog($"⏳ Starting S3 data load for {itemDisplayName} - please wait, this operation may take time...");

            bool ingestSuccess = await _dataProcessor.IngestFileAsync(s3Path, extent == null ? null : new ExtentBounds(extent.XMin, extent.YMin, extent.XMax, extent.YMax), actualS3Type, ingestProgressReporter, ct);

            heartbeatCts.Cancel();
            try { await heartbeatTask; } catch (OperationCanceledException) { }

            if (!ingestSuccess)
            {
                AddToLog($"❌ Failed to ingest data from {s3Path}");
                StatusText = $"Error loading data from {s3Path}";
                return false;
            }

            AddToLog($"✅ Successfully loaded {itemDisplayName} data from S3");

            if (ct.IsCancellationRequested)
                return false;

            await Task.Delay(50);

            string featureLayerName = $"{MakeFriendlyName(parentS3Theme)} - {itemDisplayName}";
            StatusText = $"Creating layers for {itemDisplayName}...";
            AddToLog($"🔄 Creating feature layers for {itemDisplayName}...");

            var itemProgressReporter = new Progress<string>(status =>
            {
                StatusText = status;
                AddToLog(status);
                var baseProgress = (processedCount * 100.0) / totalCount;
                var itemProgress = 3.0;
                ProgressValue = Math.Min(baseProgress + itemProgress, 99.0);
            });

            await _dataProcessor.CreateFeatureLayerAsync(featureLayerName, itemProgressReporter, parentS3Theme, actualS3Type, DataOutputPath, SelectedCompression, ct);

            ProgressValue = ((processedCount + 1) * 100.0) / totalCount;

            StatusText = $"✅ Completed {itemDisplayName} ({processedCount + 1}/{totalCount})";
            AddToLog($"✅ Successfully loaded {itemDisplayName} for {MakeFriendlyName(parentS3Theme)}");

            await Task.Delay(100);

            return true;
        }

        private async Task LoadOvertureDataAsync()
        {
            try
            {
                var selectedLeafItems = GetSelectedLeafItems();
                if (selectedLeafItems.Count == 0)
                {
                    AddToLog("No themes or sub-themes selected.");
                    return;
                }

                _cts?.Dispose();
                _cts = new CancellationTokenSource();
                var cancellationToken = _cts.Token;

                SelectedTabIndex = (int)WizardTab.Status;
                StatusText = $"Loading {selectedLeafItems.Count} selected data types...";
                AddToLog($"Starting to load {selectedLeafItems.Count} data type(s) from release {LatestRelease}");
                DateTime loadStartUtc = DateTime.UtcNow;
                Envelope extent = await GetLoadExtentAsync();

                if (cancellationToken.IsCancellationRequested) return;

                string dataPath = DataOutputPath;
                if (!await CheckAndReplaceExistingDataAsync(selectedLeafItems, dataPath)) return;

                _dataProcessor.BeginNewOutputSession();
                _dataProcessor.SelectedCartographicProfile = SelectedCartographicProfile;
                _dataProcessor.SelectedMapStyle = null;

                int totalDataTypesToProcess = selectedLeafItems.Count;
                int processedDataTypes = 0;

                for (int itemIndex = 0; itemIndex < selectedLeafItems.Count; itemIndex++)
                {
                    if (cancellationToken.IsCancellationRequested) return;

                    bool success = await LoadAndCreateLayersForItemAsync(
                        selectedLeafItems[itemIndex], itemIndex, totalDataTypesToProcess,
                        processedDataTypes, extent, cancellationToken);

                    if (success) processedDataTypes++;
                }

                if (cancellationToken.IsCancellationRequested) return;

                StatusText = "Adding layers to map in optimal stacking order...";
                AddToLog("🗺️ All data exported successfully. Now adding layers to map with optimal stacking order...");

                await Task.Delay(100);

                var progressReporter = new Progress<string>(status =>
                {
                    StatusText = status;
                    AddToLog(status);
                });

                await _dataProcessor.AddAllLayersToMapAsync(progressReporter);

                if (_savedViewExtentForRestore != null)
                {
                    await QueuedTask.Run(() =>
                    {
                        if (MapView.Active != null)
                        {
                            MapView.Active.ZoomTo(_savedViewExtentForRestore);
                        }
                        _savedViewExtentForRestore = null;
                    });
                }

                _lastLoadedDataPath = DataOutputPath;

                var loadElapsed = DateTime.UtcNow - loadStartUtc;
                string summaryStyle = SelectedCartographicProfile?.DisplayName ?? "Default";
                string summaryExtent = extent != null
                    ? $"{extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}"
                    : "none";
                AddToLog($"Load summary: layers={_dataProcessor.LastAddedLayerCount}, map purpose={summaryStyle}, elapsed={loadElapsed:hh\\:mm\\:ss}, extent={summaryExtent}");

                StatusText = $"Successfully loaded all selected themes from release {LatestRelease}";
                AddToLog("All selected themes loaded successfully");
                AddToLog("🚀 Data loaded successfully");
                AddToLog("----------------");
                if (extent != null)
                {
                    AddToLog($"Data was loaded for this extent only: {extent.XMin:F2}, {extent.YMin:F2}, {extent.XMax:F2}, {extent.YMax:F2}");
                    AddToLog("When you load data for a different extent, the existing data will be replaced.");
                    AddToLog("This ensures a clean folder structure for MFC creation.");
                }
                AddToLog("----------------");
                ProgressValue = 100;
            }

            catch (Exception ex)
            {
                // Determine if this is a file access issue
                bool isFileAccessError = ex.Message.Contains("because it is being used by another process") ||
                                         ex.Message.Contains("access") ||
                                         ex.Message.Contains("denied") ||
                                         ex.Message.Contains("locked");

                if (isFileAccessError)
                {
                    StatusText = "File access error";
                    AddToLog($"ERROR: File access error. One or more files are locked by another process.");
                    AddToLog($"Try the following:");
                    AddToLog($"1. Close any other ArcGIS Pro projects that might be using this data");
                    AddToLog($"2. Remove any layers from your current map that use Overture data");
                    AddToLog($"3. In extreme cases, restart ArcGIS Pro and try again");
                    AddToLog($"Detailed error: {ex.Message}");
                }
                else
                {
                    StatusText = $"Error loading data: {ex.Message}";
                    AddToLog($"ERROR: {ex.Message}");
                    AddToLog($"Stack trace: {ex.StackTrace}");
                }

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

            pane.ResetState();
            pane.SelectedTabIndex = (int)WizardTab.SelectData;
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

