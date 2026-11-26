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
        #region Properties
        private string _latestRelease;
        public string LatestRelease
        {
            get => _latestRelease;
            set => SetProperty(ref _latestRelease, value);
        }

        private string _releaseStatusText = "Checking published releases...";
        public string ReleaseStatusText
        {
            get => _releaseStatusText;
            set => SetProperty(ref _releaseStatusText, value);
        }

        private bool _isManualReleaseEntryVisible;
        public bool IsManualReleaseEntryVisible
        {
            get => _isManualReleaseEntryVisible;
            set => SetProperty(ref _isManualReleaseEntryVisible, value);
        }

        private string _manualReleaseText;
        public string ManualReleaseText
        {
            get => _manualReleaseText;
            set
            {
                if (SetProperty(ref _manualReleaseText, value))
                {
                    (ApplyManualReleaseCommand as RelayCommand)?.RaiseCanExecuteChanged();
                }
            }
        }

        private ObservableCollection<SelectableThemeItem> _themes;
        public ObservableCollection<SelectableThemeItem> Themes
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
                (RefreshReleaseCommand as RelayCommand)?.RaiseCanExecuteChanged();
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
            set
            {
                if (SetProperty(ref _selectedTabIndex, value))
                {
                    UpdateNavigation();
                    NotifyPropertyChanged(nameof(IsSelectDataTab));
                }
            }
        }

        public bool IsSelectDataTab => SelectedTabIndex == (int)WizardTab.SelectData;

        private string _nextButtonText = "Preview >";
        public string NextButtonText
        {
            get => _nextButtonText;
            set => SetProperty(ref _nextButtonText, value);
        }

        private bool _isNextButtonVisible = true;
        public bool IsNextButtonVisible
        {
            get => _isNextButtonVisible;
            set => SetProperty(ref _isNextButtonVisible, value);
        }

        private bool _isBackButtonVisible = false;
        public bool IsBackButtonVisible
        {
            get => _isBackButtonVisible;
            set => SetProperty(ref _isBackButtonVisible, value);
        }

        private void UpdateNavigation()
        {
            switch ((WizardTab)SelectedTabIndex)
            {
                case WizardTab.SelectData:
                    IsBackButtonVisible = false;
                    IsNextButtonVisible = true;
                    NextButtonText = "Preview >";
                    break;
                case WizardTab.Preview:
                    IsBackButtonVisible = true;
                    IsNextButtonVisible = true;
                    NextButtonText = "Download Data";
                    break;
                case WizardTab.Status:
                    IsBackButtonVisible = false;
                    IsNextButtonVisible = true;
                    NextButtonText = "Finish";
                    break;
                case WizardTab.CacheManagement:
                    IsBackButtonVisible = true;
                    IsNextButtonVisible = true;
                    NextButtonText = "Finish";
                    break;
            }
        }

        public bool IsCacheManagementTabVisible => ArcGISProVersionHelper.IsPro36OrLater;

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
                if (value) ClearOtherExtentSources();
                RaiseExtentSourceCommandsCanExecuteChanged();
            }
        }

        /// <summary>
        /// Keeps the six AOI radio options mutually exclusive. Setting a flag
        /// to false never cascades (each setter only clears others on true), so
        /// this cannot recurse.
        /// </summary>
        private void ClearOtherExtentSources([CallerMemberName] string keep = null)
        {
            if (keep != nameof(UseCurrentMapExtent)) UseCurrentMapExtent = false;
            if (keep != nameof(UseCustomExtent)) UseCustomExtent = false;
            if (keep != nameof(UseTypedExtent)) UseTypedExtent = false;
            if (keep != nameof(UseLayerExtent)) UseLayerExtent = false;
            if (keep != nameof(UsePolygonExtent)) UsePolygonExtent = false;
            if (keep != nameof(UseSelectedFeatureExtent)) UseSelectedFeatureExtent = false;
        }

        private void RaiseExtentSourceCommandsCanExecuteChanged()
        {
            (SetCustomExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (DrawPolygonCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (UseSelectedFeaturesCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        private string _attributeFilter = "";
        /// <summary>Optional WHERE-clause fragment applied to downloads and
        /// previews. Validated by GeoParquetSql.ValidateAttributeFilter before
        /// any query runs.</summary>
        public string AttributeFilter
        {
            get => _attributeFilter;
            set => SetProperty(ref _attributeFilter, value);
        }

        private bool _useCustomExtent = false;
        public bool UseCustomExtent
        {
            get => _useCustomExtent;
            set
            {
                SetProperty(ref _useCustomExtent, value);
                if (value) ClearOtherExtentSources();
                RaiseExtentSourceCommandsCanExecuteChanged();
            }
        }

        private bool _useTypedExtent;
        /// <summary>Typed WGS84 bounding box (four coordinate fields).</summary>
        public bool UseTypedExtent
        {
            get => _useTypedExtent;
            set
            {
                SetProperty(ref _useTypedExtent, value);
                if (value) ClearOtherExtentSources();
                RaiseExtentSourceCommandsCanExecuteChanged();
            }
        }

        private string _typedExtentWest = "";
        public string TypedExtentWest
        {
            get => _typedExtentWest;
            set { if (SetProperty(ref _typedExtentWest, value ?? "")) NotifyPropertyChanged(nameof(TypedExtentStatus)); }
        }

        private string _typedExtentSouth = "";
        public string TypedExtentSouth
        {
            get => _typedExtentSouth;
            set { if (SetProperty(ref _typedExtentSouth, value ?? "")) NotifyPropertyChanged(nameof(TypedExtentStatus)); }
        }

        private string _typedExtentEast = "";
        public string TypedExtentEast
        {
            get => _typedExtentEast;
            set { if (SetProperty(ref _typedExtentEast, value ?? "")) NotifyPropertyChanged(nameof(TypedExtentStatus)); }
        }

        private string _typedExtentNorth = "";
        public string TypedExtentNorth
        {
            get => _typedExtentNorth;
            set { if (SetProperty(ref _typedExtentNorth, value ?? "")) NotifyPropertyChanged(nameof(TypedExtentStatus)); }
        }

        /// <summary>Live validation feedback for the typed bbox fields.</summary>
        public string TypedExtentStatus
        {
            get
            {
                if (string.IsNullOrWhiteSpace(TypedExtentWest) && string.IsNullOrWhiteSpace(TypedExtentSouth) &&
                    string.IsNullOrWhiteSpace(TypedExtentEast) && string.IsNullOrWhiteSpace(TypedExtentNorth))
                    return "Enter decimal degrees (WGS84), e.g. West -119.9, South 36.6, East -119.6, North 36.9.";
                string error = GeographicBounds.TryParseBbox(TypedExtentWest, TypedExtentSouth, TypedExtentEast, TypedExtentNorth, out var bounds);
                return error ?? $"Valid extent: {bounds.XMin:F4}, {bounds.YMin:F4} to {bounds.XMax:F4}, {bounds.YMax:F4}";
            }
        }

        /// <summary>The typed bbox when valid, else null.</summary>
        internal ExtentBounds GetTypedExtentOrNull() =>
            GeographicBounds.TryParseBbox(TypedExtentWest, TypedExtentSouth, TypedExtentEast, TypedExtentNorth, out var bounds) == null
                ? bounds : null;

        private bool _useLayerExtent;
        /// <summary>Use a chosen map layer's full extent as the download area.</summary>
        public bool UseLayerExtent
        {
            get => _useLayerExtent;
            set
            {
                SetProperty(ref _useLayerExtent, value);
                if (value)
                {
                    ClearOtherExtentSources();
                    _ = RefreshExtentLayersAsync();
                }
                RaiseExtentSourceCommandsCanExecuteChanged();
            }
        }

        private ObservableCollection<Layer> _availableExtentLayers = new();
        public ObservableCollection<Layer> AvailableExtentLayers
        {
            get => _availableExtentLayers;
            private set => SetProperty(ref _availableExtentLayers, value);
        }

        private Layer _selectedExtentLayer;
        public Layer SelectedExtentLayer
        {
            get => _selectedExtentLayer;
            set => SetProperty(ref _selectedExtentLayer, value);
        }

        private bool _usePolygonExtent;
        /// <summary>Use a polygon drawn on the map as the download area.</summary>
        public bool UsePolygonExtent
        {
            get => _usePolygonExtent;
            set
            {
                SetProperty(ref _usePolygonExtent, value);
                if (value) ClearOtherExtentSources();
                RaiseExtentSourceCommandsCanExecuteChanged();
                NotifyPropertyChanged(nameof(IsPolygonAoiMode));
            }
        }

        /// <summary>True when the download area is an exact polygon (drawn or
        /// from selected features) — the modes the clip checkbox applies to.</summary>
        public bool IsPolygonAoiMode => UsePolygonExtent || UseSelectedFeatureExtent;

        private bool _clipToAoiPolygon;
        /// <summary>When true, downloaded geometries are clipped to the drawn
        /// polygon (applies to the ingest, so the _source archive and display
        /// layers both reflect it). Off by default: whole features are kept.</summary>
        public bool ClipToAoiPolygon
        {
            get => _clipToAoiPolygon;
            set => SetProperty(ref _clipToAoiPolygon, value);
        }

        private bool _hasAoiPolygon;
        public bool HasAoiPolygon
        {
            get => _hasAoiPolygon;
            private set => SetProperty(ref _hasAoiPolygon, value);
        }

        private string _aoiPolygonDisplay = "No polygon drawn";
        public string AoiPolygonDisplay
        {
            get => _aoiPolygonDisplay;
            private set => SetProperty(ref _aoiPolygonDisplay, value);
        }

        /// <summary>WGS84 WKT of the drawn AOI polygon (built by
        /// GeoParquetSql.BuildPolygonWkt), or null when none is drawn.</summary>
        private string _aoiPolygonWkt;

        /// <summary>WGS84 envelope of the drawn AOI polygon.</summary>
        private Envelope _aoiPolygonEnvelope;

        private bool _useSelectedFeatureExtent;
        /// <summary>Use the union of the map's selected polygon features (a
        /// county, city, parcel...) as the download area.</summary>
        public bool UseSelectedFeatureExtent
        {
            get => _useSelectedFeatureExtent;
            set
            {
                SetProperty(ref _useSelectedFeatureExtent, value);
                if (value) ClearOtherExtentSources();
                RaiseExtentSourceCommandsCanExecuteChanged();
                NotifyPropertyChanged(nameof(IsPolygonAoiMode));
            }
        }

        private bool _hasSelectedFeatureAoi;
        public bool HasSelectedFeatureAoi
        {
            get => _hasSelectedFeatureAoi;
            private set => SetProperty(ref _hasSelectedFeatureAoi, value);
        }

        private string _selectedFeatureAoiDisplay = "No features captured";
        public string SelectedFeatureAoiDisplay
        {
            get => _selectedFeatureAoiDisplay;
            private set => SetProperty(ref _selectedFeatureAoiDisplay, value);
        }

        /// <summary>WGS84 WKT of the captured selected-feature area (exported
        /// by ArcGIS from the map selection), or null when none captured.</summary>
        private string _selectedFeatureWkt;

        /// <summary>WGS84 envelope of the captured selected-feature area.</summary>
        private Envelope _selectedFeatureEnvelope;

        private bool _isLoading = true;
        public bool IsLoading
        {
            get => _isLoading;
            set
            {
                SetProperty(ref _isLoading, value);
                (NextCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
                RaisePreviewCommandsCanExecuteChanged();
            }
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

        private string _selectedCompression = "ZSTD";
        public string SelectedCompression
        {
            get => _selectedCompression;
            set => SetProperty(ref _selectedCompression, value);
        }

        public const string GeoParquetVersion1xOption = "1.x (default)";
        public const string GeoParquetVersion2Option = "2.0";

        private string _selectedGeoParquetVersion = GeoParquetVersion1xOption;
        public string SelectedGeoParquetVersion
        {
            get => _selectedGeoParquetVersion;
            set => SetProperty(ref _selectedGeoParquetVersion, value);
        }

        public List<string> GeoParquetVersionOptions => new List<string>
        {
            GeoParquetVersion1xOption,
            GeoParquetVersion2Option
        };

        /// <summary>DuckDB COPY GEOPARQUET_VERSION token for the current UI selection.</summary>
        public string SelectedGeoParquetCopyVersion =>
            GeoParquetSql.NormalizeGeoParquetVersion(_selectedGeoParquetVersion);

        private bool _addSourceProvenanceFields;
        public bool AddSourceProvenanceFields
        {
            get => _addSourceProvenanceFields;
            set => SetProperty(ref _addSourceProvenanceFields, value);
        }

        private bool _saveToFileGeodatabase;
        public bool SaveToFileGeodatabase
        {
            get => _saveToFileGeodatabase;
            set
            {
                if (SetProperty(ref _saveToFileGeodatabase, value))
                    (BrowseFileGeodatabaseCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private string _fileGeodatabasePath;
        public string FileGeodatabasePath
        {
            get => _fileGeodatabasePath;
            set => SetProperty(ref _fileGeodatabasePath, value);
        }

        // Available compression options for binding
        public List<string> CompressionOptions => new List<string> { "ZSTD", "SNAPPY", "GZIP" };

        private bool _loadingAddinSettings;

        private string _httpProxy = string.Empty;
        public string HttpProxy
        {
            get => _httpProxy;
            set
            {
                if (SetProperty(ref _httpProxy, value ?? string.Empty))
                    PersistAddinSettings();
            }
        }

        private string _httpProxyUsername = string.Empty;
        public string HttpProxyUsername
        {
            get => _httpProxyUsername;
            set
            {
                if (SetProperty(ref _httpProxyUsername, value ?? string.Empty))
                    PersistAddinSettings();
            }
        }

        private string _httpProxyPassword = string.Empty;
        public string HttpProxyPassword
        {
            get => _httpProxyPassword;
            set
            {
                if (SetProperty(ref _httpProxyPassword, value ?? string.Empty))
                    PersistAddinSettings();
            }
        }

        private string _memoryLimit = AddinSettings.DefaultMemoryLimit;
        public string MemoryLimit
        {
            get => _memoryLimit;
            set
            {
                if (SetProperty(ref _memoryLimit, value ?? AddinSettings.DefaultMemoryLimit))
                    PersistAddinSettings();
            }
        }

        private string _maxMemory = AddinSettings.DefaultMaxMemory;
        public string MaxMemory
        {
            get => _maxMemory;
            set
            {
                if (SetProperty(ref _maxMemory, value ?? AddinSettings.DefaultMaxMemory))
                    PersistAddinSettings();
            }
        }

        private string _detectedProxyDisplay = "Windows system proxy (auto-detect)";
        public string DetectedProxyDisplay
        {
            get => _detectedProxyDisplay;
            private set => SetProperty(ref _detectedProxyDisplay, value);
        }

        private void LoadAddinSettings()
        {
            _loadingAddinSettings = true;
            try
            {
                var settings = AddinSettingsStore.Load();
                HttpProxy = settings.HttpProxy ?? string.Empty;
                HttpProxyUsername = settings.HttpProxyUsername ?? string.Empty;
                HttpProxyPassword = settings.HttpProxyPassword ?? string.Empty;
                MemoryLimit = settings.EffectiveMemoryLimit;
                MaxMemory = settings.EffectiveMaxMemory;
                RefreshDetectedProxyDisplay(settings);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"LoadAddinSettings failed: {ex.Message}");
            }
            finally
            {
                _loadingAddinSettings = false;
            }
        }

        private void PersistAddinSettings()
        {
            if (_loadingAddinSettings)
                return;

            var settings = new AddinSettings
            {
                HttpProxy = HttpProxy,
                HttpProxyUsername = HttpProxyUsername,
                HttpProxyPassword = HttpProxyPassword,
                MemoryLimit = MemoryLimit,
                MaxMemory = MaxMemory
            };
            AddinSettingsStore.Save(settings);
            ReleaseValidator.RefreshHttpClient();
            RefreshDetectedProxyDisplay(settings);

            if (_dataProcessor != null)
            {
                _ = ApplyDuckDbSettingsAsync();
            }
        }

        private async Task ApplyDuckDbSettingsAsync()
        {
            try
            {
                await _dataProcessor.ApplySessionSettingsAsync();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"ApplyDuckDbSettingsAsync failed: {ex.Message}");
            }
        }

        private void RefreshDetectedProxyDisplay(AddinSettings settings)
        {
            try
            {
                var resolved = DuckDBManager.ResolveProxy(settings);
                DetectedProxyDisplay = resolved.IsConfigured
                    ? resolved.ToLogSafeDescription()
                    : "No proxy (direct connection, or leave blank to auto-detect the Windows system proxy)";
            }
            catch (Exception ex)
            {
                DetectedProxyDisplay = "Windows system proxy (auto-detect)";
                System.Diagnostics.Debug.WriteLine($"Proxy display refresh failed: {ex.Message}");
            }
        }

        // Map purpose / cartography properties
        public List<CartographicProfile> AvailableCartographicProfiles { get; } = CartographicProfileCatalog.AllProfiles;

        private CartographicProfile _selectedCartographicProfile;
        /// <summary>
        /// The user-selected map purpose profile, or null for default ArcGIS Pro symbology.
        /// </summary>
        public CartographicProfile SelectedCartographicProfile
        {
            get => _selectedCartographicProfile;
            set
            {
                if (_selectedCartographicProfile == value) return;
                SetProperty(ref _selectedCartographicProfile, value);
                NotifyPropertyChanged(nameof(IsMapStyleSelected));
                NotifyPropertyChanged(nameof(SelectedMapStyleDescription));
                NotifyPropertyChanged(nameof(SelectedMapStyleCaption));
                (ClearMapStyleCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (ApplyMapStyleCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (RepairMapSymbologyCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        public bool IsMapStyleSelected => _selectedCartographicProfile != null;
        public string SelectedMapStyleDescription
        {
            get
            {
                if (_selectedCartographicProfile == null)
                    return "No map purpose selected. Layers will use default ArcGIS Pro symbology.";

                return _selectedCartographicProfile.Description;
            }
        }

        public string SelectedMapStyleCaption
        {
            get
            {
                if (_selectedCartographicProfile == null)
                    return "Tip: Choose a map purpose when you want Overture layers styled around a specific cartographic focus.";

                string baseStyleName = MapStyleCatalog.AllStyles
                    .FirstOrDefault(style => string.Equals(style.Id, _selectedCartographicProfile.BaseStyleId, StringComparison.OrdinalIgnoreCase))
                    ?.DisplayName
                    ?? _selectedCartographicProfile.BaseStyleId
                    ?? "Default";

                string category = string.IsNullOrWhiteSpace(_selectedCartographicProfile.Category)
                    ? "Purpose"
                    : _selectedCartographicProfile.Category;

                return $"Purpose: {category}. Foundation: {baseStyleName}.";
            }
        }

        public ICommand ClearMapStyleCommand { get; private set; }
        public ICommand ApplyMapStyleCommand { get; private set; }
        public ICommand RepairMapSymbologyCommand { get; private set; }

        private async Task ApplyMapStyleToExistingLayersAsync()
        {
            var profile = SelectedCartographicProfile;
            if (profile == null) return;

            var mapView = MapView.Active;
            if (mapView == null)
            {
                AddToLog("No active map view. Open a map first.");
                return;
            }

            AddToLog($"Applying '{profile.DisplayName}' map purpose to existing layers...");

            int applied = 0;
            int skipped = 0;

            await QueuedTask.Run(() =>
            {
                var map = mapView.Map;
                foreach (var layer in map.GetLayersAsFlattenedList().OfType<FeatureLayer>())
                {
                    if (CartographyService.ApplyStyleToExistingLayer(layer, profile))
                        applied++;
                    else
                        skipped++;
                }
            });

            AddToLog($"Map purpose applied to {applied} layer(s). {skipped} layer(s) unchanged (non-Overture or unrecognized).");
        }

        private async Task RepairMapSymbologyAsync()
        {
            var mapView = MapView.Active;
            if (mapView == null)
            {
                AddToLog("No active map view. Open a map first.");
                return;
            }

            var profile = SelectedCartographicProfile
                          ?? CartographicProfileCatalog.GetById("analyst_canvas")
                          ?? AvailableCartographicProfiles.FirstOrDefault();

            if (profile == null)
            {
                AddToLog("No map purposes are available to repair symbology.");
                return;
            }

            AddToLog($"Repairing map symbology with '{profile.DisplayName}' map purpose...");

            int applied = 0;
            int skipped = 0;

            await QueuedTask.Run(() =>
            {
                var map = mapView.Map;
                foreach (var layer in map.GetLayersAsFlattenedList().OfType<FeatureLayer>())
                {
                    if (CartographyService.ApplyStyleToExistingLayer(layer, profile))
                        applied++;
                    else
                        skipped++;
                }
            });

            AddToLog($"Symbology repair complete. Applied to {applied} layer(s). {skipped} layer(s) unchanged.");
        }

        // Cache management properties
        private string _cacheLocation = "";
        public string CacheLocation
        {
            get => _cacheLocation;
            set => SetProperty(ref _cacheLocation, value);
        }

        private string _cacheSize = "";
        public string CacheSize
        {
            get => _cacheSize;
            set => SetProperty(ref _cacheSize, value);
        }

        private int _cacheFileCount = 0;
        public int CacheFileCount
        {
            get => _cacheFileCount;
            set => SetProperty(ref _cacheFileCount, value);
        }

        public ICommand RefreshCacheInfoCommand { get; private set; }
        public ICommand ClearCacheCommand { get; private set; }

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

        /// <summary>Saved map view extent (map's SR) before adding layers; restored after add to prevent zoom-out.</summary>
        private Envelope _savedViewExtentForRestore;

        private bool _isSelectAllChecked;
        public bool IsSelectAllChecked
        {
            get => _isSelectAllChecked;
            set
            {
                if (_isSelectAllChecked != value)
                {
                    // SetProperty will update the backing field and notify the UI.
                    SetProperty(ref _isSelectAllChecked, value, nameof(IsSelectAllChecked));
                    // Execute the logic to select/deselect all items based on the new value.
                    ExecuteSelectAllInternal(value);
                }
            }
        }

        private bool _isUpdatingSelectionInternally = false;

        #endregion
    }
}
