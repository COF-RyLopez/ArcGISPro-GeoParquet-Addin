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
                }
            }
        }

        private string _nextButtonText = "Next >";
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
                    NextButtonText = "Next >";
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

        private bool _addSourceProvenanceFields;
        public bool AddSourceProvenanceFields
        {
            get => _addSourceProvenanceFields;
            set => SetProperty(ref _addSourceProvenanceFields, value);
        }

        // Available compression options for binding
        public List<string> CompressionOptions => new List<string> { "ZSTD", "SNAPPY", "GZIP" };

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
