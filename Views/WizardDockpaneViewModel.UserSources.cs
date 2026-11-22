using ArcGIS.Desktop.Framework;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Custom Sources members: user-saved GeoParquet URLs that appear in the
    /// theme tree beside the Overture themes and download through the same
    /// snapshot pipeline (spatial filter, staging, layer creation). Inspired
    /// by the QGIS GeoParquet Downloader's custom data sources. Persisted to
    /// user_sources.json — plain JSON users can hand-edit, back up or share.
    /// </summary>
    internal partial class WizardDockpaneViewModel
    {
        internal const string CustomSourcesThemeKey = "custom";
        private const string CustomSourcesDisplayName = "Custom Sources";

        /// <summary>
        /// The live dockpane instance, so the ribbon Add Data Source dialog can
        /// refresh the theme tree after saving a source. Null until the pane is
        /// first created; set in the constructor.
        /// </summary>
        internal static WizardDockpaneViewModel Instance { get; set; }

        private List<UserDataSource> _userSources = new();

        private static string UserSourceStorePath => UserSourceStore.GetStorePath(DeterminedDefaultMfcBasePath);

        public ICommand AddUserSourceCommand { get; private set; }
        public ICommand RemoveUserSourceCommand { get; private set; }

        private string _newSourceName;
        public string NewSourceName
        {
            get => _newSourceName;
            set
            {
                SetProperty(ref _newSourceName, value);
                (AddUserSourceCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private string _newSourceUrl;
        public string NewSourceUrl
        {
            get => _newSourceUrl;
            set
            {
                SetProperty(ref _newSourceUrl, value);
                (AddUserSourceCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private bool _isValidatingSource;

        private void InitializeUserSourceCommands()
        {
            AddUserSourceCommand = new RelayCommand(async () => await AddUserSourceAsync(),
                () => !_isValidatingSource && !IsWorking &&
                      !string.IsNullOrWhiteSpace(NewSourceName) && !string.IsNullOrWhiteSpace(NewSourceUrl));
            RemoveUserSourceCommand = new RelayCommand(() => RemoveSelectedUserSource(),
                () => SelectedItemForPreview?.UserSource is { IsBuiltIn: false });
        }

        /// <summary>
        /// Appends the Custom Sources parent node to the theme tree when any
        /// sources are saved. Called from InitializeThemes and after edits.
        /// </summary>
        private void AppendCustomSourcesNode(ObservableCollection<SelectableThemeItem> themes)
        {
            _userSources = UserSourceStore.Load(UserSourceStorePath);
            if (_userSources.Count == 0) return;

            var parent = new SelectableThemeItem(CustomSourcesDisplayName, CustomSourcesThemeKey, CustomSourcesThemeKey, false);
            foreach (var source in _userSources)
            {
                var leaf = new SelectableThemeItem(source.Name, UserSourceStore.SanitizeTypeName(source.Name), CustomSourcesThemeKey, true)
                {
                    Parent = parent,
                    UserSource = source
                };
                leaf.SelectionChanged += OnLeafThemeSelectionChanged;
                parent.SubItems.Add(leaf);
            }
            themes.Add(parent);
        }

        /// <summary>
        /// Shows the dockpane and expands the given provider's parent node in
        /// the theme tree. Null themeKey = Overture (the top-level themes).
        /// Expands only — collapses nothing, selects nothing.
        /// </summary>
        internal static void ShowFocused(string themeKey)
        {
            Show();
            Instance?.FocusThemeGroup(themeKey);
        }

        internal void FocusThemeGroup(string themeKey)
        {
            if (string.IsNullOrEmpty(themeKey))
            {
                ApplyProviderFilter(SelectableThemeItem.OvertureFilterKey);
                IsOvertureReleaseVisible = true; // Overture entry: the release header applies
                return;
            }
            ApplyProviderFilter(themeKey);
            var group = SelectableThemeItem.FindGroup(Themes, themeKey);
            if (group != null) group.IsExpanded = true;
            // The Overture release header is noise while another provider has
            // focus — hide it unless Overture themes are also selected.
            IsOvertureReleaseVisible = SelectableThemeItem.DecideOvertureReleaseVisible(Themes, current: false);
        }

        /// <summary>Download Data entry point: the pane with every provider visible.</summary>
        internal static void ShowAllProviders()
        {
            Show();
            Instance?.ApplyProviderFilter(null);
        }

        private string _providerFilterKey;

        /// <summary>
        /// Scopes the theme tree to one provider (null = show everything).
        /// Hiding is per top-level node; selections are untouched, so Select All
        /// is hidden while a filter is active to avoid selecting unseen themes.
        /// </summary>
        internal void ApplyProviderFilter(string filterKey)
        {
            _providerFilterKey = filterKey;
            if (Themes != null)
                foreach (var item in Themes)
                    item.IsVisible = SelectableThemeItem.DecideProviderVisible(item, filterKey);
            NotifyPropertyChanged(nameof(IsProviderFiltered));
            NotifyPropertyChanged(nameof(IsSelectAllVisible));
            NotifyPropertyChanged(nameof(ProviderFilterLabel));
        }

        public bool IsProviderFiltered => _providerFilterKey != null;

        public bool IsSelectAllVisible => _providerFilterKey == null;

        public string ProviderFilterLabel => _providerFilterKey switch
        {
            SelectableThemeItem.OvertureFilterKey => "Showing Overture Maps themes only",
            PresetCatalog.OsmThemeKey => "Showing OpenStreetMap (LayerCake) only",
            PresetCatalog.SourceCoopThemeKey => "Showing Source Cooperative only",
            CustomSourcesThemeKey => "Showing Custom Sources only",
            _ => null
        };

        private ICommand _showAllProvidersCommand;
        public ICommand ShowAllProvidersCommand =>
            _showAllProvidersCommand ??= new RelayCommand(() => ApplyProviderFilter(null));

        private bool _isOvertureReleaseVisible = true;

        /// <summary>
        /// Whether the Overture release header (release label, status, refresh)
        /// is shown. Only Overture downloads use the release; the header hides
        /// when a non-Overture provider has focus or only preset/custom leaves
        /// are selected.
        /// </summary>
        public bool IsOvertureReleaseVisible
        {
            get => _isOvertureReleaseVisible;
            set
            {
                if (SetProperty(ref _isOvertureReleaseVisible, value))
                    NotifyPropertyChanged(nameof(IsMapPurposeVisible));
            }
        }

        /// <summary>
        /// The Choose Map Purpose styles are tuned to the Overture schema, so
        /// the section shows exactly when the Overture release header does.
        /// </summary>
        public bool IsMapPurposeVisible => _isOvertureReleaseVisible;

        internal void UpdateOvertureReleaseVisibility() =>
            IsOvertureReleaseVisible = SelectableThemeItem.DecideOvertureReleaseVisible(Themes, IsOvertureReleaseVisible);

        /// <summary>True for the built-in preset parent nodes (PresetCatalog).</summary>
        private static bool IsPresetThemeKey(string themeKey) =>
            themeKey == PresetCatalog.OsmThemeKey || themeKey == PresetCatalog.SourceCoopThemeKey;

        /// <summary>
        /// Appends the built-in preset groups (OpenStreetMap LayerCake, Source
        /// Cooperative) to the theme tree. Presets are UserDataSource instances
        /// with shipped geometry metadata, so they download through the same
        /// pipeline as user-defined custom sources — no probe, no persistence.
        /// </summary>
        private void AppendPresetNodes(ObservableCollection<SelectableThemeItem> themes)
        {
            AppendPresetGroup(themes, PresetCatalog.OsmDisplayName, PresetCatalog.OsmThemeKey, PresetCatalog.OpenStreetMap);
            AppendPresetGroup(themes, PresetCatalog.SourceCoopDisplayName, PresetCatalog.SourceCoopThemeKey, PresetCatalog.SourceCooperative);
        }

        private void AppendPresetGroup(ObservableCollection<SelectableThemeItem> themes,
            string displayName, string themeKey, IReadOnlyList<UserDataSource> presets)
        {
            var parent = new SelectableThemeItem(displayName, themeKey, themeKey, false);
            foreach (var source in presets)
            {
                var leaf = new SelectableThemeItem(source.Name, UserSourceStore.SanitizeTypeName(source.Name), themeKey, true)
                {
                    Parent = parent,
                    UserSource = source
                };
                leaf.SelectionChanged += OnLeafThemeSelectionChanged;
                parent.SubItems.Add(leaf);
            }
            themes.Add(parent);
        }

        internal void RebuildCustomSourcesNode()
        {
            if (Themes == null) return;
            var existing = Themes.FirstOrDefault(t => t.ParentThemeForS3 == CustomSourcesThemeKey);
            if (existing != null) Themes.Remove(existing);
            AppendCustomSourcesNode(Themes);
            ApplyProviderFilter(_providerFilterKey); // the rebuilt node defaults to visible
            UpdateIsSelectAllCheckedStatus();
            (LoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        private async Task AddUserSourceAsync()
        {
            string name = NewSourceName?.Trim();
            string url = NewSourceUrl?.Trim();
            string validationError = UserSourceStore.ValidateNewSource(name, url, _userSources);
            if (validationError != null)
            {
                StatusText = validationError;
                return;
            }

            _isValidatingSource = true;
            (AddUserSourceCommand as RelayCommand)?.RaiseCanExecuteChanged();
            StatusText = $"Validating {url} ...";
            AddToLog($"Validating custom source: {url}");
            try
            {
                await _dataProcessor.InitializeDuckDBAsync();
                var source = await _dataProcessor.ProbeUserSourceAsync(name, url);
                _userSources.Add(source);
                UserSourceStore.Save(UserSourceStorePath, _userSources);
                RebuildCustomSourcesNode();
                NewSourceName = string.Empty;
                NewSourceUrl = string.Empty;
                StatusText = $"Added custom source '{name}' (geometry column: {source.GeometryColumn}).";
                AddToLog(StatusText);
            }
            catch (Exception ex)
            {
                StatusText = $"Could not add source: {ex.Message}";
                AddToLog(StatusText);
            }
            finally
            {
                _isValidatingSource = false;
                (AddUserSourceCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private void RemoveSelectedUserSource()
        {
            var source = SelectedItemForPreview?.UserSource;
            if (source == null || source.IsBuiltIn) return;
            _userSources.RemoveAll(s => ReferenceEquals(s, source) ||
                string.Equals(s.Name, source.Name, StringComparison.OrdinalIgnoreCase));
            UserSourceStore.Save(UserSourceStorePath, _userSources);
            RebuildCustomSourcesNode();
            SelectedItemForPreview = null;
            StatusText = $"Removed custom source '{source.Name}'.";
            AddToLog(StatusText);
        }
    }
}
