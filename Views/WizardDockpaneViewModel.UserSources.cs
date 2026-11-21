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
                () => SelectedItemForPreview?.UserSource != null);
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

        internal void RebuildCustomSourcesNode()
        {
            if (Themes == null) return;
            var existing = Themes.FirstOrDefault(t => t.ParentThemeForS3 == CustomSourcesThemeKey);
            if (existing != null) Themes.Remove(existing);
            AppendCustomSourcesNode(Themes);
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
            if (source == null) return;
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
