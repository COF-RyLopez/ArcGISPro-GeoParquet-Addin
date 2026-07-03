using ArcGIS.Core.CIM;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DuckDBGeoparquet.Views
{
    internal class OvertureGeocoderDockpaneViewModel : DockPane
    {
        private const string DockPaneId = "DuckDBGeoparquet_Views_OvertureGeocoderDockpane";
        private OvertureGeocoderService _geocoderService;
        private IDisposable _selectionOverlay;
        private string _searchQuery;
        private GeocodeCandidate _selectedCandidate;
        private string _statusText = "Load Addresses + Places first in Overture Maps Data Loader, then search here.";
        private bool _isSearching;
        private bool _isBuildingLocator;
        private bool _isAddingToMap;
        private bool _isGeocodingFile;
        private bool _useLocatorSearch;
        private string _locatorStatusText = "Locator not built.";

        public OvertureGeocoderDockpaneViewModel()
        {
            Results = [];

            if (DesignerProperties.GetIsInDesignMode(new DependencyObject()))
            {
                SearchQuery = "123 Main St";
                Results.Add(new GeocodeCandidate
                {
                    DisplayLabel = "123 Main St, Sample City, CA, 93721, US",
                    Latitude = 36.7378,
                    Longitude = -119.7871,
                    SourceType = "Address",
                    MatchTier = "exact",
                    ConfidenceTier = "High",
                    Score = 300
                });
                SelectedCandidate = Results.FirstOrDefault();
                return;
            }

            InitializeRuntime();
        }

        protected override async Task InitializeAsync()
        {
            try
            {
                _geocoderService ??= new OvertureGeocoderService();
                await _geocoderService.InitializeAsync();
                RefreshLocatorStatus();
                StatusText = "Geocoder ready. Search loaded Overture addresses and places.";
            }
            catch (Exception ex)
            {
                StatusText = $"Geocoder initialization failed: {ex.Message}";
            }
        }

        public static void Show()
        {
            if (FrameworkApplication.DockPaneManager.Find(DockPaneId) is not OvertureGeocoderDockpaneViewModel pane)
            {
                return;
            }

            pane.Activate();
        }

        public ObservableCollection<GeocodeCandidate> Results { get; }

        public string SearchQuery
        {
            get => _searchQuery;
            set
            {
                if (SetProperty(ref _searchQuery, value))
                {
                    (SearchAddressCommand as RelayCommand)?.RaiseCanExecuteChanged();
                }
            }
        }

        public GeocodeCandidate SelectedCandidate
        {
            get => _selectedCandidate;
            set
            {
                if (SetProperty(ref _selectedCandidate, value))
                {
                    (ZoomToSelectedCommand as RelayCommand)?.RaiseCanExecuteChanged();
                    if (value != null)
                    {
                        // Like Pro's Locate pane: clicking a result zooms to it.
                        _ = ZoomToSelectedAsync();
                    }
                }
            }
        }

        public string StatusText
        {
            get => _statusText;
            set => SetProperty(ref _statusText, value);
        }

        public bool IsResultListEmpty => Results.Count == 0;
        public string LocatorStatusText
        {
            get => _locatorStatusText;
            set => SetProperty(ref _locatorStatusText, value);
        }

        public bool UseLocatorSearch
        {
            get => _useLocatorSearch;
            set
            {
                if (SetProperty(ref _useLocatorSearch, value))
                {
                    if (_geocoderService != null)
                    {
                        _geocoderService.PreferLocatorSearch = value;
                    }
                }
            }
        }

        public ICommand SearchAddressCommand { get; private set; }
        public ICommand ZoomToSelectedCommand { get; private set; }
        public ICommand ClearResultsCommand { get; private set; }
        public ICommand AddResultsToMapCommand { get; private set; }
        public ICommand GeocodeFileCommand { get; private set; }
        public ICommand BuildLocatorCommand { get; private set; }
        public ICommand RebuildLocatorCommand { get; private set; }
        public ICommand RefreshLocatorStatusCommand { get; private set; }

        private void InitializeRuntime()
        {
            _geocoderService = new OvertureGeocoderService();

            SearchAddressCommand = new RelayCommand(async () => await SearchAsync(), () => !_isSearching && !string.IsNullOrWhiteSpace(SearchQuery));
            ZoomToSelectedCommand = new RelayCommand(async () => await ZoomToSelectedAsync(), () => SelectedCandidate != null);
            ClearResultsCommand = new RelayCommand(ClearResults);
            AddResultsToMapCommand = new RelayCommand(async () => await AddResultsToMapAsync(), () => !_isAddingToMap);
            GeocodeFileCommand = new RelayCommand(async () => await GeocodeFileAsync(), () => !_isGeocodingFile);
            BuildLocatorCommand = new RelayCommand(async () => await BuildLocatorAsync(false), () => !_isBuildingLocator);
            RebuildLocatorCommand = new RelayCommand(async () => await BuildLocatorAsync(true), () => !_isBuildingLocator);
            RefreshLocatorStatusCommand = new RelayCommand(RefreshLocatorStatus);
        }

        /// <summary>
        /// Writes the current search results to a point feature class in the
        /// project geodatabase and adds it to the map (Locate pane's
        /// "Add To Feature Class" behavior).
        /// </summary>
        private async Task AddResultsToMapAsync()
        {
            if (Results.Count == 0)
            {
                StatusText = "No results to add — run a search first.";
                return;
            }

            _isAddingToMap = true;
            try
            {
                StatusText = "Adding results to the map...";
                string name = $"OvertureGeocode_{DateTime.Now:yyyyMMdd_HHmmss}";
                await GeocodeResultWriter.WriteToFeatureClassAsync(Results.ToList(), name);
                StatusText = $"Added {Results.Count} result(s) to the map as '{name}'.";
            }
            catch (Exception ex)
            {
                StatusText = $"Could not add results to the map: {ex.Message}";
            }
            finally
            {
                _isAddingToMap = false;
            }
        }

        /// <summary>
        /// Geocodes a delimited file of addresses (like the Geocode File tool)
        /// using the same search pipeline as single queries — the ArcGIS
        /// locator when preferred and ready, the local DuckDB search otherwise
        /// — and adds the matches to the map as a feature class.
        /// </summary>
        private async Task GeocodeFileAsync()
        {
            var dialog = new Microsoft.Win32.OpenFileDialog
            {
                Title = "Select a file of addresses to geocode",
                Filter = "Delimited text (*.csv;*.txt)|*.csv;*.txt|All files (*.*)|*.*"
            };
            if (dialog.ShowDialog() != true)
            {
                return;
            }

            _isGeocodingFile = true;
            try
            {
                string[] lines = await Task.Run(() => File.ReadAllLines(dialog.FileName));
                if (lines.Length == 0)
                {
                    StatusText = "The selected file is empty.";
                    return;
                }

                // Use a column named address/single line when the header has
                // one; otherwise treat every row's first column as the query.
                string[] header = SplitCsvLine(lines[0]);
                string[] knownNames = ["address", "singleline", "single_line", "single line input", "full_address", "street"];
                int column = -1;
                for (int i = 0; i < header.Length; i++)
                {
                    if (knownNames.Contains(header[i].Trim().Trim('"').ToLowerInvariant()))
                    {
                        column = i;
                        break;
                    }
                }
                int firstRow = column >= 0 ? 1 : 0;
                if (column < 0) column = 0;

                const int MaxRows = 500;
                var queries = new List<string>();
                for (int i = firstRow; i < lines.Length && queries.Count < MaxRows; i++)
                {
                    var cells = SplitCsvLine(lines[i]);
                    if (cells.Length > column)
                    {
                        string q = cells[column].Trim().Trim('"');
                        if (!string.IsNullOrWhiteSpace(q))
                        {
                            queries.Add(q);
                        }
                    }
                }

                if (queries.Count == 0)
                {
                    StatusText = "No addresses found in the file.";
                    return;
                }

                var matched = new List<GeocodeCandidate>();
                var matchedQueries = new List<string>();
                int done = 0;
                foreach (var q in queries)
                {
                    var best = await _geocoderService.GeocodeBestAsync(q);
                    if (best != null)
                    {
                        matched.Add(best);
                        matchedQueries.Add(q);
                    }
                    done++;
                    if (done % 20 == 0)
                    {
                        StatusText = $"Geocoding file... {done}/{queries.Count}";
                    }
                }

                if (matched.Count == 0)
                {
                    StatusText = $"No matches for {queries.Count} address(es).";
                    return;
                }

                string name = $"OvertureGeocodeFile_{DateTime.Now:yyyyMMdd_HHmmss}";
                await GeocodeResultWriter.WriteToFeatureClassAsync(matched, name, matchedQueries);
                bool truncated = lines.Length - firstRow > MaxRows;
                StatusText = $"Geocoded {matched.Count} of {queries.Count} address(es)" +
                             (truncated ? $" (first {MaxRows} rows)" : string.Empty) +
                             $"; added layer '{name}'.";
            }
            catch (Exception ex)
            {
                StatusText = $"Geocode file failed: {ex.Message}";
            }
            finally
            {
                _isGeocodingFile = false;
            }
        }

        private static string[] SplitCsvLine(string line)
        {
            // Minimal CSV split honoring double-quoted cells.
            var cells = new List<string>();
            var sb = new StringBuilder();
            bool inQuotes = false;
            foreach (char ch in line)
            {
                if (ch == '"')
                {
                    inQuotes = !inQuotes;
                }
                else if (ch == ',' && !inQuotes)
                {
                    cells.Add(sb.ToString());
                    sb.Clear();
                }
                else
                {
                    sb.Append(ch);
                }
            }
            cells.Add(sb.ToString());
            return [.. cells];
        }

        private async Task SearchAsync()
        {
            if (string.IsNullOrWhiteSpace(SearchQuery))
            {
                StatusText = "Enter an address query before searching.";
                return;
            }

            _isSearching = true;
            (SearchAddressCommand as RelayCommand)?.RaiseCanExecuteChanged();
            StatusText = UseLocatorSearch
                ? "Searching hybrid candidates (locator mode enabled with local fallback)..."
                : "Searching hybrid local candidates...";

            try
            {
                Envelope extent = await GetCurrentMapExtentWgs84Async();
                var candidates = await _geocoderService.SearchAsync(SearchQuery, extent, 30);

                Results.Clear();
                foreach (var candidate in candidates)
                {
                    Results.Add(candidate);
                }

                NotifyPropertyChanged(nameof(IsResultListEmpty));

                if (Results.Count == 0)
                {
                    StatusText = "No address/place matches found in loaded ROI data.";
                    SelectedCandidate = null;
                }
                else
                {
                    SelectedCandidate = Results[0];
                    StatusText = $"Found {Results.Count} candidate(s).";
                }
            }
            catch (Exception ex)
            {
                StatusText = $"Search failed: {ex.Message}";
            }
            finally
            {
                _isSearching = false;
                (SearchAddressCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private async Task BuildLocatorAsync(bool forceRebuild)
        {
            _isBuildingLocator = true;
            (BuildLocatorCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (RebuildLocatorCommand as RelayCommand)?.RaiseCanExecuteChanged();
            LocatorStatusText = forceRebuild ? "Rebuilding hybrid locator..." : "Building hybrid locator...";

            try
            {
                var result = await _geocoderService.BuildLocatorAsync(forceRebuild);
                if (result.Succeeded)
                {
                    StatusText = "Hybrid locator build completed.";
                    RefreshLocatorStatus();
                }
                else
                {
                    // Keep the failure message visible — RefreshLocatorStatus
                    // would replace it with the generic "not built yet" text.
                    LocatorStatusText = result.Message;
                    StatusText = "Hybrid locator build failed. See locator status for details.";
                }
            }
            catch (Exception ex)
            {
                LocatorStatusText = $"Locator build error: {ex.Message}";
                StatusText = "Locator build encountered an error.";
            }
            finally
            {
                _isBuildingLocator = false;
                (BuildLocatorCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (RebuildLocatorCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private void RefreshLocatorStatus()
        {
            if (_geocoderService == null)
            {
                LocatorStatusText = "Locator service unavailable.";
                return;
            }

            var metadata = _geocoderService.GetLocatorMetadata();
            if (metadata == null || !_geocoderService.IsLocatorReady())
            {
                LocatorStatusText = "Hybrid locator not built yet.";
                return;
            }

            LocatorStatusText = $"Locator ready ({metadata.ReleaseFolderName}), built {metadata.LastBuiltUtc:u}.";
        }

        private async Task ZoomToSelectedAsync()
        {
            if (SelectedCandidate == null)
            {
                return;
            }

            var mapView = MapView.Active;
            if (mapView == null)
            {
                StatusText = "Open a map view to zoom to a result.";
                return;
            }

            double lon = SelectedCandidate.Longitude;
            double lat = SelectedCandidate.Latitude;
            string label = SelectedCandidate.DisplayLabel;
            try
            {
                // Geometry/symbol construction and overlay/zoom must run on the
                // MCT (QueuedTask), not the UI thread that raised the selection.
                var overlay = await QueuedTask.Run(() =>
                {
                    var wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                    var location = MapPointBuilderEx.CreateMapPoint(lon, lat, wgs84);
                    var marker = SymbolFactory.Instance.ConstructPointSymbol(ColorFactory.Instance.RedRGB, 11.0, SimpleMarkerStyle.Circle);
                    var handle = mapView.AddOverlay(location, marker.MakeSymbolReference());
                    mapView.ZoomTo(location);
                    return handle;
                });

                _selectionOverlay?.Dispose();
                _selectionOverlay = overlay;
                StatusText = $"Zoomed to {label}";
            }
            catch (Exception ex)
            {
                StatusText = $"Could not zoom to selected result: {ex.Message}";
            }
        }

        private void ClearResults()
        {
            Results.Clear();
            SelectedCandidate = null;
            _selectionOverlay?.Dispose();
            _selectionOverlay = null;
            NotifyPropertyChanged(nameof(IsResultListEmpty));
            StatusText = "Cleared search results.";
        }

        private static async Task<Envelope> GetCurrentMapExtentWgs84Async()
        {
            Envelope extent = null;
            await QueuedTask.Run(() =>
            {
                var mapView = MapView.Active;
                if (mapView?.Extent == null)
                {
                    return;
                }

                var mapExtent = mapView.Extent;
                if (mapExtent.SpatialReference == null || mapExtent.SpatialReference.Wkid != 4326)
                {
                    var wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                    extent = GeometryEngine.Instance.Project(mapExtent, wgs84) as Envelope;
                }
                else
                {
                    extent = mapExtent;
                }
            });

            return extent;
        }

        ~OvertureGeocoderDockpaneViewModel()
        {
            _selectionOverlay?.Dispose();
            _geocoderService?.Dispose();
        }
    }
}
