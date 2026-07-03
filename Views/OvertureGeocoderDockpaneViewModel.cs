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
        private readonly List<IDisposable> _candidateOverlays = [];
        private readonly List<IDisposable> _selectionOverlays = [];
        private string _searchQuery;
        private GeocodeCandidate _selectedCandidate;
        private string _statusText = "Load Addresses + Places first in Overture Maps Data Loader, then search here.";
        private bool _isSearching;
        private bool _isBuildingLocator;
        private bool _isAddingToMap;
        private bool _isGeocodingFile;
        private bool _suppressSelectionZoom;
        private bool _useLocatorSearch;
        private string _locatorStatusText = "Locator not built.";

        public OvertureGeocoderDockpaneViewModel()
        {
            Results = [];
            SelectedCandidates = [];

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
                SelectedCandidates.Add(SelectedCandidate);
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
        public ObservableCollection<GeocodeCandidate> SelectedCandidates { get; }

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
                    RaiseResultCommandStatesChanged();
                    if (value != null && !_suppressSelectionZoom)
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

            SearchAddressCommand = new RelayCommand(async () => await SearchAsync(), () => !_isSearching && !_isAddingToMap && !_isGeocodingFile && !string.IsNullOrWhiteSpace(SearchQuery));
            ZoomToSelectedCommand = new RelayCommand(async () => await ZoomToSelectedAsync(), () => GetSelectedCandidatesForAction().Count > 0);
            ClearResultsCommand = new RelayCommand(ClearResults);
            AddResultsToMapCommand = new RelayCommand(async () => await AddResultsToMapAsync(), () => !_isSearching && !_isAddingToMap && !_isGeocodingFile && Results.Count > 0);
            GeocodeFileCommand = new RelayCommand(async () => await GeocodeFileAsync(), () => !_isSearching && !_isAddingToMap && !_isGeocodingFile);
            BuildLocatorCommand = new RelayCommand(async () => await BuildLocatorAsync(false), () => !_isBuildingLocator);
            RebuildLocatorCommand = new RelayCommand(async () => await BuildLocatorAsync(true), () => !_isBuildingLocator);
            RefreshLocatorStatusCommand = new RelayCommand(RefreshLocatorStatus);
        }

        private void RaiseResultCommandStatesChanged()
        {
            (ZoomToSelectedCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (AddResultsToMapCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        private void RaiseFileWorkflowCommandStatesChanged()
        {
            (SearchAddressCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (AddResultsToMapCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (GeocodeFileCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        internal void SetSelectedCandidates(IEnumerable<GeocodeCandidate> candidates)
        {
            SelectedCandidates.Clear();
            foreach (var candidate in candidates.Where(c => c != null))
            {
                SelectedCandidates.Add(candidate);
            }

            RaiseResultCommandStatesChanged();
        }

        private List<GeocodeCandidate> GetSelectedCandidatesForAction()
        {
            var selected = SelectedCandidates.Where(HasUsableLocation).ToList();
            if (selected.Count == 0 && HasUsableLocation(SelectedCandidate))
            {
                selected.Add(SelectedCandidate);
            }

            return selected;
        }

        private List<GeocodeCandidate> GetCandidatesToWrite()
        {
            var selected = SelectedCandidates.ToList();
            return selected.Count > 0 ? selected : Results.ToList();
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
            RaiseFileWorkflowCommandStatesChanged();
            try
            {
                bool usingSelectedCandidates = SelectedCandidates.Count > 0;
                var candidatesToWrite = GetCandidatesToWrite();
                if (candidatesToWrite.Count == 0)
                {
                    StatusText = "No results to add — run a search first.";
                    return;
                }

                StatusText = "Adding results to the map...";
                string name = $"OvertureGeocode_{DateTime.Now:yyyyMMdd_HHmmss}";
                await GeocodeResultWriter.WriteToFeatureClassAsync(candidatesToWrite, name);
                StatusText = usingSelectedCandidates
                    ? $"Added {candidatesToWrite.Count} selected result(s) to the map as '{name}'."
                    : $"Added {candidatesToWrite.Count} result(s) to the map as '{name}'.";
            }
            catch (Exception ex)
            {
                StatusText = $"Could not add results to the map: {ex.Message}";
            }
            finally
            {
                _isAddingToMap = false;
                RaiseFileWorkflowCommandStatesChanged();
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
                Filter = "Delimited text (*.csv;*.txt;*.tsv)|*.csv;*.txt;*.tsv|All files (*.*)|*.*"
            };
            if (dialog.ShowDialog() != true)
            {
                return;
            }

            _isGeocodingFile = true;
            RaiseFileWorkflowCommandStatesChanged();
            try
            {
                string[] lines = await Task.Run(() => File.ReadAllLines(dialog.FileName));
                if (lines.Length == 0)
                {
                    StatusText = "The selected file is empty.";
                    return;
                }

                const int MaxRows = 500;
                var parseResult = ParseGeocodeFileQueries(lines, MaxRows);
                var queries = parseResult.Queries;

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
                    GeocodeCandidate best = null;
                    foreach (var searchQuery in q.SearchQueries)
                    {
                        best = await _geocoderService.GeocodeBestAsync(searchQuery);
                        if (HasUsableLocation(best))
                        {
                            break;
                        }
                    }

                    if (HasUsableLocation(best))
                    {
                        matched.Add(best);
                        matchedQueries.Add(q.SourceQuery);
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
                StatusText = $"Geocoded {matched.Count} of {queries.Count} address(es)" +
                             (parseResult.WasTruncated ? $" (first {MaxRows} rows)" : string.Empty) +
                             $"; added layer '{name}'.";
            }
            catch (Exception ex)
            {
                StatusText = $"Geocode file failed: {ex.Message}";
            }
            finally
            {
                _isGeocodingFile = false;
                RaiseFileWorkflowCommandStatesChanged();
            }
        }

        private static ParsedGeocodeFile ParseGeocodeFileQueries(string[] lines, int maxRows)
        {
            char delimiter = DetectDelimiter(lines[0]);
            string[] header = SplitDelimitedLine(lines[0], delimiter);
            string[] normalizedHeader = [.. header.Select(NormalizeHeaderToken)];

            bool looksLikeHeader = normalizedHeader.Any(IsKnownGeocodeHeaderName);
            int firstRow = looksLikeHeader ? 1 : 0;

            int addressColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "address", "singleline", "single_line", "single line input", "full_address")
                : (header.Length == 1 ? 0 : -1);
            int streetColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "street", "street_name", "road", "name", "saddstr")
                : -1;
            int houseNumberColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "number", "housenumber", "house_number", "saddno")
                : -1;
            int housePrefixColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "prefix", "predir", "streetprefix", "saddpref")
                : -1;
            int streetTypeColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "streettype", "street_type", "sttype", "saddsttyp")
                : -1;
            int streetSuffixColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "suffix", "postdir", "streetsuffix", "saddstsuf")
                : -1;
            int unitColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "unit", "apt", "apartment", "suite", "sunit")
                : -1;
            int cityColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "city", "scity", "locality")
                : -1;
            int stateColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "state", "state2", "region", "province")
                : -1;
            int zipColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "zip", "zip5", "szip", "szip5", "postcode", "postalcode")
                : -1;

            if (looksLikeHeader && addressColumn < 0 && streetColumn < 0)
            {
                throw new InvalidOperationException(
                    "Could not find an address column. Include an 'address' field or separate street fields in the file.");
            }

            var queries = new List<GeocodeFileQuery>();
            for (int i = firstRow; i < lines.Length && queries.Count < maxRows; i++)
            {
                var cells = SplitDelimitedLine(lines[i], delimiter);
                var searchQueries = BuildGeocodeQueries(
                    cells,
                    addressColumn,
                    houseNumberColumn,
                    housePrefixColumn,
                    streetColumn,
                    streetTypeColumn,
                    streetSuffixColumn,
                    unitColumn,
                    cityColumn,
                    stateColumn,
                    zipColumn);

                if (searchQueries.Count == 0 && !looksLikeHeader && cells.Length > 0)
                {
                    string fallback = CleanCell(cells[0]);
                    if (!string.IsNullOrWhiteSpace(fallback))
                    {
                        searchQueries = [fallback];
                    }
                }

                if (searchQueries.Count > 0)
                {
                    queries.Add(new GeocodeFileQuery(searchQueries[0], searchQueries));
                }
            }

            return new ParsedGeocodeFile(queries, lines.Length - firstRow > maxRows);
        }

        private static IReadOnlyList<string> BuildGeocodeQueries(
            string[] cells,
            int addressColumn,
            int houseNumberColumn,
            int housePrefixColumn,
            int streetColumn,
            int streetTypeColumn,
            int streetSuffixColumn,
            int unitColumn,
            int cityColumn,
            int stateColumn,
            int zipColumn)
        {
            string address = CleanCell(GetCell(cells, addressColumn));
            if (string.IsNullOrWhiteSpace(address))
            {
                var addressParts = new[]
                {
                    CleanCell(GetCell(cells, houseNumberColumn)),
                    CleanCell(GetCell(cells, housePrefixColumn)),
                    CleanCell(GetCell(cells, streetColumn)),
                    CleanCell(GetCell(cells, streetTypeColumn)),
                    CleanCell(GetCell(cells, streetSuffixColumn)),
                    CleanCell(GetCell(cells, unitColumn))
                };

                address = string.Join(" ", addressParts.Where(part => !string.IsNullOrWhiteSpace(part)));
            }

            if (string.IsNullOrWhiteSpace(address))
            {
                return [];
            }

            string city = CleanCell(GetCell(cells, cityColumn));
            string state = CleanCell(GetCell(cells, stateColumn));
            string zip = CleanCell(GetCell(cells, zipColumn));

            return GeocodeFileQueryBuilder.BuildQueryVariants(address, city, state, zip);
        }

        private static int FindColumn(string[] normalizedHeader, params string[] aliases)
        {
            var normalizedAliases = aliases
                .Select(NormalizeHeaderToken)
                .Where(alias => !string.IsNullOrWhiteSpace(alias))
                .ToHashSet(StringComparer.Ordinal);

            for (int i = 0; i < normalizedHeader.Length; i++)
            {
                if (normalizedAliases.Contains(normalizedHeader[i]))
                {
                    return i;
                }
            }

            return -1;
        }

        private static bool IsKnownGeocodeHeaderName(string name)
        {
            return FindColumn(
                [name],
                "address",
                "singleline",
                "single_line",
                "single line input",
                "full_address",
                "street",
                "street_name",
                "road",
                "name",
                "saddstr",
                "saddno",
                "city",
                "scity",
                "state",
                "state2",
                "zip",
                "szip",
                "postcode") >= 0;
        }

        private static string NormalizeHeaderToken(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            var normalized = new StringBuilder(value.Length);
            foreach (char ch in value.Trim().Trim('"').ToLowerInvariant())
            {
                if (char.IsLetterOrDigit(ch))
                {
                    normalized.Append(ch);
                }
            }

            return normalized.ToString();
        }

        private static string GetCell(string[] cells, int index)
        {
            return index >= 0 && index < cells.Length ? cells[index] : string.Empty;
        }

        private static string CleanCell(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            return string.Join(" ",
                value.Trim().Trim('"')
                    .Split((char[])null, StringSplitOptions.RemoveEmptyEntries));
        }

        private static char DetectDelimiter(string line)
        {
            char bestDelimiter = ',';
            int bestCount = -1;

            foreach (char candidate in new[] { '\t', ',', ';', '|' })
            {
                int count = 0;
                bool inQuotes = false;
                for (int i = 0; i < line.Length; i++)
                {
                    char ch = line[i];
                    if (ch == '"')
                    {
                        if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                        {
                            i++;
                            continue;
                        }

                        inQuotes = !inQuotes;
                    }
                    else if (ch == candidate && !inQuotes)
                    {
                        count++;
                    }
                }

                if (count > bestCount)
                {
                    bestDelimiter = candidate;
                    bestCount = count;
                }
            }

            return bestDelimiter;
        }

        private static string[] SplitDelimitedLine(string line, char delimiter)
        {
            // Minimal delimited split honoring double-quoted cells.
            var cells = new List<string>();
            var sb = new StringBuilder();
            bool inQuotes = false;
            for (int i = 0; i < line.Length; i++)
            {
                char ch = line[i];
                if (ch == '"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        sb.Append('"');
                        i++;
                        continue;
                    }

                    inQuotes = !inQuotes;
                }
                else if (ch == delimiter && !inQuotes)
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

        private sealed record GeocodeFileQuery(string SourceQuery, IReadOnlyList<string> SearchQueries);
        private sealed record ParsedGeocodeFile(IReadOnlyList<GeocodeFileQuery> Queries, bool WasTruncated);

        private async Task SearchAsync()
        {
            if (string.IsNullOrWhiteSpace(SearchQuery))
            {
                StatusText = "Enter an address query before searching.";
                return;
            }

            _isSearching = true;
            RaiseFileWorkflowCommandStatesChanged();
            StatusText = UseLocatorSearch
                ? "Searching loaded datasets (locator mode enabled with local fallback)..."
                : "Searching loaded Overture address/place datasets...";

            try
            {
                var candidates = await _geocoderService.SearchAsync(SearchQuery, null, 30);

                ClearCandidateOverlays();
                ClearSelectionOverlays();
                SelectedCandidates.Clear();
                _suppressSelectionZoom = true;
                try
                {
                    SelectedCandidate = null;
                }
                finally
                {
                    _suppressSelectionZoom = false;
                }
                Results.Clear();
                foreach (var candidate in candidates)
                {
                    Results.Add(candidate);
                }

                NotifyPropertyChanged(nameof(IsResultListEmpty));
                RaiseResultCommandStatesChanged();

                if (Results.Count == 0)
                {
                    StatusText = "No address/place matches found in the loaded Overture datasets.";
                    SelectedCandidate = null;
                }
                else
                {
                    await ShowCandidateOverlaysAsync(Results.ToList());
                    StatusText = $"Found {Results.Count} candidate(s) in the loaded Overture datasets.";
                }
            }
            catch (Exception ex)
            {
                StatusText = $"Search failed: {ex.Message}";
            }
            finally
            {
                _isSearching = false;
                RaiseFileWorkflowCommandStatesChanged();
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
            var selected = GetSelectedCandidatesForAction();
            if (selected.Count == 0)
            {
                return;
            }

            var mapView = MapView.Active;
            if (mapView == null)
            {
                StatusText = "Open a map view to zoom to a result.";
                return;
            }

            try
            {
                // Geometry/symbol construction and overlay/zoom must run on the
                // MCT (QueuedTask), not the UI thread that raised the selection.
                var overlays = await QueuedTask.Run(() =>
                {
                    var wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                    var marker = SymbolFactory.Instance.ConstructPointSymbol(ColorFactory.Instance.RedRGB, 11.0, SimpleMarkerStyle.Circle);
                    var handles = new List<IDisposable>();
                    var points = new List<MapPoint>();

                    foreach (var candidate in selected)
                    {
                        var location = MapPointBuilderEx.CreateMapPoint(candidate.Longitude, candidate.Latitude, wgs84);
                        points.Add(location);
                        handles.Add(mapView.AddOverlay(location, marker.MakeSymbolReference()));
                    }

                    if (points.Count == 1)
                    {
                        mapView.ZoomTo(points[0]);
                    }
                    else
                    {
                        double xmin = points.Min(p => p.X);
                        double ymin = points.Min(p => p.Y);
                        double xmax = points.Max(p => p.X);
                        double ymax = points.Max(p => p.Y);
                        if (xmin == xmax && ymin == ymax)
                        {
                            mapView.ZoomTo(points[0]);
                        }
                        else
                        {
                            mapView.ZoomTo(EnvelopeBuilderEx.CreateEnvelope(xmin, ymin, xmax, ymax, wgs84));
                        }
                    }

                    return handles;
                });

                ClearSelectionOverlays();
                _selectionOverlays.AddRange(overlays);
                StatusText = selected.Count == 1
                    ? $"Zoomed to {selected[0].DisplayLabel}"
                    : $"Zoomed to {selected.Count} selected candidates.";
            }
            catch (Exception ex)
            {
                StatusText = $"Could not zoom to selected result: {ex.Message}";
            }
        }

        private async Task ShowCandidateOverlaysAsync(IReadOnlyList<GeocodeCandidate> candidates)
        {
            ClearCandidateOverlays();

            var mapView = MapView.Active;
            if (mapView == null)
            {
                return;
            }

            var visibleCandidates = candidates
                .Where(HasUsableLocation)
                .Take(100)
                .ToList();
            if (visibleCandidates.Count == 0)
            {
                return;
            }

            var overlays = await QueuedTask.Run(() =>
            {
                var handles = new List<IDisposable>();
                var wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                var marker = SymbolFactory.Instance.ConstructPointSymbol(
                    new CIMRGBColor { R = 0, G = 122, B = 255, Alpha = 80 },
                    8.0,
                    SimpleMarkerStyle.Circle);

                foreach (var candidate in visibleCandidates)
                {
                    var location = MapPointBuilderEx.CreateMapPoint(candidate.Longitude, candidate.Latitude, wgs84);
                    handles.Add(mapView.AddOverlay(location, marker.MakeSymbolReference()));
                }

                return handles;
            });

            _candidateOverlays.AddRange(overlays);
        }

        private static bool HasUsableLocation(GeocodeCandidate candidate)
        {
            return candidate != null &&
                   !double.IsNaN(candidate.Longitude) &&
                   !double.IsNaN(candidate.Latitude) &&
                   !double.IsInfinity(candidate.Longitude) &&
                   !double.IsInfinity(candidate.Latitude) &&
                   candidate.Longitude >= -180 &&
                   candidate.Longitude <= 180 &&
                   candidate.Latitude >= -90 &&
                   candidate.Latitude <= 90;
        }

        private void ClearCandidateOverlays()
        {
            foreach (var overlay in _candidateOverlays)
            {
                try { overlay.Dispose(); }
                catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"Candidate overlay cleanup skipped: {ex.Message}"); }
            }
            _candidateOverlays.Clear();
        }

        private void ClearSelectionOverlays()
        {
            foreach (var overlay in _selectionOverlays)
            {
                try { overlay.Dispose(); }
                catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"Selection overlay cleanup skipped: {ex.Message}"); }
            }
            _selectionOverlays.Clear();
        }

        private void ClearResults()
        {
            Results.Clear();
            SelectedCandidates.Clear();
            SelectedCandidate = null;
            ClearCandidateOverlays();
            ClearSelectionOverlays();
            NotifyPropertyChanged(nameof(IsResultListEmpty));
            RaiseResultCommandStatesChanged();
            StatusText = "Cleared search results.";
        }

        ~OvertureGeocoderDockpaneViewModel()
        {
            ClearCandidateOverlays();
            ClearSelectionOverlays();
            _geocoderService?.Dispose();
        }
    }
}
