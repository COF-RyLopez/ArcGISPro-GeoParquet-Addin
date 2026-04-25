using ArcGIS.Core.CIM;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
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
        public ICommand BuildLocatorCommand { get; private set; }
        public ICommand RebuildLocatorCommand { get; private set; }
        public ICommand RefreshLocatorStatusCommand { get; private set; }

        private void InitializeRuntime()
        {
            _geocoderService = new OvertureGeocoderService();

            SearchAddressCommand = new RelayCommand(async () => await SearchAsync(), () => !_isSearching && !string.IsNullOrWhiteSpace(SearchQuery));
            ZoomToSelectedCommand = new RelayCommand(async () => await ZoomToSelectedAsync(), () => SelectedCandidate != null);
            ClearResultsCommand = new RelayCommand(ClearResults);
            BuildLocatorCommand = new RelayCommand(async () => await BuildLocatorAsync(false), () => !_isBuildingLocator);
            RebuildLocatorCommand = new RelayCommand(async () => await BuildLocatorAsync(true), () => !_isBuildingLocator);
            RefreshLocatorStatusCommand = new RelayCommand(RefreshLocatorStatus);
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
                LocatorStatusText = result.Message;
                StatusText = result.Succeeded
                    ? "Hybrid locator build completed."
                    : "Hybrid locator build failed. See locator status for details.";
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
                RefreshLocatorStatus();
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

            try
            {
                var wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                var location = MapPointBuilderEx.CreateMapPoint(SelectedCandidate.Longitude, SelectedCandidate.Latitude, wgs84);
                var marker = SymbolFactory.Instance.ConstructPointSymbol(ColorFactory.Instance.RedRGB, 11.0, SimpleMarkerStyle.Circle);

                _selectionOverlay?.Dispose();
                _selectionOverlay = mapView.AddOverlay(location, marker.MakeSymbolReference());

                await QueuedTask.Run(() => mapView.ZoomTo(location));
                StatusText = $"Zoomed to {SelectedCandidate.DisplayLabel}";
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
