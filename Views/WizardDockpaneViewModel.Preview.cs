using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows.Input;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Preview-tab members: bridges the WPF ViewModel to the ArcGIS Maps SDK
    /// for JavaScript map hosted in the WebView2 control (see
    /// Views/PreviewMap/preview.html and Services/PreviewBridge.cs).
    ///
    /// The preview is a pre-load dry run: "Preview Sample" pulls a capped
    /// GeoJSON sample of the selected themes straight from Overture S3 via
    /// DuckDB, so users can sanity-check what a load would return before
    /// committing to the full download.
    /// </summary>
    internal partial class WizardDockpaneViewModel
    {
        private const string PreviewDataHostName = "overturedata";
        private const int PreviewSampleMaxFeatures = 2000;

        private Action<string, string> _mapPreviewVirtualHost;
        private bool _isPreviewSampling;

        public PreviewBridge PreviewBridge { get; private set; }

        private bool _isPreviewAvailable;
        public bool IsPreviewAvailable
        {
            get => _isPreviewAvailable;
            private set => SetProperty(ref _isPreviewAvailable, value);
        }

        public ICommand PreviewShowExtentCommand { get; private set; }
        public ICommand PreviewSampleCommand { get; private set; }
        public ICommand PreviewClearCommand { get; private set; }

        private void InitializePreviewCommands()
        {
            PreviewShowExtentCommand = new RelayCommand(
                async () => await ShowConfiguredExtentOnPreviewAsync(),
                () => IsPreviewAvailable);
            PreviewSampleCommand = new RelayCommand(
                async () => await PreviewSampleFromS3Async(),
                () => IsPreviewAvailable && !_isPreviewSampling);
            PreviewClearCommand = new RelayCommand(
                () => PreviewBridge?.ClearLayers(),
                () => IsPreviewAvailable);
        }

        /// <summary>
        /// Called by the view's code-behind once the WebView2 control is
        /// initialized. Supplies the delegates the ViewModel needs to talk to
        /// the browser without holding a reference to the control itself.
        /// </summary>
        /// <param name="postMessage">Posts a JSON string to the page.</param>
        /// <param name="mapVirtualHost">Maps a virtual host name to a local folder.</param>
        internal void AttachPreview(Action<string> postMessage, Action<string, string> mapVirtualHost)
        {
            _mapPreviewVirtualHost = mapVirtualHost;

            PreviewBridge = new PreviewBridge(postMessage, AddToLog);
            PreviewBridge.MapReady += () =>
            {
                IsPreviewAvailable = true;
                RaisePreviewCommandsCanExecuteChanged();
                AddToLog("Preview map ready.");
            };
            PreviewBridge.PreviewUnavailable += error =>
            {
                IsPreviewAvailable = false;
                RaisePreviewCommandsCanExecuteChanged();
                AddToLog($"Preview map unavailable: {error}");
            };
            PreviewBridge.LayerLoaded += (name, _) => AddToLog($"Preview: layer '{name}' rendered.");
            PreviewBridge.LayerError += (name, error) => AddToLog($"Preview: layer '{name}' failed: {error}");
        }

        /// <summary>
        /// Called by the view's code-behind when WebView2 initialization fails
        /// (e.g., the WebView2 runtime is missing).
        /// </summary>
        internal void NotifyPreviewInitFailed(string error)
        {
            IsPreviewAvailable = false;
            AddToLog($"Preview map could not be initialized: {error}");
        }

        private void RaisePreviewCommandsCanExecuteChanged()
        {
            (PreviewShowExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (PreviewSampleCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (PreviewClearCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        /// <summary>
        /// Shows the currently configured download extent (custom or active
        /// map view) as a dashed box on the preview map.
        /// </summary>
        private async Task ShowConfiguredExtentOnPreviewAsync()
        {
            Envelope wgs84Extent = await GetConfiguredWgs84ExtentAsync();
            if (wgs84Extent != null)
            {
                PreviewBridge?.ShowExtent(wgs84Extent.XMin, wgs84Extent.YMin, wgs84Extent.XMax, wgs84Extent.YMax);
            }
            else
            {
                AddToLog("Preview: no extent to show — open a map view or set a custom extent.");
            }
        }

        private async Task<Envelope> GetConfiguredWgs84ExtentAsync()
        {
            if (UseCustomExtent && CustomExtent != null)
            {
                // CustomExtent is already WGS84 (see GetLoadExtentAsync).
                return CustomExtent;
            }

            if (MapView.Active == null)
                return null;

            return await QueuedTask.Run(() =>
            {
                Envelope mapExtent = MapView.Active?.Extent;
                if (mapExtent == null) return null;
                if (mapExtent.SpatialReference == null || mapExtent.SpatialReference.Wkid == 4326)
                    return mapExtent;
                return GeometryEngine.Instance.Project(mapExtent,
                    SpatialReferenceBuilder.CreateSpatialReference(4326)) as Envelope;
            });
        }

        /// <summary>
        /// Pulls a capped GeoJSON sample of each selected theme directly from
        /// Overture S3 (extent-filtered, LIMIT-capped, seconds not minutes)
        /// and renders it on the preview map — before any full download.
        /// </summary>
        private async Task PreviewSampleFromS3Async()
        {
            if (_isPreviewSampling) return;

            var selectedLeafItems = GetSelectedLeafItems();
            if (selectedLeafItems.Count == 0)
            {
                AddToLog("Preview: select one or more themes on the Select Data tab first.");
                return;
            }
            if (_dataProcessor == null)
            {
                AddToLog("Preview: data engine is not initialized yet — try again in a moment.");
                return;
            }

            _isPreviewSampling = true;
            (PreviewSampleCommand as RelayCommand)?.RaiseCanExecuteChanged();
            try
            {
                Envelope extent = await GetConfiguredWgs84ExtentAsync();
                ExtentBounds extentBounds = extent == null
                    ? null
                    : new ExtentBounds(extent.XMin, extent.YMin, extent.XMax, extent.YMax);

                // Samples are throwaway; keep them out of the real data folders.
                string sampleDir = Path.Combine(Path.GetTempPath(), "OvertureAddinPreviewSamples");
                try { if (Directory.Exists(sampleDir)) Directory.Delete(sampleDir, true); }
                catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"Preview sample cleanup: {ex.Message}"); }
                Directory.CreateDirectory(sampleDir);

                _mapPreviewVirtualHost?.Invoke(PreviewDataHostName, sampleDir);

                PreviewBridge?.ClearLayers();
                if (extent != null)
                    PreviewBridge?.ShowExtent(extent.XMin, extent.YMin, extent.XMax, extent.YMax);

                string trimmedRelease = LatestRelease?.Trim() ?? "";
                int rendered = 0;
                foreach (var item in selectedLeafItems)
                {
                    string s3Path = trimmedRelease.Length > 0
                        ? $"{S3_BASE_PATH}/{trimmedRelease}/theme={item.ParentThemeForS3}/type={item.ActualType}/*.parquet"
                        : $"{S3_BASE_PATH}/theme={item.ParentThemeForS3}/type={item.ActualType}/*.parquet";
                    string fileName = $"{item.ActualType}.geojson";

                    AddToLog($"Preview: sampling {item.DisplayName} from S3 (up to {PreviewSampleMaxFeatures} features)...");
                    try
                    {
                        await _dataProcessor.ExportPreviewSampleAsync(
                            s3Path,
                            Path.Combine(sampleDir, fileName),
                            extentBounds,
                            PreviewSampleMaxFeatures);
                        PreviewBridge?.AddGeoJsonLayer(
                            $"https://{PreviewDataHostName}/{Uri.EscapeDataString(fileName)}",
                            $"{item.DisplayName} (sample)");
                        rendered++;
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"Preview: sample for {item.DisplayName} failed: {ex.Message}");
                    }
                }

                AddToLog(rendered > 0
                    ? $"Preview: requested {rendered} sample layer(s). Samples are capped at {PreviewSampleMaxFeatures} features per type — the full load may contain more."
                    : "Preview: no sample layers could be produced — see errors above.");
            }
            finally
            {
                _isPreviewSampling = false;
                (PreviewSampleCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }
    }
}
