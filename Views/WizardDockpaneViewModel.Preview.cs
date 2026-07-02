using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Preview-tab members: bridges the WPF ViewModel to the ArcGIS Maps SDK
    /// for JavaScript map hosted in the WebView2 control (see
    /// Views/PreviewMap/preview.html and Services/PreviewBridge.cs).
    /// </summary>
    internal partial class WizardDockpaneViewModel
    {
        private const string PreviewDataHostName = "overturedata";

        private Action<string, string> _mapPreviewVirtualHost;
        private string _mappedPreviewDataFolder;

        // Last extent reported by the JS map (in the map's spatial reference).
        private double _previewExtentXMin, _previewExtentYMin, _previewExtentXMax, _previewExtentYMax;
        private int _previewExtentWkid;
        private bool _hasPreviewExtent;

        public PreviewBridge PreviewBridge { get; private set; }

        private bool _isPreviewAvailable;
        public bool IsPreviewAvailable
        {
            get => _isPreviewAvailable;
            private set => SetProperty(ref _isPreviewAvailable, value);
        }

        public ICommand PreviewShowExtentCommand { get; private set; }
        public ICommand PreviewLoadDataCommand { get; private set; }
        public ICommand PreviewClearCommand { get; private set; }
        public ICommand AdoptPreviewExtentCommand { get; private set; }

        private void InitializePreviewCommands()
        {
            PreviewShowExtentCommand = new RelayCommand(
                async () => await ShowConfiguredExtentOnPreviewAsync(),
                () => IsPreviewAvailable);
            PreviewLoadDataCommand = new RelayCommand(
                () => LoadDownloadedDataIntoPreview(),
                () => IsPreviewAvailable && Directory.Exists(ResolvePreviewDataFolder()));
            PreviewClearCommand = new RelayCommand(
                () => PreviewBridge?.ClearLayers(),
                () => IsPreviewAvailable);
            AdoptPreviewExtentCommand = new RelayCommand(
                async () => await AdoptPreviewExtentAsync(),
                () => IsPreviewAvailable && _hasPreviewExtent);
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
            PreviewBridge.ExtentChanged += (xmin, ymin, xmax, ymax, wkid) =>
            {
                _previewExtentXMin = xmin;
                _previewExtentYMin = ymin;
                _previewExtentXMax = xmax;
                _previewExtentYMax = ymax;
                _previewExtentWkid = wkid;
                if (!_hasPreviewExtent)
                {
                    _hasPreviewExtent = true;
                    (AdoptPreviewExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();
                }
            };
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
            (PreviewLoadDataCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (PreviewClearCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (AdoptPreviewExtentCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        /// <summary>
        /// Shows the currently configured download extent (custom or active
        /// map view) as a dashed box on the preview map.
        /// </summary>
        private async Task ShowConfiguredExtentOnPreviewAsync()
        {
            Envelope wgs84Extent = null;

            if (UseCustomExtent && CustomExtent != null)
            {
                // CustomExtent is already WGS84 (see GetLoadExtentAsync).
                wgs84Extent = CustomExtent;
            }
            else if (MapView.Active != null)
            {
                wgs84Extent = await QueuedTask.Run(() =>
                {
                    Envelope mapExtent = MapView.Active?.Extent;
                    if (mapExtent == null) return null;
                    if (mapExtent.SpatialReference == null || mapExtent.SpatialReference.Wkid == 4326)
                        return mapExtent;
                    return GeometryEngine.Instance.Project(mapExtent,
                        SpatialReferenceBuilder.CreateSpatialReference(4326)) as Envelope;
                });
            }

            if (wgs84Extent != null)
            {
                PreviewBridge?.ShowExtent(wgs84Extent.XMin, wgs84Extent.YMin, wgs84Extent.XMax, wgs84Extent.YMax);
            }
            else
            {
                AddToLog("Preview: no extent to show — open a map view or set a custom extent.");
            }
        }

        /// <summary>
        /// Renders the downloaded GeoParquet files on the preview map via the
        /// WebView2 virtual host. Note the JS ParquetLayer only reads
        /// GZIP/Snappy/uncompressed files; ZSTD (the export default) will fail
        /// to render — a warning is logged so the user knows why.
        /// </summary>
        private void LoadDownloadedDataIntoPreview()
        {
            string dataFolder = ResolvePreviewDataFolder();
            if (!Directory.Exists(dataFolder))
            {
                AddToLog($"Preview: data folder not found: {dataFolder}");
                return;
            }

            // (Re)map the virtual host if the data folder changed since the
            // last preview (a new release changes DataOutputPath).
            if (!string.Equals(_mappedPreviewDataFolder, dataFolder, StringComparison.OrdinalIgnoreCase))
            {
                _mapPreviewVirtualHost?.Invoke(PreviewDataHostName, dataFolder);
                _mappedPreviewDataFolder = dataFolder;
            }

            if (string.Equals(SelectedCompression, "ZSTD", StringComparison.OrdinalIgnoreCase))
            {
                AddToLog("Preview note: the JS ParquetLayer cannot read ZSTD-compressed files. " +
                         "If layers fail to render, re-export with SNAPPY or GZIP compression.");
            }

            PreviewBridge?.ClearLayers();

            int fileCount = 0;
            foreach (var typeDir in Directory.GetDirectories(dataFolder))
            {
                string typeName = Path.GetFileName(typeDir);
                foreach (var parquetFile in Directory.GetFiles(typeDir, "*.parquet"))
                {
                    string fileName = Path.GetFileName(parquetFile);
                    string virtualUrl = $"https://{PreviewDataHostName}/{Uri.EscapeDataString(typeName)}/{Uri.EscapeDataString(fileName)}";
                    PreviewBridge?.AddParquetLayer(virtualUrl, Path.GetFileNameWithoutExtension(fileName));
                    fileCount++;
                }
            }

            AddToLog(fileCount > 0
                ? $"Preview: requested {fileCount} GeoParquet file(s) from {dataFolder}."
                : $"Preview: no .parquet files found under {dataFolder}.");
        }

        private string ResolvePreviewDataFolder()
        {
            if (!string.IsNullOrEmpty(_lastLoadedDataPath) && Directory.Exists(_lastLoadedDataPath))
                return _lastLoadedDataPath;
            return DataOutputPath;
        }

        /// <summary>
        /// Adopts the preview map's current extent as the custom download
        /// extent, projecting from the view's spatial reference (typically
        /// Web Mercator) to WGS84, which downstream code expects.
        /// </summary>
        private async Task AdoptPreviewExtentAsync()
        {
            if (!_hasPreviewExtent) return;

            double xmin = _previewExtentXMin, ymin = _previewExtentYMin;
            double xmax = _previewExtentXMax, ymax = _previewExtentYMax;
            int wkid = _previewExtentWkid;

            Envelope wgs84Extent = await QueuedTask.Run(() =>
            {
                var sourceSr = SpatialReferenceBuilder.CreateSpatialReference(wkid);
                var envelope = EnvelopeBuilderEx.CreateEnvelope(xmin, ymin, xmax, ymax, sourceSr);
                if (wkid == 4326) return envelope;
                return GeometryEngine.Instance.Project(envelope,
                    SpatialReferenceBuilder.CreateSpatialReference(4326)) as Envelope;
            });

            if (wgs84Extent == null)
            {
                AddToLog("Preview: could not project the preview extent to WGS84.");
                return;
            }

            CustomExtent = wgs84Extent;
            UseCustomExtent = true;
            AddToLog($"Adopted preview extent as download extent: " +
                     $"{wgs84Extent.XMin:F4}, {wgs84Extent.YMin:F4} → {wgs84Extent.XMax:F4}, {wgs84Extent.YMax:F4} (WGS84)");
        }
    }
}
