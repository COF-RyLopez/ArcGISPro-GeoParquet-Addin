using System;
using System.Collections.Generic;
using System.Text.Json;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Manages bidirectional messaging between the WPF ViewModel
    /// and the ArcGIS Maps SDK for JavaScript 5.1 map running inside WebView2.
    /// 
    /// Outbound (C# → JS): Call methods like ShowExtent(), AddParquetLayer(), etc.
    /// Inbound (JS → C#): Subscribe to events like LayerLoaded, ExtentChanged, MapReady.
    /// </summary>
    public class PreviewBridge
    {
        private readonly Action<string> _postMessageAction;
        private readonly Action<string> _logAction;

        /// <summary>
        /// Raised when the JS map finishes loading a layer.
        /// Parameters: layerName, featureCount (-1 if unknown).
        /// </summary>
        public event Action<string, int> LayerLoaded;

        /// <summary>
        /// Raised when a layer fails to load.
        /// Parameters: layerName, errorMessage.
        /// </summary>
        public event Action<string, string> LayerError;

        /// <summary>
        /// Raised when the user pans/zooms the preview map and it becomes stationary.
        /// Parameters: xmin, ymin, xmax, ymax (in the map's spatial reference).
        /// </summary>
        public event Action<double, double, double, double, int> ExtentChanged;

        /// <summary>
        /// Raised when the JS map view is fully initialized and ready.
        /// </summary>
        public event Action MapReady;

        /// <summary>
        /// Raised when the preview page reports it cannot run
        /// (e.g., the ArcGIS JS SDK failed to load from the CDN).
        /// Parameter: errorMessage.
        /// </summary>
        public event Action<string> PreviewUnavailable;

        /// <summary>
        /// Whether the preview map has reported ready.
        /// </summary>
        public bool IsMapReady { get; private set; }

        /// <summary>
        /// Creates a new PreviewBridge.
        /// </summary>
        /// <param name="postMessageAction">
        /// Delegate that posts a string message to the WebView2 control.
        /// Typically: (json) => webView.CoreWebView2.PostWebMessageAsString(json)
        /// </param>
        /// <param name="logAction">Optional logger for diagnostics.</param>
        public PreviewBridge(Action<string> postMessageAction, Action<string> logAction = null)
        {
            _postMessageAction = postMessageAction ?? throw new ArgumentNullException(nameof(postMessageAction));
            _logAction = logAction;
        }

        // ── Outbound: C# → JS ─────────────────────────────────────

        /// <summary>
        /// Renders a dashed extent rectangle on the preview map and zooms to it.
        /// </summary>
        public void ShowExtent(double xmin, double ymin, double xmax, double ymax)
        {
            _logAction?.Invoke($"Preview: showing extent [{xmin:F4}, {ymin:F4}, {xmax:F4}, {ymax:F4}]");
            PostMessage(new { type = "showExtent", xmin, ymin, xmax, ymax });
        }

        /// <summary>
        /// Adds a GeoParquet file as a ParquetLayer on the preview map.
        /// </summary>
        /// <param name="fileUrl">
        /// URL to the parquet file. For local files, use the virtual host mapping
        /// (e.g., "https://localdata/building/building_001.parquet").
        /// </param>
        /// <param name="displayName">Layer name shown in the preview status bar.</param>
        /// <param name="renderer">
        /// Optional ArcGIS JS renderer definition (JSON-serializable).
        /// If null, the JS SDK will use a default renderer.
        /// </param>
        public void AddParquetLayer(string fileUrl, string displayName, object renderer = null)
        {
            _logAction?.Invoke($"Preview: adding ParquetLayer '{displayName}' from {fileUrl}");
            PostMessage(new { type = "addParquetLayer", url = fileUrl, name = displayName, renderer });
        }

        /// <summary>
        /// Adds inline GeoJSON as a GeoJSONLayer on the preview map. The text
        /// is sent through the message bridge and turned into a blob URL on
        /// the JS side — WebView2 virtual-host fetches are unreliable from the
        /// SDK's web workers, so no URL fetch is involved.
        /// </summary>
        public void AddGeoJsonLayer(string displayName, string geoJsonText, object renderer = null)
        {
            _logAction?.Invoke($"Preview: adding GeoJSON layer '{displayName}' ({(geoJsonText?.Length ?? 0):N0} chars)");
            PostMessage(new { type = "addGeoJsonLayer", name = displayName, data = geoJsonText, renderer });
        }

        /// <summary>
        /// Removes all preview layers and the extent graphic.
        /// </summary>
        public void ClearLayers()
        {
            _logAction?.Invoke("Preview: clearing all layers");
            PostMessage(new { type = "clearLayers" });
        }

        /// <summary>
        /// Changes the basemap of the preview map.
        /// </summary>
        /// <param name="basemapId">
        /// Esri basemap ID, e.g., "dark-gray-vector", "streets-vector", "satellite".
        /// </param>
        public void SetBasemap(string basemapId)
        {
            _logAction?.Invoke($"Preview: setting basemap to '{basemapId}'");
            PostMessage(new { type = "setBasemap", basemap = basemapId });
        }

        /// <summary>
        /// Zooms the preview map to the specified extent.
        /// </summary>
        public void ZoomTo(double xmin, double ymin, double xmax, double ymax, int wkid = 4326)
        {
            PostMessage(new { type = "zoomTo", xmin, ymin, xmax, ymax, wkid });
        }

        // ── Inbound: JS → C# ──────────────────────────────────────

        /// <summary>
        /// Handles an incoming web message from the preview HTML page.
        /// Call this from the WebMessageReceived event handler.
        /// </summary>
        /// <param name="json">Raw JSON string from WebView2.</param>
        public void HandleWebMessage(string json)
        {
            if (string.IsNullOrEmpty(json))
                return;

            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (!root.TryGetProperty("type", out var typeElement))
                {
                    _logAction?.Invoke("Preview: received message without 'type' property");
                    return;
                }

                string msgType = typeElement.GetString();

                switch (msgType)
                {
                    case "mapReady":
                        IsMapReady = true;
                        _logAction?.Invoke("Preview: map is ready");
                        MapReady?.Invoke();
                        break;

                    case "layerLoaded":
                        {
                            string name = root.TryGetProperty("name", out var n) ? n.GetString() : "unknown";
                            int count = root.TryGetProperty("featureCount", out var c) ? c.GetInt32() : -1;
                            _logAction?.Invoke($"Preview: layer '{name}' loaded ({count} features)");
                            LayerLoaded?.Invoke(name, count);
                        }
                        break;

                    case "layerError":
                        {
                            string name = root.TryGetProperty("name", out var n) ? n.GetString() : "unknown";
                            string error = root.TryGetProperty("error", out var e) ? e.GetString() : "unknown error";
                            _logAction?.Invoke($"Preview: layer '{name}' error: {error}");
                            LayerError?.Invoke(name, error);
                        }
                        break;

                    case "extentChanged":
                        {
                            if (root.TryGetProperty("xmin", out var x1) &&
                                root.TryGetProperty("ymin", out var y1) &&
                                root.TryGetProperty("xmax", out var x2) &&
                                root.TryGetProperty("ymax", out var y2))
                            {
                                int wkid = root.TryGetProperty("wkid", out var w) ? w.GetInt32() : 4326;
                                ExtentChanged?.Invoke(x1.GetDouble(), y1.GetDouble(), x2.GetDouble(), y2.GetDouble(), wkid);
                            }
                        }
                        break;

                    case "previewUnavailable":
                        {
                            string error = root.TryGetProperty("error", out var e) ? e.GetString() : "unknown error";
                            _logAction?.Invoke($"Preview: unavailable — {error}");
                            PreviewUnavailable?.Invoke(error);
                        }
                        break;

                    default:
                        _logAction?.Invoke($"Preview: unhandled message type '{msgType}'");
                        break;
                }
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
            {
                _logAction?.Invoke($"Preview: malformed message ignored: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"PreviewBridge message error: {ex.Message}");
            }
        }

        // ── Internal ───────────────────────────────────────────────

        private static readonly JsonSerializerOptions _serializerOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
        };

        private void PostMessage(object payload)
        {
            try
            {
                _postMessageAction(JsonSerializer.Serialize(payload, _serializerOptions));
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"PreviewBridge PostMessage error: {ex.Message}");
            }
        }
    }
}
