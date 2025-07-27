using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using System.Diagnostics;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Theme definition for multi-layer feature service
    /// </summary>
    public class ThemeDefinition
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string S3Path { get; set; }
        public string GeometryType { get; set; }
        public string[] Fields { get; set; }
    }

    /// <summary>
    /// Lightweight HTTP server that bridges ArcGIS Pro requests to DuckDB
    /// Implements ArcGIS REST API Feature Service specification using HttpListener
    /// </summary>
    public class FeatureServiceBridge : IDisposable
    {
        private readonly DataProcessor _dataProcessor;
        private HttpListener _listener;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly int _port;
        private bool _isRunning;
        private Task _listenerTask;

        // ArcGIS Feature Service constants
        private const int _maxRecordCount = 1000;
        private readonly object _spatialReference = new
        {
            wkid = 4326,
            latestWkid = 4326
        };

        // JSON serialization options
        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        // Multi-layer theme definitions
        private readonly List<ThemeDefinition> _themes = new List<ThemeDefinition>
        {
            new ThemeDefinition 
            { 
                Id = 0, 
                Name = "Places", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=places/type=place/*.parquet",
                GeometryType = "esriGeometryPoint",
                Fields = new[] { "id", "names", "categories", "websites", "emails", "phones", "brand", "addresses", "sources" }
            },
            new ThemeDefinition 
            { 
                Id = 1, 
                Name = "Buildings", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=buildings/type=building/*.parquet",
                GeometryType = "esriGeometryPolygon",
                Fields = new[] { "id", "names", "height", "num_floors", "class", "sources" }
            },
            new ThemeDefinition 
            { 
                Id = 2, 
                Name = "Addresses", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=addresses/type=address/*.parquet",
                GeometryType = "esriGeometryPoint",
                Fields = new[] { "id", "number", "address_levels", "sources" }
            },
            new ThemeDefinition 
            { 
                Id = 3, 
                Name = "Transportation - Roads", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=transportation/type=segment/*.parquet",
                GeometryType = "esriGeometryPolyline",
                Fields = new[] { "id", "names", "routes", "subtype", "class", "sources" }
            },
            new ThemeDefinition 
            { 
                Id = 4, 
                Name = "Transportation - Connectors", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=transportation/type=connector/*.parquet",
                GeometryType = "esriGeometryPoint",
                Fields = new[] { "id", "sources" }
            },
            new ThemeDefinition 
            { 
                Id = 5, 
                Name = "Base - Land", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=base/type=land/*.parquet",
                GeometryType = "esriGeometryPolygon",
                Fields = new[] { "id", "names", "class", "subtype", "sources" }
            },
            new ThemeDefinition 
            { 
                Id = 6, 
                Name = "Base - Water", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=base/type=water/*.parquet",
                GeometryType = "esriGeometryPolygon",
                Fields = new[] { "id", "names", "class", "subtype", "sources" }
            }
        };

        // In-memory cache status - Thread-safe async synchronization
        private bool _dataLoaded = false;
        private readonly SemaphoreSlim _loadSemaphore = new SemaphoreSlim(1, 1);
        private readonly object _loadLock = new object();

        public FeatureServiceBridge(DataProcessor dataProcessor, int port = 8080)
        {
            _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
            _port = port;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public string ServiceUrl => $"http://localhost:{_port}/arcgis/rest/services/overture/FeatureServer";

        /// <summary>
        /// Ensures in-memory data tables are loaded for current map viewport (like existing data loader)
        /// Thread-safe async implementation to prevent race conditions
        /// </summary>
        private async Task EnsureDataLoadedAsync()
        {
            // Fast path - if already loaded, return immediately
            if (_dataLoaded) return;

            // Thread-safe async synchronization - only one thread can load data at a time
            await _loadSemaphore.WaitAsync();
            try 
            {
                // Double-check after acquiring semaphore - another thread might have loaded data
                if (_dataLoaded) return;

                Debug.WriteLine($"🔄 Getting current map viewport to determine caching region (like existing data loader)...");

                // Get current map extent - same approach as existing data loader
                ArcGIS.Core.Geometry.Envelope extent = null;
                await QueuedTask.Run(() =>
                {
                    if (MapView.Active != null)
                    {
                        var mapExtent = MapView.Active.Extent;
                        if (mapExtent != null)
                        {
                            // Project to WGS84 if needed (same as existing data loader)
                            var wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                            if (mapExtent.SpatialReference == null || mapExtent.SpatialReference.Wkid != 4326)
                            {
                                extent = GeometryEngine.Instance.Project(mapExtent, wgs84) as ArcGIS.Core.Geometry.Envelope;
                            }
                            else
                            {
                                extent = mapExtent;
                            }
                        }
                    }
                });

                if (extent == null)
                {
                    Debug.WriteLine($"❌ No active map view found - cannot determine viewport for caching");
                    return;
                }

                // Add small buffer around viewport for better user experience
                var buffer = 0.01; // ~1km buffer
                var boundsXmin = extent.XMin - buffer;
                var boundsYmin = extent.YMin - buffer;
                var boundsXmax = extent.XMax + buffer;
                var boundsYmax = extent.YMax + buffer;

                Debug.WriteLine($"🎯 Loading data for CURRENT VIEWPORT: ({boundsXmin:F4}, {boundsYmin:F4}) to ({boundsXmax:F4}, {boundsYmax:F4})");
                Debug.WriteLine($"📏 Viewport size: {(boundsXmax - boundsXmin):F4}° × {(boundsYmax - boundsYmin):F4}° - much smaller than California!");

                // Validate DuckDB connection before proceeding
                try
                {
                    await ValidateDuckDbConnectionAsync();
                }
                catch (Exception connEx)
                {
                    Debug.WriteLine($"❌ DuckDB connection validation failed: {connEx.Message}");
                    throw new InvalidOperationException("DuckDB connection is not available", connEx);
                }

                // Load each theme into in-memory table for fast querying - VIEWPORT ONLY
                int successfulTables = 0;
                foreach (var theme in _themes)
                {
                    var tableName = $"theme_{theme.Id}_{theme.Name.Replace(" ", "_").Replace("-", "_").ToLowerInvariant()}";
                    Debug.WriteLine($"📥 Loading {theme.Name} for current viewport into '{tableName}'...");

                    try
                    {
                        // Create in-memory table with VIEWPORT BOUNDS - same pattern as existing data loader
                        var createTableQuery = $@"
                            CREATE OR REPLACE TABLE {tableName} AS 
                            SELECT * FROM read_parquet('{theme.S3Path}')
                            WHERE bbox.xmin IS NOT NULL AND bbox.ymin IS NOT NULL 
                              AND bbox.xmax IS NOT NULL AND bbox.ymax IS NOT NULL
                              AND bbox.xmin <= {boundsXmax.ToString("G", CultureInfo.InvariantCulture)} 
                              AND bbox.xmax >= {boundsXmin.ToString("G", CultureInfo.InvariantCulture)}
                              AND bbox.ymin <= {boundsYmax.ToString("G", CultureInfo.InvariantCulture)} 
                              AND bbox.ymax >= {boundsYmin.ToString("G", CultureInfo.InvariantCulture)}";

                        await _dataProcessor.ExecuteQueryAsync(createTableQuery);
                        Debug.WriteLine($"✅ {theme.Name} viewport data loaded successfully into '{tableName}'");
                        successfulTables++;
                    }
                    catch (Exception tableEx)
                    {
                        Debug.WriteLine($"⚠️ Failed to load {theme.Name} into table '{tableName}': {tableEx.Message}");
                        // Continue with other themes even if one fails
                    }
                }

                // Only mark as loaded if at least one table was created successfully
                if (successfulTables > 0)
                {
                    _dataLoaded = true;
                    Debug.WriteLine($"🎉 Current viewport data loaded into DuckDB cache! ({successfulTables}/{_themes.Count} themes loaded successfully) ⚡");
                }
                else
                {
                    Debug.WriteLine($"❌ No themes loaded successfully - data will fallback to S3 queries");
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"❌ Critical error loading viewport data into in-memory tables: {ex.Message}");
                Debug.WriteLine($"   Exception type: {ex.GetType().Name}");
                if (ex.InnerException != null)
                {
                    Debug.WriteLine($"   Inner exception: {ex.InnerException.Message}");
                }
                // Don't set _dataLoaded = true on error, so it can retry
                throw; // Re-throw to allow caller to handle
            }
            finally 
            {
                _loadSemaphore.Release();
            }
        }

        /// <summary>
        /// Validates that the DuckDB connection is healthy and ready for queries
        /// </summary>
        private async Task ValidateDuckDbConnectionAsync()
        {
            try
            {
                // Simple query to test connection health
                var testResults = await _dataProcessor.ExecuteQueryAsync("SELECT 1 as test");
                if (testResults == null || testResults.Count == 0)
                {
                    throw new InvalidOperationException("DuckDB connection test failed - no results returned");
                }
                Debug.WriteLine("✅ DuckDB connection validation successful");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"❌ DuckDB connection validation failed: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Gets the in-memory table name for a theme
        /// </summary>
        private string GetTableName(ThemeDefinition theme)
        {
            return $"theme_{theme.Id}_{theme.Name.Replace(" ", "_").Replace("-", "_").ToLowerInvariant()}";
        }

        /// <summary>
        /// Starts the HTTP server
        /// </summary>
        public Task StartAsync()
        {
            if (_isRunning) return Task.CompletedTask;

            try
            {
                _listener = new HttpListener();
                _listener.Prefixes.Add($"http://localhost:{_port}/");
                _listener.Start();

                Debug.WriteLine($"Starting DuckDB Feature Service Bridge on {ServiceUrl}");

                // Start listening for requests
                _listenerTask = Task.Run(async () => await ListenForRequestsAsync(_cancellationTokenSource.Token));

                _isRunning = true;
                Debug.WriteLine($"✅ Feature Service Bridge ready: {ServiceUrl}");
                
                // Initialize in-memory data tables in background for better performance
                _ = Task.Run(async () => 
                {
                    try
                    {
                        await EnsureDataLoadedAsync();
                        Debug.WriteLine("📦 Background in-memory cache initialization completed successfully");
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"⚠️ Background in-memory cache initialization failed: {ex.Message}");
                        Debug.WriteLine($"   Service will use S3 fallback queries for all requests");
                    }
                });

                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to start Feature Service Bridge: {ex.Message}");
                return Task.FromException(ex);
            }
        }

        /// <summary>
        /// Stops the HTTP server
        /// </summary>
        public async Task StopAsync()
        {
            if (!_isRunning) return;

            Debug.WriteLine("Stopping DuckDB Feature Service Bridge...");

            _cancellationTokenSource.Cancel();

            if (_listener != null)
            {
                _listener.Stop();
                _listener.Close();
            }

            if (_listenerTask != null)
            {
                try
                {
                    await _listenerTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancelled
                }
            }

            _isRunning = false;
            Debug.WriteLine("✅ Feature Service Bridge stopped");
        }

        /// <summary>
        /// Main request listening loop
        /// </summary>
        private async Task ListenForRequestsAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && _listener.IsListening)
            {
                try
                {
                    var contextTask = _listener.GetContextAsync();
                    
                    // Add timeout to prevent hanging connections
                    using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                    using (var combined = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token))
                    {
                        var context = await contextTask.ConfigureAwait(false);

                        // Handle request on background thread with timeout
                        _ = Task.Run(async () => 
                        {
                            try
                            {
                                await HandleRequestAsync(context);
                            }
                            catch (Exception ex)
                            {
                                Debug.WriteLine($"⚠️ Background request handling failed: {ex.Message}");
                                try { context.Response.Close(); } catch { }
                            }
                        }, combined.Token);
                    }
                }
                catch (ObjectDisposedException)
                {
                    // Listener was disposed, exit gracefully
                    break;
                }
                catch (HttpListenerException ex) when (ex.ErrorCode == 995) // WSA_OPERATION_ABORTED
                {
                    // Listener was stopped, exit gracefully  
                    break;
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Error in request listener: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Routes incoming HTTP requests to appropriate handlers
        /// </summary>
        private async Task HandleRequestAsync(HttpListenerContext context)
        {
            try
            {
                var request = context.Request;
                var response = context.Response;

                // Add CORS headers
                response.Headers.Add("Access-Control-Allow-Origin", "*");
                response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
                response.Headers.Add("Access-Control-Allow-Headers", "Content-Type");

                // Handle preflight requests
                if (request.HttpMethod == "OPTIONS")
                {
                    response.StatusCode = 200;
                    response.Close();
                    return;
                }

                var path = request.Url.AbsolutePath.ToLowerInvariant();
                Debug.WriteLine($"Feature Service Request: {request.HttpMethod} {path}");

                // Route to appropriate handler
                if (path == "/arcgis/rest/services/overture/featureserver")
                {
                    await HandleServiceMetadata(context);
                }
                else if (path.StartsWith("/arcgis/rest/services/overture/featureserver/") && path.EndsWith("/query"))
                {
                    // Extract layer ID from path like "/arcgis/rest/services/overture/featureserver/0/query"
                    var layerId = ExtractLayerIdFromPath(path);
                    await HandleQuery(context, layerId);
                }
                else if (path.StartsWith("/arcgis/rest/services/overture/featureserver/") && !path.EndsWith("/query"))
                {
                    // Extract layer ID from path like "/arcgis/rest/services/overture/featureserver/0"
                    var layerId = ExtractLayerIdFromPath(path);
                    await HandleLayerMetadata(context, layerId);
                }
                else if (path == "/health")
                {
                    await HandleHealthCheck(context);
                }
                else
                {
                    // 404 Not Found
                    response.StatusCode = 404;
                    var errorJson = JsonSerializer.Serialize(new { error = new { code = 404, message = "Endpoint not found" } }, _jsonOptions);
                    await WriteJsonResponse(context, errorJson);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error handling request: {ex.Message}");
                try
                {
                    context.Response.StatusCode = 500;
                    var errorJson = JsonSerializer.Serialize(new { error = new { code = 500, message = ex.Message } }, _jsonOptions);
                    await WriteJsonResponse(context, errorJson);
                }
                catch
                {
                    // If we can't even write an error response, just close the connection
                    context.Response.Close();
                }
            }
        }

        /// <summary>
        /// Handles Feature Service root requests - returns service metadata
        /// </summary>
        private async Task HandleServiceMetadata(HttpListenerContext context)
        {
            try
            {
                var serviceMetadata = new
                {
                    currentVersion = 11.1,
                    serviceDescription = "DuckDB Multi-Layer Feature Service - Overture Maps",
                    hasVersionedData = false,
                    supportsDisconnectedEditing = false,
                    supportsDatumTransformation = true,
                    supportsReturnDeleteResults = false,
                    hasStaticData = false,
                    maxRecordCount = _maxRecordCount,
                    supportedQueryFormats = "JSON,geoJSON,PBF",
                    capabilities = "Query",
                    description = "Live cloud-native access to all Overture Maps themes via DuckDB",
                    copyrightText = "Data from Overture Maps Foundation via DuckDB",
                    spatialReference = _spatialReference,
                    initialExtent = new
                    {
                        xmin = -180.0,
                        ymin = -90.0,
                        xmax = 180.0,
                        ymax = 90.0,
                        spatialReference = _spatialReference
                    },
                    fullExtent = new
                    {
                        xmin = -180.0,
                        ymin = -90.0,
                        xmax = 180.0,
                        ymax = 90.0,
                        spatialReference = _spatialReference
                    },
                    allowGeometryUpdates = false,
                    units = "esriDecimalDegrees",
                    layers = _themes.Select(t => new { id = t.Id, name = t.Name, type = "Feature Layer" }).ToArray(),
                    tables = new object[] { }
                };

                var json = JsonSerializer.Serialize(serviceMetadata, _jsonOptions);
                await WriteJsonResponse(context, json);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error handling service metadata request: {ex.Message}");
                context.Response.StatusCode = 500;
                var errorJson = JsonSerializer.Serialize(new { error = new { code = 500, message = ex.Message } }, _jsonOptions);
                await WriteJsonResponse(context, errorJson);
            }
        }

        /// <summary>
        /// Extract layer ID from URL path
        /// </summary>
        private int ExtractLayerIdFromPath(string path)
        {
            // Extract layer ID from paths like "/arcgis/rest/services/overture/featureserver/0" or "/arcgis/rest/services/overture/featureserver/0/query"
            var segments = path.Split('/');
            for (int i = 0; i < segments.Length; i++)
            {
                if (segments[i].Equals("featureserver", StringComparison.OrdinalIgnoreCase) && i + 1 < segments.Length)
                {
                    if (int.TryParse(segments[i + 1], out int layerId))
                    {
                        return layerId;
                    }
                }
            }
            return 0; // Default to layer 0
        }

        /// <summary>
        /// Handles Layer metadata requests
        /// </summary>
        private async Task HandleLayerMetadata(HttpListenerContext context, int layerId)
        {
            try
            {
                // Add CORS headers for HEAD requests
                context.Response.Headers.Add("Access-Control-Allow-Origin", "*");
                context.Response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS");
                context.Response.Headers.Add("Access-Control-Allow-Headers", "Content-Type");

                // Handle HEAD requests without body
                if (context.Request.HttpMethod == "HEAD")
                {
                    context.Response.StatusCode = 200;
                    context.Response.Close();
                    return;
                }

                // Find the theme for this layer
                var theme = _themes.FirstOrDefault(t => t.Id == layerId);
                if (theme == null)
                {
                    context.Response.StatusCode = 404;
                    var errorJson = JsonSerializer.Serialize(new { error = new { code = 404, message = $"Layer {layerId} not found" } }, _jsonOptions);
                    await WriteJsonResponse(context, errorJson);
                    return;
                }

                var layerMetadata = new
                {
                    currentVersion = 11.1,
                    id = theme.Id,
                    name = theme.Name,
                    type = "Feature Layer",
                    description = $"Live cloud-native {theme.Name} data from Overture Maps via DuckDB",
                    geometryType = theme.GeometryType,
                    sourceSpatialReference = _spatialReference,
                    copyrightText = "Overture Maps Foundation",
                    parentLayer = (object)null,
                    subLayers = new object[] { },
                    minScale = 0,
                    maxScale = 0,
                    defaultVisibility = true,
                    extent = new
                    {
                        xmin = -180.0,
                        ymin = -90.0,
                        xmax = 180.0,
                        ymax = 90.0,
                        spatialReference = _spatialReference
                    },
                    hasAttachments = false,
                    htmlPopupType = "esriServerHTMLPopupTypeAsHTMLText",
                    displayField = "id",
                    typeIdField = (string)null,
                    subtypeField = (string)null,
                    fields = GetFieldDefinitions(theme),
                    canModifyLayer = false,
                    canScaleSymbols = false,
                    hasLabels = false,
                    capabilities = "Query",
                    maxRecordCount = _maxRecordCount,
                    supportsStatistics = false,
                    supportsAdvancedQueries = false,
                    supportedQueryFormats = "JSON,geoJSON,PBF",
                    isDataVersioned = false,
                    useStandardizedQueries = true,
                    supportedSpatialRelationships = new string[]
                    {
                        "esriSpatialRelIntersects",
                        "esriSpatialRelContains",
                        "esriSpatialRelCrosses",
                        "esriSpatialRelEnvelopeIntersects",
                        "esriSpatialRelOverlaps",
                        "esriSpatialRelTouches",
                        "esriSpatialRelWithin"
                    },
                    advancedQueryCapabilities = new
                    {
                        useStandardizedQueries = true,
                        supportsStatistics = false,
                        supportsPercentileStatistics = false,
                        supportsHavingClause = false,
                        supportsCountDistinct = false,
                        supportsOrderBy = false,
                        supportsDistinct = false,
                        supportsPagination = false,
                        supportsTrueCurve = false,
                        supportsReturningQueryExtent = true,
                        supportsQueryWithDistance = false,
                        supportsSqlExpression = false
                    }
                };

                var json = JsonSerializer.Serialize(layerMetadata, _jsonOptions);
                await WriteJsonResponse(context, json);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error handling layer metadata request: {ex.Message}");
                context.Response.StatusCode = 500;
                var errorJson = JsonSerializer.Serialize(new { error = new { code = 500, message = ex.Message } }, _jsonOptions);
                await WriteJsonResponse(context, errorJson);
            }
        }

        /// <summary>
        /// Handles feature queries - the main data retrieval endpoint
        /// </summary>
        private async Task HandleQuery(HttpListenerContext context, int layerId)
        {
            try
            {
                // Find the theme for this layer
                var theme = _themes.FirstOrDefault(t => t.Id == layerId);
                if (theme == null)
                {
                    context.Response.StatusCode = 404;
                    var errorJson = JsonSerializer.Serialize(new { error = new { code = 404, message = $"Layer {layerId} not found" } }, _jsonOptions);
                    await WriteJsonResponse(context, errorJson);
                    return;
                }

                // Ensure data is loaded into in-memory tables with graceful fallback
                try
                {
                    await EnsureDataLoadedAsync();
                }
                catch (Exception loadEx)
                {
                    Debug.WriteLine($"⚠️ Failed to load in-memory cache for layer {layerId} ({theme.Name}): {loadEx.Message}");
                    Debug.WriteLine($"   Will use S3 fallback queries instead");
                    // Continue with S3 fallback - don't fail the entire request
                }

                var queryParams = ParseQueryParameters(context.Request);
                
                // Extract query parameters
                var whereClause = GetQueryParam(queryParams, "where") ?? "1=1";
                var outFields = GetQueryParam(queryParams, "outFields") ?? "*";
                var geometryParam = GetQueryParam(queryParams, "geometry");
                var spatialRel = GetQueryParam(queryParams, "spatialRel") ?? "esriSpatialRelIntersects";
                var returnGeometry = GetQueryParam(queryParams, "returnGeometry")?.ToLowerInvariant() != "false";
                var maxRecords = int.TryParse(GetQueryParam(queryParams, "resultRecordCount"), out var max) ? max : _maxRecordCount;
                var format = GetQueryParam(queryParams, "f") ?? "json";

                Debug.WriteLine($"Layer {layerId} ({theme.Name}) Query: where={whereClause}, outFields={outFields}, geometry={geometryParam}, spatialRel={spatialRel}");

                // Build DuckDB query
                var duckDbQuery = BuildDuckDbQuery(theme, whereClause, geometryParam, spatialRel, outFields, maxRecords, returnGeometry);
                
                // Execute query
                var features = await ExecuteDuckDbQuery(duckDbQuery);

                // Build ArcGIS REST API compliant response
                var response = new
                {
                    objectIdFieldName = "OBJECTID",
                    uniqueIdField = new { name = "OBJECTID", isSystemMaintained = true },
                    globalIdFieldName = "",
                    geometryType = theme.GeometryType,
                    spatialReference = _spatialReference,
                    hasZ = false,
                    hasM = false,
                    fields = GetFieldDefinitions(theme),
                    displayFieldName = "id",
                    features = features,
                    exceededTransferLimit = features.Count >= maxRecords
                };

                var json = JsonSerializer.Serialize(response, _jsonOptions);
                
                // Debug: Log a sample of the response for troubleshooting
                var debugJson = json.Length > 500 ? json.Substring(0, 500) + "..." : json;
                Debug.WriteLine($"🔍 Layer {layerId} JSON Response Sample: {debugJson}");
                
                await WriteJsonResponse(context, json);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error handling query request for layer {layerId}: {ex.Message}");
                context.Response.StatusCode = 500;
                var errorJson = JsonSerializer.Serialize(new { error = new { code = 500, message = ex.Message } }, _jsonOptions);
                await WriteJsonResponse(context, errorJson);
            }
        }

        /// <summary>
        /// Health check endpoint
        /// </summary>
        private async Task HandleHealthCheck(HttpListenerContext context)
        {
            var healthResponse = new { status = "healthy", service = "DuckDB Feature Service Bridge" };
            var json = JsonSerializer.Serialize(healthResponse, _jsonOptions);
            await WriteJsonResponse(context, json);
        }

        /// <summary>
        /// Parse query parameters from the request
        /// </summary>
        private Dictionary<string, string> ParseQueryParameters(HttpListenerRequest request)
        {
            var queryParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Parse URL query string
            if (request.Url?.Query != null)
            {
                var query = HttpUtility.ParseQueryString(request.Url.Query);
                foreach (string key in query.AllKeys)
                {
                    if (key != null)
                    {
                        queryParams[key] = query[key];
                    }
                }
            }

            // Parse POST body if present
            if (request.HttpMethod == "POST" && request.HasEntityBody)
            {
                using (var reader = new StreamReader(request.InputStream, request.ContentEncoding))
                {
                    var body = reader.ReadToEnd();
                    var postQuery = HttpUtility.ParseQueryString(body);
                    foreach (string key in postQuery.AllKeys)
                    {
                        if (key != null)
                        {
                            queryParams[key] = postQuery[key]; // POST params override URL params
                        }
                    }
                }
            }

            return queryParams;
        }

        /// <summary>
        /// Get a query parameter value
        /// </summary>
        private string GetQueryParam(Dictionary<string, string> queryParams, string key)
        {
            return queryParams.TryGetValue(key, out var value) ? value : null;
        }

        /// <summary>
        /// Build DuckDB SQL query from ArcGIS parameters - In-memory high-performance querying
        /// </summary>
        private string BuildDuckDbQuery(ThemeDefinition theme, string whereClause, string geometryParam, string spatialRel, string outFields, int maxRecords, bool returnGeometry)
        {
            var query = new StringBuilder();
            query.Append("SELECT ");

            // Handle output fields - Updated for theme-specific schemas
            if (outFields == "*")
            {
                // Use theme-specific fields with common bbox and geometry
                var fieldList = new List<string> { "id" };
                
                // Add bbox fields that are common to all themes
                fieldList.AddRange(new[] { "bbox.xmin as bbox_xmin", "bbox.ymin as bbox_ymin", "bbox.xmax as bbox_xmax", "bbox.ymax as bbox_ymax" });
                
                // Add theme-specific fields
                foreach (var field in theme.Fields.Where(f => f != "id"))
                {
                    fieldList.Add(field);
                }
                
                // Add geometry only if requested
                if (returnGeometry)
                {
                    fieldList.Add("ST_AsText(geometry) as geometry_wkt");
                }
                
                query.Append(string.Join(", ", fieldList));
            }
            else if (outFields == "OBJECTID")
            {
                query.Append("id"); // Use id as the basis for OBJECTID
            }
            else
            {
                // For specific field requests, add geometry if needed
                if (returnGeometry && !outFields.Contains("geometry"))
                {
                    query.Append($"{outFields}, ST_AsText(geometry) as geometry_wkt");
                }
                else
                {
                    query.Append(outFields);
                }
            }

            // Check if in-memory table exists, otherwise fallback to S3
            var tableName = GetTableName(theme);
            if (_dataLoaded)
            {
                // IN-MEMORY: Query from pre-loaded in-memory table for lightning-fast performance
                query.Append($" FROM {tableName}");
            }
            else
            {
                // FALLBACK: Direct S3 query if in-memory data not loaded yet
                query.Append($" FROM read_parquet('{theme.S3Path}')");
                Debug.WriteLine($"⚠️ Fallback to S3 for {theme.Name} - in-memory data not loaded yet");
            }

            var conditions = new List<string>();

            // Handle WHERE clause
            if (!string.IsNullOrEmpty(whereClause) && whereClause != "1=1")
            {
                // Basic WHERE clause translation (this could be expanded)
                conditions.Add($"({whereClause})");
            }

            // Handle spatial geometry filtering
            if (!string.IsNullOrEmpty(geometryParam))
            {
                var spatialCondition = ConvertArcGISGeometryToSql(geometryParam, spatialRel);
                if (!string.IsNullOrEmpty(spatialCondition))
                {
                    conditions.Add(spatialCondition);
                }
            }

            if (conditions.Count > 0)
            {
                query.Append(" WHERE ");
                query.Append(string.Join(" AND ", conditions));
            }

            query.Append($" LIMIT {maxRecords}");

            if (_dataLoaded)
            {
                Debug.WriteLine($"⚡ In-memory query for {theme.Name} (table: {tableName}): {query}");
            }
            else
            {
                Debug.WriteLine($"📡 S3 fallback query for {theme.Name}: {query}");
            }
            
            return query.ToString();
        }

        /// <summary>
        /// Convert ArcGIS geometry to DuckDB spatial SQL
        /// </summary>
        private string ConvertArcGISGeometryToSql(string geometryJson, string spatialRel)
        {
            try
            {
                using (var doc = JsonDocument.Parse(geometryJson))
                {
                    var root = doc.RootElement;

                    if (root.TryGetProperty("xmin", out var xminProp) &&
                        root.TryGetProperty("ymin", out var yminProp) &&
                        root.TryGetProperty("xmax", out var xmaxProp) &&
                        root.TryGetProperty("ymax", out var ymaxProp))
                    {
                        // Envelope geometry - Updated for Overture Maps bbox struct
                        var xmin = xminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymin = yminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var xmax = xmaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymax = ymaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);

                        return spatialRel.ToLowerInvariant() switch
                        {
                            "esrispatialrelintersects" or "esrispatialrelenvelopeintersects" =>
                                $"(bbox.xmin <= {xmax} AND bbox.xmax >= {xmin} AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin})",
                            "esrispatialrelcontains" =>
                                $"(bbox.xmin >= {xmin} AND bbox.xmax <= {xmax} AND bbox.ymin >= {ymin} AND bbox.ymax <= {ymax})",
                            "esrispatialrelwithin" =>
                                $"(bbox.xmin <= {xmin} AND bbox.xmax >= {xmax} AND bbox.ymin <= {ymin} AND bbox.ymax >= {ymax})",
                            _ => $"(bbox.xmin <= {xmax} AND bbox.xmax >= {xmin} AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin})"
                        };
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error parsing geometry JSON: {ex.Message}");
            }

            return string.Empty;
        }

        /// <summary>
        /// Execute DuckDB query and return features
        /// </summary>
        private async Task<List<object>> ExecuteDuckDbQuery(string query)
        {
            var features = new List<object>();

            try
            {
                Debug.WriteLine($"Executing DuckDB query: {query}");

                var queryResults = await _dataProcessor.ExecuteQueryAsync(query);
                
                var objectId = 1;
                foreach (var row in queryResults)
                {
                    var attributes = new Dictionary<string, object>
                    {
                        ["OBJECTID"] = objectId++
                    };

                    object geometry = null;

                    // Add all columns from the query result and extract geometry
                    foreach (var kvp in row)
                    {
                        if (kvp.Key == "geometry_wkt" && kvp.Value != null)
                        {
                            // Parse WKT geometry into ArcGIS geometry format
                            geometry = ParseWktToArcGISGeometry(kvp.Value.ToString());
                        }
                        else if (kvp.Key != "geometry_wkt") // Don't include WKT in attributes
                        {
                            // Clean attribute values for JSON serialization
                            var cleanValue = kvp.Value;
                            if (cleanValue == null || cleanValue == DBNull.Value)
                            {
                                cleanValue = null;
                            }
                            else if (cleanValue is string str && string.IsNullOrEmpty(str))
                            {
                                cleanValue = null;
                            }
                            
                            attributes[kvp.Key] = cleanValue;
                        }
                    }

                    // Build ArcGIS REST API compliant feature
                    object feature;
                    if (geometry != null)
                    {
                        feature = new
                        {
                            attributes = attributes,
                            geometry = geometry
                        };
                    }
                    else
                    {
                        // For features without geometry, omit geometry field entirely
                        feature = new
                        {
                            attributes = attributes
                        };
                    }

                    features.Add(feature);
                }

                Debug.WriteLine($"Returned {features.Count} features");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error executing DuckDB query: {ex.Message}");
                throw;
            }

            return features;
        }

        /// <summary>
        /// Parse WKT geometry string into ArcGIS REST API geometry format
        /// </summary>
        private object ParseWktToArcGISGeometry(string wkt)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(wkt))
                    return null;

                // Simple WKT parsing for basic geometry types
                wkt = wkt.Trim().ToUpperInvariant();

                if (wkt.StartsWith("POINT"))
                {
                    // Parse POINT(x y) format
                    var coords = ExtractCoordinatesFromWkt(wkt);
                    if (coords.Count >= 2)
                    {
                        return new
                        {
                            x = coords[0],
                            y = coords[1],
                            spatialReference = _spatialReference
                        };
                    }
                }
                else if (wkt.StartsWith("MULTIPOINT"))
                {
                    // Parse MULTIPOINT((x1 y1), (x2 y2), ...) format
                    // For simplicity, extract the first point from the multipoint
                    var coords = ExtractCoordinatesFromWkt(wkt);
                    if (coords.Count >= 2)
                    {
                        return new
                        {
                            x = coords[0],
                            y = coords[1],
                            spatialReference = _spatialReference
                        };
                    }
                }
                else if (wkt.StartsWith("LINESTRING"))
                {
                    // Parse LINESTRING(x1 y1, x2 y2, ...) format
                    var coords = ExtractCoordinatesFromWkt(wkt);
                    var paths = new List<double[]>();
                    for (int i = 0; i < coords.Count; i += 2)
                    {
                        if (i + 1 < coords.Count)
                        {
                            paths.Add(new double[] { coords[i], coords[i + 1] });
                        }
                    }
                    
                    return new
                    {
                        paths = new[] { paths.ToArray() },
                        spatialReference = _spatialReference
                    };
                }
                else if (wkt.StartsWith("MULTILINESTRING"))
                {
                    // Parse MULTILINESTRING((x1 y1, x2 y2), (x3 y3, x4 y4)) format
                    // For simplicity, extract the first linestring from the multilinestring
                    var firstLineStart = wkt.IndexOf("(");
                    if (firstLineStart >= 0)
                    {
                        var lineStart = firstLineStart + 1;
                        var parenCount = 1;
                        var lineEnd = lineStart;
                        
                        // Find the matching closing parenthesis for the first linestring
                        for (int i = lineStart + 1; i < wkt.Length && parenCount > 0; i++)
                        {
                            if (wkt[i] == '(') parenCount++;
                            else if (wkt[i] == ')') parenCount--;
                            lineEnd = i;
                        }
                        
                        if (parenCount == 0)
                        {
                            // Extract first linestring coordinates
                            var lineWkt = "LINESTRING(" + wkt.Substring(lineStart, lineEnd - lineStart) + ")";
                            var coords = ExtractCoordinatesFromWkt(lineWkt);
                            var paths = new List<double[]>();
                            for (int i = 0; i < coords.Count; i += 2)
                            {
                                if (i + 1 < coords.Count)
                                {
                                    paths.Add(new double[] { coords[i], coords[i + 1] });
                                }
                            }
                            
                            return new
                            {
                                paths = new[] { paths.ToArray() },
                                spatialReference = _spatialReference
                            };
                        }
                    }
                    
                    Debug.WriteLine($"⚠️ Failed to parse MULTILINESTRING geometry");
                    return null;
                }
                else if (wkt.StartsWith("POLYGON"))
                {
                    // Parse POLYGON((x1 y1, x2 y2, ..., x1 y1)) format
                    var coords = ExtractCoordinatesFromWkt(wkt);
                    var rings = new List<double[]>();
                    for (int i = 0; i < coords.Count; i += 2)
                    {
                        if (i + 1 < coords.Count)
                        {
                            rings.Add(new double[] { coords[i], coords[i + 1] });
                        }
                    }
                    
                    return new
                    {
                        rings = new[] { rings.ToArray() },
                        spatialReference = _spatialReference
                    };
                }
                else if (wkt.StartsWith("MULTIPOLYGON"))
                {
                    // Parse MULTIPOLYGON(((x1 y1, x2 y2, ..., x1 y1)), ((x1 y1, x2 y2, ..., x1 y1))) format
                    // For simplicity, extract the first polygon from the multipolygon
                    // Find the first polygon within the multipolygon
                    var firstPolygonStart = wkt.IndexOf("((");
                    if (firstPolygonStart >= 0)
                    {
                        var polygonStart = firstPolygonStart + 1; // Start from the inner (
                        var parenCount = 1;
                        var polygonEnd = polygonStart;
                        
                        // Find the matching closing parenthesis for the first polygon
                        for (int i = polygonStart + 1; i < wkt.Length && parenCount > 0; i++)
                        {
                            if (wkt[i] == '(') parenCount++;
                            else if (wkt[i] == ')') parenCount--;
                            polygonEnd = i;
                        }
                        
                        if (parenCount == 0)
                        {
                            // Extract first polygon coordinates
                            var polygonWkt = "POLYGON" + wkt.Substring(polygonStart, polygonEnd - polygonStart + 1);
                            var coords = ExtractCoordinatesFromWkt(polygonWkt);
                            var rings = new List<double[]>();
                            for (int i = 0; i < coords.Count; i += 2)
                            {
                                if (i + 1 < coords.Count)
                                {
                                    rings.Add(new double[] { coords[i], coords[i + 1] });
                                }
                            }
                            
                            return new
                            {
                                rings = new[] { rings.ToArray() },
                                spatialReference = _spatialReference
                            };
                        }
                    }
                    
                    Debug.WriteLine($"⚠️ Failed to parse MULTIPOLYGON geometry");
                    return null;
                }

                Debug.WriteLine($"⚠️ Unsupported WKT geometry type: {wkt.Substring(0, Math.Min(50, wkt.Length))}...");
                return null;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error parsing WKT geometry: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Extract coordinate values from WKT string
        /// </summary>
        private List<double> ExtractCoordinatesFromWkt(string wkt)
        {
            var coords = new List<double>();
            
            // Find coordinates between parentheses
            var start = wkt.IndexOf('(');
            var end = wkt.LastIndexOf(')');
            
            if (start >= 0 && end > start)
            {
                var coordString = wkt.Substring(start + 1, end - start - 1);
                
                // Handle nested parentheses for POLYGON
                if (coordString.StartsWith("("))
                {
                    var innerStart = coordString.IndexOf('(');
                    var innerEnd = coordString.IndexOf(')');
                    if (innerStart >= 0 && innerEnd > innerStart)
                    {
                        coordString = coordString.Substring(innerStart + 1, innerEnd - innerStart - 1);
                    }
                }
                
                // Parse coordinate pairs
                var parts = coordString.Split(',');
                foreach (var part in parts)
                {
                    var xy = part.Trim().Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                    if (xy.Length >= 2)
                    {
                        if (double.TryParse(xy[0], NumberStyles.Float, CultureInfo.InvariantCulture, out double x) &&
                            double.TryParse(xy[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double y))
                        {
                            coords.Add(x);
                            coords.Add(y);
                        }
                    }
                }
            }
            
            return coords;
        }

        /// <summary>
        /// Get field definitions for the layer - Theme-specific schema
        /// </summary>
        private object[] GetFieldDefinitions(ThemeDefinition theme)
        {
            var fields = new List<object>
            {
                new
                {
                    name = "OBJECTID",
                    type = "esriFieldTypeOID",
                    alias = "Object ID",
                    domain = (object)null,
                    editable = false,
                    nullable = false
                },
                new
                {
                    name = "id",
                    type = "esriFieldTypeString",
                    alias = "ID",
                    length = 255,
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "bbox_xmin",
                    type = "esriFieldTypeDouble",
                    alias = "BBox XMin",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "bbox_ymin",
                    type = "esriFieldTypeDouble",
                    alias = "BBox YMin",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "bbox_xmax",
                    type = "esriFieldTypeDouble",
                    alias = "BBox XMax",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "bbox_ymax",
                    type = "esriFieldTypeDouble",
                    alias = "BBox YMax",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                }
            };

            // Add theme-specific fields
            foreach (var field in theme.Fields.Where(f => f != "id"))
            {
                fields.Add(new
                {
                    name = field,
                    type = "esriFieldTypeString",
                    alias = field.Replace("_", " "),
                    length = 1000,
                    domain = (object)null,
                    editable = false,
                    nullable = true
                });
            }

            // Don't add geometry_wkt as a field - geometry is returned separately in ArcGIS REST API

            return fields.ToArray();
        }

        /// <summary>
        /// Write JSON response to HTTP context
        /// </summary>
        private async Task WriteJsonResponse(HttpListenerContext context, string json)
        {
            var response = context.Response;
            response.ContentType = "application/json";
            response.StatusCode = 200;

            var buffer = Encoding.UTF8.GetBytes(json);
            // Don't set ContentLength64 - let HttpListener handle it automatically to avoid mismatch
            // response.ContentLength64 = buffer.Length;

            await response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
            response.Close();
        }

        public bool IsRunning => _isRunning;

        public void Dispose()
        {
            if (_isRunning)
            {
                StopAsync().Wait();
            }

            _cancellationTokenSource?.Dispose();
            _listener?.Close();
            _loadSemaphore?.Dispose();
        }
    }
} 