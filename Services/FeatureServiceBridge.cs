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
using System.Diagnostics;

namespace DuckDBGeoparquet.Services
{
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

        public FeatureServiceBridge(DataProcessor dataProcessor, int port = 8080)
        {
            _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
            _port = port;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public string ServiceUrl => $"http://localhost:{_port}/arcgis/rest/services/overture/FeatureServer";

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
                    var context = await contextTask;

                    // Handle request on background thread
                    _ = Task.Run(async () => await HandleRequestAsync(context), cancellationToken);
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
                    await HandleQuery(context);
                }
                else if (path.StartsWith("/arcgis/rest/services/overture/featureserver/") && !path.EndsWith("/query"))
                {
                    await HandleLayerMetadata(context);
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
                    serviceDescription = "DuckDB GeoParquet Feature Service Bridge",
                    hasVersionedData = false,
                    supportsDisconnectedEditing = false,
                    supportsDatumTransformation = true,
                    supportsReturnDeleteResults = false,
                    hasStaticData = false,
                    maxRecordCount = _maxRecordCount,
                    supportedQueryFormats = "JSON,geoJSON,PBF",
                    capabilities = "Query",
                    description = "Live feature service bridge to DuckDB for cloud GeoParquet data",
                    copyrightText = "Data from Overture Maps via DuckDB",
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
                    layers = new object[]
                    {
                        new { id = 0, name = "Overture Maps Data", type = "Feature Layer" }
                    },
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
        /// Handles Layer metadata requests
        /// </summary>
        private async Task HandleLayerMetadata(HttpListenerContext context)
        {
            try
            {
                var layerMetadata = new
                {
                    currentVersion = 11.1,
                    id = 0,
                    name = "Overture Maps Data",
                    type = "Feature Layer",
                    description = "Live Overture Maps data from DuckDB",
                    geometryType = "esriGeometryPolygon",
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
                    displayField = "name",
                    typeIdField = (string)null,
                    subtypeField = (string)null,
                    fields = GetFieldDefinitions(),
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
        private async Task HandleQuery(HttpListenerContext context)
        {
            try
            {
                var queryParams = ParseQueryParameters(context.Request);
                
                // Extract query parameters
                var whereClause = GetQueryParam(queryParams, "where") ?? "1=1";
                var outFields = GetQueryParam(queryParams, "outFields") ?? "*";
                var geometryParam = GetQueryParam(queryParams, "geometry");
                var spatialRel = GetQueryParam(queryParams, "spatialRel") ?? "esriSpatialRelIntersects";
                var returnGeometry = GetQueryParam(queryParams, "returnGeometry")?.ToLowerInvariant() != "false";
                var maxRecords = int.TryParse(GetQueryParam(queryParams, "resultRecordCount"), out var max) ? max : _maxRecordCount;
                var format = GetQueryParam(queryParams, "f") ?? "json";

                Debug.WriteLine($"Query parameters: where={whereClause}, outFields={outFields}, geometry={geometryParam}, spatialRel={spatialRel}");

                // Build DuckDB query
                var duckDbQuery = BuildDuckDbQuery(whereClause, geometryParam, spatialRel, outFields, maxRecords);
                
                // Execute query
                var features = await ExecuteDuckDbQuery(duckDbQuery);

                // Build ArcGIS response
                var response = new
                {
                    objectIdFieldName = "OBJECTID",
                    uniqueIdField = new { name = "OBJECTID", isSystemMaintained = true },
                    globalIdFieldName = "",
                    geometryType = "esriGeometryPolygon",
                    spatialReference = _spatialReference,
                    hasZ = false,
                    hasM = false,
                    features = features,
                    exceededTransferLimit = features.Count >= maxRecords
                };

                var json = JsonSerializer.Serialize(response, _jsonOptions);
                await WriteJsonResponse(context, json);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error handling query request: {ex.Message}");
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
        /// Build DuckDB SQL query from ArcGIS parameters
        /// </summary>
        private string BuildDuckDbQuery(string whereClause, string geometryParam, string spatialRel, string outFields, int maxRecords)
        {
            var query = new StringBuilder();
            query.Append("SELECT ");

            // Handle output fields
            if (outFields == "*")
            {
                query.Append("id, \"bbox.xmin\", \"bbox.ymin\", \"bbox.xmax\", \"bbox.ymax\", name, place_type, ST_AsText(geometry) as geometry_wkt");
            }
            else
            {
                query.Append(outFields);
            }

            query.Append(" FROM current_table");

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
                        // Envelope geometry
                        var xmin = xminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymin = yminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var xmax = xmaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymax = ymaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);

                        return spatialRel.ToLowerInvariant() switch
                        {
                            "esrispatialrelintersects" or "esrispatialrelenvelopeintersects" =>
                                $"(\"bbox.xmin\" <= {xmax} AND \"bbox.xmax\" >= {xmin} AND \"bbox.ymin\" <= {ymax} AND \"bbox.ymax\" >= {ymin})",
                            "esrispatialrelcontains" =>
                                $"(\"bbox.xmin\" >= {xmin} AND \"bbox.xmax\" <= {xmax} AND \"bbox.ymin\" >= {ymin} AND \"bbox.ymax\" <= {ymax})",
                            "esrispatialrelwithin" =>
                                $"(\"bbox.xmin\" <= {xmin} AND \"bbox.xmax\" >= {xmax} AND \"bbox.ymin\" <= {ymin} AND \"bbox.ymax\" >= {ymax})",
                            _ => $"(\"bbox.xmin\" <= {xmax} AND \"bbox.xmax\" >= {xmin} AND \"bbox.ymin\" <= {ymax} AND \"bbox.ymax\" >= {ymin})"
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

                    // Add all columns from the query result
                    foreach (var kvp in row)
                    {
                        attributes[kvp.Key] = kvp.Value;
                    }

                    var feature = new
                    {
                        attributes = attributes,
                        geometry = (object)null // For now, we'll return null geometry
                    };

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
        /// Get field definitions for the layer
        /// </summary>
        private object[] GetFieldDefinitions()
        {
            return new object[]
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
                    name = "bbox.xmin",
                    type = "esriFieldTypeDouble",
                    alias = "BBox XMin",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "bbox.ymin",
                    type = "esriFieldTypeDouble",
                    alias = "BBox YMin",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "bbox.xmax",
                    type = "esriFieldTypeDouble",
                    alias = "BBox XMax",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "bbox.ymax",
                    type = "esriFieldTypeDouble",
                    alias = "BBox YMax",
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "name",
                    type = "esriFieldTypeString",
                    alias = "Name",
                    length = 255,
                    domain = (object)null,
                    editable = false,
                    nullable = true
                },
                new
                {
                    name = "place_type",
                    type = "esriFieldTypeString",
                    alias = "Place Type",
                    length = 255,
                    domain = (object)null,
                    editable = false,
                    nullable = true
                }
            };
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
            response.ContentLength64 = buffer.Length;

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
        }
    }
} 