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
using ArcGIS.Core.Geometry;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Lightweight HTTP server that bridges ArcGIS Pro requests to DuckDB
    /// Implements ArcGIS REST API Feature Service specification
    /// </summary>
    public class FeatureServiceBridge : IDisposable
    {
        private readonly DataProcessor _dataProcessor;
        private readonly ILogger<FeatureServiceBridge> _logger;
        private WebApplication _app;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly int _port;
        private bool _isRunning;

        // ArcGIS Feature Service JSON serialization options
        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        public FeatureServiceBridge(DataProcessor dataProcessor, int port = 8080, ILogger<FeatureServiceBridge> logger = null)
        {
            _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
            _port = port;
            _logger = logger;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public string ServiceUrl => $"http://localhost:{_port}/arcgis/rest/services/overture/FeatureServer";

        /// <summary>
        /// Starts the HTTP server
        /// </summary>
        public async Task StartAsync()
        {
            if (_isRunning) return;

            var builder = WebApplication.CreateBuilder();
            
            // Configure logging
            builder.Logging.ClearProviders();
            if (_logger != null)
            {
                builder.Logging.AddProvider(new CustomLoggerProvider(_logger));
            }

            // Configure CORS for ArcGIS Pro
            builder.Services.AddCors(options =>
            {
                options.AddDefaultPolicy(policy =>
                {
                    policy.AllowAnyOrigin()
                          .AllowAnyMethod()
                          .AllowAnyHeader();
                });
            });

            builder.WebHost.UseUrls($"http://localhost:{_port}");

            _app = builder.Build();
            _app.UseCors();

            // Configure ArcGIS REST API endpoints
            ConfigureEndpoints();

            _logger?.LogInformation($"Starting DuckDB Feature Service Bridge on {ServiceUrl}");
            
            await _app.StartAsync(_cancellationTokenSource.Token);
            _isRunning = true;

            _logger?.LogInformation($"✅ Feature Service Bridge ready: {ServiceUrl}");
        }

        /// <summary>
        /// Stops the HTTP server
        /// </summary>
        public async Task StopAsync()
        {
            if (!_isRunning) return;

            _logger?.LogInformation("Stopping DuckDB Feature Service Bridge...");
            
            _cancellationTokenSource.Cancel();
            
            if (_app != null)
            {
                await _app.StopAsync();
                await _app.DisposeAsync();
            }
            
            _isRunning = false;
            _logger?.LogInformation("✅ Feature Service Bridge stopped");
        }

        private void ConfigureEndpoints()
        {
            // Feature Service Root - Service metadata
            _app.MapGet("/arcgis/rest/services/overture/FeatureServer", HandleServiceMetadata);

            // Layer Info - Layer metadata  
            _app.MapGet("/arcgis/rest/services/overture/FeatureServer/{layerId:int}", HandleLayerMetadata);

            // Query Endpoint - The main query interface
            _app.MapGet("/arcgis/rest/services/overture/FeatureServer/{layerId:int}/query", HandleQuery);
            _app.MapPost("/arcgis/rest/services/overture/FeatureServer/{layerId:int}/query", HandleQuery);

            // Health check
            _app.MapGet("/health", () => Results.Ok(new { status = "healthy", service = "DuckDB Feature Service Bridge" }));
        }

        /// <summary>
        /// Handles Feature Service root requests - returns service metadata
        /// </summary>
        private async Task<IResult> HandleServiceMetadata(HttpContext context)
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
                    maxRecordCount = 1000,
                    supportedQueryFormats = "JSON,geoJSON,PBF",
                    capabilities = "Query",
                    description = "Live feature service bridge to DuckDB for cloud GeoParquet data",
                    copyrightText = "Data from Overture Maps via DuckDB",
                    spatialReference = new
                    {
                        wkid = 4326,
                        latestWkid = 4326
                    },
                    initialExtent = new
                    {
                        xmin = -180.0,
                        ymin = -90.0,
                        xmax = 180.0,
                        ymax = 90.0,
                        spatialReference = new { wkid = 4326, latestWkid = 4326 }
                    },
                    fullExtent = new
                    {
                        xmin = -180.0,
                        ymin = -90.0,
                        xmax = 180.0,
                        ymax = 90.0,
                        spatialReference = new { wkid = 4326, latestWkid = 4326 }
                    },
                    allowGeometryUpdates = false,
                    units = "esriDecimalDegrees",
                    layers = new object[]
                    {
                        new { id = 0, name = "Overture Maps Data" }
                    },
                    tables = new object[0]
                };

                var format = context.Request.Query["f"].FirstOrDefault() ?? "json";
                return await WriteJsonResponse(context, serviceMetadata, format);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error handling service metadata request");
                return Results.Problem($"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Handles Layer metadata requests
        /// </summary>
        private async Task<IResult> HandleLayerMetadata(HttpContext context, int layerId)
        {
            try
            {
                if (layerId != 0)
                {
                    return Results.NotFound(new { error = new { code = 400, message = "Invalid layer ID" } });
                }

                var layerMetadata = new
                {
                    currentVersion = 11.1,
                    id = 0,
                    name = "Overture Maps Data",
                    type = "Feature Layer",
                    displayField = "id",
                    description = "Live DuckDB query layer for cloud GeoParquet data",
                    copyrightText = "Data from Overture Maps via DuckDB",
                    geometryType = "esriGeometryPoint", // Default - will be dynamic based on query
                    minScale = 0,
                    maxScale = 0,
                    extent = new
                    {
                        xmin = -180.0,
                        ymin = -90.0,
                        xmax = 180.0,
                        ymax = 90.0,
                        spatialReference = new { wkid = 4326, latestWkid = 4326 }
                    },
                    hasAttachments = false,
                    htmlPopupType = "esriServerHTMLPopupTypeNone",
                    objectIdField = "OBJECTID",
                    globalIdField = "",
                    typeIdField = "",
                    fields = new object[]
                    {
                        new
                        {
                            name = "OBJECTID",
                            type = "esriFieldTypeOID",
                            alias = "OBJECTID",
                            nullable = false,
                            editable = false
                        },
                        new
                        {
                            name = "id",
                            type = "esriFieldTypeString",
                            alias = "ID",
                            length = 255,
                            nullable = true,
                            editable = false
                        }
                    },
                    supportedQueryFormats = "JSON,geoJSON,PBF",
                    maxRecordCount = 1000,
                    capabilities = "Query"
                };

                var format = context.Request.Query["f"].FirstOrDefault() ?? "json";
                return await WriteJsonResponse(context, layerMetadata, format);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error handling layer metadata request");
                return Results.Problem($"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Handles query requests - the main feature data interface
        /// </summary>
        private async Task<IResult> HandleQuery(HttpContext context, int layerId)
        {
            try
            {
                if (layerId != 0)
                {
                    return Results.NotFound(new { error = new { code = 400, message = "Invalid layer ID" } });
                }

                // Parse query parameters
                var queryParams = await ParseQueryParameters(context);
                
                _logger?.LogInformation($"DuckDB query request: WHERE={queryParams.Where}, Geometry={queryParams.HasGeometry}");

                // Convert ArcGIS query to DuckDB parameters
                var duckDbQuery = await BuildDuckDbQuery(queryParams);
                
                // Execute DuckDB query via DataProcessor
                var features = await ExecuteDuckDbQuery(duckDbQuery, queryParams);

                // Return ArcGIS-compatible response
                var response = new
                {
                    objectIdFieldName = "OBJECTID",
                    globalIdFieldName = "",
                    geometryType = queryParams.GeometryType ?? "esriGeometryPoint",
                    spatialReference = new { wkid = 4326, latestWkid = 4326 },
                    fields = GetFieldDefinitions(),
                    features = features,
                    exceededTransferLimit = features.Count >= (queryParams.ResultRecordCount ?? 1000)
                };

                return await WriteJsonResponse(context, response, queryParams.Format);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error handling query request: {Message}", ex.Message);
                return Results.Problem($"Query error: {ex.Message}");
            }
        }

        /// <summary>
        /// Parses ArcGIS REST API query parameters
        /// </summary>
        private async Task<ArcGisQueryParameters> ParseQueryParameters(HttpContext context)
        {
            var query = context.Request.Query;
            var form = context.Request.HasFormContentType ? await context.Request.ReadFormAsync() : null;
            
            // Helper to get parameter from query or form
            string GetParameter(string key) => query[key].FirstOrDefault() ?? form?[key].FirstOrDefault();

            var parameters = new ArcGisQueryParameters
            {
                Where = GetParameter("where") ?? "1=1",
                OutFields = GetParameter("outFields") ?? "*",
                ReturnGeometry = GetParameter("returnGeometry")?.ToLowerInvariant() != "false",
                Format = GetParameter("f") ?? "json",
                ResultRecordCount = int.TryParse(GetParameter("resultRecordCount"), out int count) ? count : 1000,
                ResultOffset = int.TryParse(GetParameter("resultOffset"), out int offset) ? offset : 0,
                OrderByFields = GetParameter("orderByFields"),
                GeometryType = GetParameter("geometryType")
            };

            // Parse spatial parameters
            var geometryParam = GetParameter("geometry");
            if (!string.IsNullOrEmpty(geometryParam))
            {
                parameters.QueryGeometry = ParseGeometryParameter(geometryParam);
                parameters.SpatialRel = GetParameter("spatialRel") ?? "esriSpatialRelIntersects";
                parameters.InSR = GetParameter("inSR");
            }

            return parameters;
        }

        /// <summary>
        /// Converts ArcGIS query parameters to DuckDB SQL
        /// </summary>
        private async Task<string> BuildDuckDbQuery(ArcGisQueryParameters parameters)
        {
            var whereClause = parameters.Where;
            
            // Add spatial filter if geometry is provided
            if (parameters.QueryGeometry != null)
            {
                var spatialClause = BuildSpatialFilter(parameters.QueryGeometry, parameters.SpatialRel, parameters.InSR);
                whereClause = $"({whereClause}) AND ({spatialClause})";
            }

            // Build field list
            var fields = parameters.OutFields == "*" ? "*" : parameters.OutFields;
            
            // Add ROW_NUMBER for OBJECTID if needed
            if (parameters.ReturnGeometry)
            {
                fields = $"ROW_NUMBER() OVER() as OBJECTID, {fields}, geometry";
            }
            else
            {
                fields = $"ROW_NUMBER() OVER() as OBJECTID, {fields}";
            }

            var query = $@"
                SELECT {fields}
                FROM current_table 
                WHERE {whereClause}
            ";

            // Add ordering
            if (!string.IsNullOrEmpty(parameters.OrderByFields))
            {
                query += $" ORDER BY {parameters.OrderByFields}";
            }

            // Add paging
            if (parameters.ResultOffset > 0)
            {
                query += $" OFFSET {parameters.ResultOffset}";
            }
            
            if (parameters.ResultRecordCount.HasValue)
            {
                query += $" LIMIT {parameters.ResultRecordCount.Value}";
            }

            return query;
        }

        /// <summary>
        /// Executes DuckDB query and converts results to ArcGIS feature format
        /// </summary>
        private async Task<List<object>> ExecuteDuckDbQuery(string query, ArcGisQueryParameters parameters)
        {
            // Use existing DataProcessor to execute query
            // This would need to be implemented to work with the current_table
            // For now, return sample data structure
            
            var features = new List<object>();
            
            // TODO: Execute actual DuckDB query via _dataProcessor
            // var results = await _dataProcessor.ExecuteQueryAsync(query);
            
            // Sample feature for demonstration
            features.Add(new
            {
                attributes = new
                {
                    OBJECTID = 1,
                    id = "sample_feature_1"
                },
                geometry = parameters.ReturnGeometry ? new
                {
                    x = -118.2437,
                    y = 34.0522
                } : null
            });

            return features;
        }

        /// <summary>
        /// Builds spatial filter SQL for DuckDB
        /// </summary>
        private string BuildSpatialFilter(object geometry, string spatialRel, string inSR)
        {
            // Convert ArcGIS spatial query to DuckDB spatial SQL
            // This is a simplified version - would need full implementation
            
            if (geometry is JsonElement geoElement)
            {
                if (geoElement.TryGetProperty("x", out var x) && geoElement.TryGetProperty("y", out var y))
                {
                    // Point geometry
                    return $"ST_DWithin(geometry, ST_Point({x.GetDouble()}, {y.GetDouble()}), 1000)";
                }
                else if (geoElement.TryGetProperty("xmin", out var xmin))
                {
                    // Envelope geometry
                    var xminVal = xmin.GetDouble();
                    var yminVal = geoElement.GetProperty("ymin").GetDouble();
                    var xmaxVal = geoElement.GetProperty("xmax").GetDouble();
                    var ymaxVal = geoElement.GetProperty("ymax").GetDouble();
                    
                    return $@"
                        bbox.xmin BETWEEN {xminVal.ToString("G", CultureInfo.InvariantCulture)} AND {xmaxVal.ToString("G", CultureInfo.InvariantCulture)} AND
                        bbox.ymin BETWEEN {yminVal.ToString("G", CultureInfo.InvariantCulture)} AND {ymaxVal.ToString("G", CultureInfo.InvariantCulture)}
                    ";
                }
            }

            return "1=1"; // Default to no spatial filter
        }

        /// <summary>
        /// Parses geometry parameter from request
        /// </summary>
        private object ParseGeometryParameter(string geometryParam)
        {
            try
            {
                return JsonSerializer.Deserialize<JsonElement>(geometryParam);
            }
            catch
            {
                // Try simple envelope format: xmin,ymin,xmax,ymax
                var parts = geometryParam.Split(',');
                if (parts.Length == 4 && 
                    double.TryParse(parts[0], out double xmin) &&
                    double.TryParse(parts[1], out double ymin) &&
                    double.TryParse(parts[2], out double xmax) &&
                    double.TryParse(parts[3], out double ymax))
                {
                    return new { xmin, ymin, xmax, ymax };
                }
                
                return null;
            }
        }

        /// <summary>
        /// Gets field definitions for the response
        /// </summary>
        private object[] GetFieldDefinitions()
        {
            return new object[]
            {
                new
                {
                    name = "OBJECTID",
                    type = "esriFieldTypeOID",
                    alias = "OBJECTID"
                },
                new
                {
                    name = "id",
                    type = "esriFieldTypeString",
                    alias = "ID",
                    length = 255
                }
            };
        }

        /// <summary>
        /// Writes JSON response in the requested format
        /// </summary>
        private async Task<IResult> WriteJsonResponse(HttpContext context, object data, string format)
        {
            string jsonResponse;
            string contentType;

            switch (format.ToLowerInvariant())
            {
                case "pjson":
                    // Pretty-printed JSON
                    var prettyOptions = new JsonSerializerOptions(_jsonOptions) { WriteIndented = true };
                    jsonResponse = JsonSerializer.Serialize(data, prettyOptions);
                    contentType = "application/json";
                    break;
                
                case "geojson":
                    // TODO: Convert to GeoJSON format
                    jsonResponse = JsonSerializer.Serialize(data, _jsonOptions);
                    contentType = "application/geo+json";
                    break;
                
                default: // json
                    jsonResponse = JsonSerializer.Serialize(data, _jsonOptions);
                    contentType = "application/json";
                    break;
            }

            context.Response.ContentType = contentType;
            context.Response.Headers.Add("Access-Control-Allow-Origin", "*");
            
            await context.Response.WriteAsync(jsonResponse);
            return Results.Empty;
        }

        public void Dispose()
        {
            _ = Task.Run(async () => await StopAsync());
            _cancellationTokenSource?.Dispose();
        }
    }

    /// <summary>
    /// Parsed ArcGIS REST API query parameters
    /// </summary>
    public class ArcGisQueryParameters
    {
        public string Where { get; set; } = "1=1";
        public string OutFields { get; set; } = "*";
        public bool ReturnGeometry { get; set; } = true;
        public string Format { get; set; } = "json";
        public int? ResultRecordCount { get; set; }
        public int ResultOffset { get; set; }
        public string OrderByFields { get; set; }
        public string GeometryType { get; set; }
        public object QueryGeometry { get; set; }
        public string SpatialRel { get; set; }
        public string InSR { get; set; }
        public bool HasGeometry => QueryGeometry != null;
    }

    /// <summary>
    /// Custom logger provider for integration with existing logging
    /// </summary>
    public class CustomLoggerProvider : ILoggerProvider
    {
        private readonly ILogger _logger;

        public CustomLoggerProvider(ILogger logger)
        {
            _logger = logger;
        }

        public ILogger CreateLogger(string categoryName) => _logger;
        public void Dispose() { }
    }
} 