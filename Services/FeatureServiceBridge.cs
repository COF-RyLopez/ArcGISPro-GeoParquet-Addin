using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Security.Cryptography;
using System.IO.Compression;
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
        private const int _maxRecordCount = 10000;
        private readonly object _spatialReference = new
        {
            wkid = 4326,
            latestWkid = 4326
        };

        // JSON serialization options
        private readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        // Multi-layer service: add key Overture themes (kept lean; attributes discovered on-demand)
        // Rendering order: configured to ensure polygons at bottom, lines in middle, points on top in ArcGIS Pro
        // Order chosen: Places (points) [Layer 0], Roads (lines) [Layer 1], Buildings (polygons) [Layer 2]
        private readonly List<ThemeDefinition> _themes = new List<ThemeDefinition>
        {
            new ThemeDefinition 
            { 
                Id = 0, // Places will be Layer 0 (topmost in Pro rendering observed)
                Name = "Places - Points",
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=places/type=place/*.parquet",
                GeometryType = "esriGeometryPoint",
                Fields = new[] { "id" }
            },
            new ThemeDefinition 
            { 
                Id = 1, // Roads will be Layer 1
                Name = "Transportation - Roads", 
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=transportation/type=segment/*.parquet",
                GeometryType = "esriGeometryPolyline",
                Fields = new[] { "id" }
            },
            new ThemeDefinition 
            { 
                Id = 2, // Buildings will be Layer 2 (bottom)
                Name = "Buildings - Footprints",
                S3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=buildings/type=building/*.parquet",
                GeometryType = "esriGeometryPolygon",
                Fields = new[] { "id" }
            }
        };

        // In-memory cache status - Thread-safe async synchronization
        private bool _dataLoaded = false;
        private readonly SemaphoreSlim _loadSemaphore = new SemaphoreSlim(1, 1);
        private readonly object _loadLock = new object();
        private readonly SemaphoreSlim _duckSemaphore = new SemaphoreSlim(1, 1);
        private double _cachedXmin, _cachedYmin, _cachedXmax, _cachedYmax;
        private const double _cacheBuffer = 0.10; // degrees
        // Not const to avoid CS0162 unreachable code warnings in logging branches
        private static readonly bool _verboseSqlLogging = false; // set true to log full SQL

        public FeatureServiceBridge(DataProcessor dataProcessor, int port = 8080)
        {
            _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
            _port = port;
            _cancellationTokenSource = new CancellationTokenSource();
            _discoveredFields = new Dictionary<int, HashSet<string>>();
            _discoveredFieldSql = new Dictionary<int, Dictionary<string, string>>();
            _structColumns = new Dictionary<int, HashSet<string>>();
        }

        // Cache of discovered optional attributes per layer id
        private readonly Dictionary<int, HashSet<string>> _discoveredFields;
        // For discovered fields that require SQL expressions (e.g., flattened STRUCT members)
        private readonly Dictionary<int, Dictionary<string, string>> _discoveredFieldSql;
        // Track which columns are STRUCT so we can synthesize struct_extract on demand
        private readonly Dictionary<int, HashSet<string>> _structColumns;
        // Cache of materialized export tables for heavy outFields=* paging
        private readonly Dictionary<string, string> _materializedExports = new Dictionary<string, string>();
        private readonly Dictionary<string, DateTime> _materializedTouched = new Dictionary<string, DateTime>();
        private readonly TimeSpan _materializedTtl = TimeSpan.FromMinutes(15);
        // Prevent concurrent CREATEs for the same materialization key
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, SemaphoreSlim> _materializeLocks = new System.Collections.Concurrent.ConcurrentDictionary<string, SemaphoreSlim>();
        // Limit background pre-materializations to reduce UI stutter
        private static readonly SemaphoreSlim _backgroundMaterializeGate = new SemaphoreSlim(1, 1);
        // Debounce state for prewarming: last-seen timestamp and whether a prewarm task is scheduled
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _prewarmLastSeenTicks = new System.Collections.Concurrent.ConcurrentDictionary<string, long>();
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _prewarmScheduled = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>();

        // AOI (Area-of-Interest) selection: when set, all queries route to AOI-materialized tables
        private string _aoiWkt = null;
        private readonly Dictionary<int, string> _aoiTables = new Dictionary<int, string>();
        private readonly SemaphoreSlim _aoiLock = new SemaphoreSlim(1, 1);
        private const string _divisionsS3Path = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=divisions/type=division/*.parquet";

        /// <summary>
        /// Ensure a materialized export table exists for the given theme and geometry key.
        /// Returns the table name if available, otherwise null.
        /// </summary>
        private async Task<string> EnsureMaterializedForKeyAsync(
            ThemeDefinition theme,
            string key,
            string whereClause,
            string geometryParam,
            string spatialRel,
            int? outWkid,
            int? geometryPrecision,
            string quantizationParameters)
        {
            if (_materializedExports.TryGetValue(key, out var existing))
            {
                _materializedTouched[key] = DateTime.UtcNow;
                Debug.WriteLine($"📖 Using pre-existing materialized table {existing} for key {key}");
                return existing;
            }

            var semaphore = _materializeLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
            await semaphore.WaitAsync();
            try
            {
                if (_materializedExports.TryGetValue(key, out existing))
                {
                    _materializedTouched[key] = DateTime.UtcNow;
                    return existing;
                }

                var matTable = $"export_{theme.Id}_{Math.Abs(key.GetHashCode())}";
                var fullQuery = BuildDuckDbQuery(theme, whereClause, geometryParam, spatialRel, "*", int.MaxValue, true /*returnGeometry*/, outWkid, 0, geometryPrecision, quantizationParameters)
                                 .Replace($" LIMIT {int.MaxValue}", "");
                var createSql = $"CREATE TEMPORARY TABLE IF NOT EXISTS {matTable} AS {fullQuery}";
                try
                {
                    await _dataProcessor.ExecuteQueryAsync(createSql);
                    _materializedExports[key] = matTable;
                    _materializedTouched[key] = DateTime.UtcNow;
                    Debug.WriteLine($"📦 Materialized export table {matTable} for key {key}");
                    return matTable;
                }
                catch (Exception ex)
                {
                    if (ex.Message?.IndexOf("already exists", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        if (_materializedExports.TryGetValue(key, out existing))
                        {
                            _materializedTouched[key] = DateTime.UtcNow;
                            return existing;
                        }
                        // Assume a racing create succeeded
                        _materializedExports[key] = matTable;
                        _materializedTouched[key] = DateTime.UtcNow;
                        return matTable;
                    }

                    Debug.WriteLine($"⚠️ EnsureMaterializedForKeyAsync failed: {ex.Message}");
                    return null;
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        // Helper: split a STRUCT member list by top-level commas (ignores commas inside nested parentheses)
        private static IEnumerable<string> SplitTopLevelComma(string s)
        {
            var parts = new List<string>();
            if (string.IsNullOrEmpty(s)) return parts;
            int depth = 0; int start = 0;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c == '(') depth++;
                else if (c == ')') depth = Math.Max(0, depth - 1);
                else if (c == ',' && depth == 0)
                {
                    parts.Add(s.Substring(start, i - start).Trim());
                    start = i + 1;
                }
            }
            if (start < s.Length) parts.Add(s.Substring(start).Trim());
            return parts.Where(p => !string.IsNullOrEmpty(p));
        }

        private async Task EnsureThemeFieldsDiscoveredAsync(ThemeDefinition theme)
        {
            if (_discoveredFields.ContainsKey(theme.Id)) return;
            try
            {
                // Always inspect schema directly from source to avoid limitations of in-memory cache (which only carries id/bbox/geometry)
                var describeQuery = $"DESCRIBE SELECT * FROM read_parquet('{theme.S3Path}', filename=true, hive_partitioning=1) LIMIT 0";
                var rows = await _dataProcessor.ExecuteQueryAsync(describeQuery);
                var discovered = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var discoveredSql = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var structCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var allowedTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                { "VARCHAR", "BOOL", "BOOLEAN", "UTINYINT", "TINYINT", "USMALLINT", "SMALLINT", "UINTEGER", "INTEGER", "UBIGINT", "BIGINT", "HUGEINT", "REAL", "DOUBLE", "DECIMAL" };

                foreach (var row in rows)
                {
                    var colName = row.TryGetValue("column_name", out var n) ? n?.ToString() : null;
                    var colType = row.TryGetValue("column_type", out var t) ? t?.ToString() : null;
                    if (string.IsNullOrWhiteSpace(colName) || string.IsNullOrWhiteSpace(colType)) continue;
                    if (string.Equals(colName, "id", StringComparison.OrdinalIgnoreCase)) continue;
                    if (colName.Equals("bbox", StringComparison.OrdinalIgnoreCase)) continue;
                    if (colName.Equals("geometry", StringComparison.OrdinalIgnoreCase)) continue;
                    var upper = colType.ToUpperInvariant();

                    if (allowedTypes.Contains(upper))
                    {
                        // simple scalar
                        discovered.Add(colName);
                    }
                    else if (upper.StartsWith("LIST(STRUCT(") || Regex.IsMatch(colType, @"^\s*STRUCT\(.*\)\s*\[\]\s*$", RegexOptions.IgnoreCase | RegexOptions.Singleline))
                    {
                        // LIST of STRUCT (DuckDB may format as LIST(STRUCT(...)) or STRUCT(...)[])
                        // Expose first element's scalar members: list_extract(col, 1) -> struct_extract(...)
                        var start = colType.IndexOf("STRUCT(", StringComparison.OrdinalIgnoreCase);
                        var inner = colType.Substring(start + "STRUCT(".Length);
                        // Remove any trailing list suffixes like ")[]" or ")[]?"
                        inner = Regex.Replace(inner, @"\)\s*\[\]\s*\??\s*$", ")", RegexOptions.IgnoreCase);
                        // Trim to the last closing ')' to capture only member list
                        var closeIdx = inner.LastIndexOf(')');
                        if (closeIdx >= 0)
                            inner = inner.Substring(0, closeIdx);
                        var parts = SplitTopLevelComma(inner);
                        foreach (var p in parts)
                        {
                            var spaceIdx = p.IndexOf(' ');
                            if (spaceIdx <= 0) continue;
                            var member = p.Substring(0, spaceIdx).Trim().Trim('"');
                            var mType = p.Substring(spaceIdx + 1).Trim();
                            var upperMemberType = mType.ToUpperInvariant();
                            if (string.Equals(member, "property", StringComparison.OrdinalIgnoreCase)) continue; // avoid non-scalar/unstable member
                            if (upperMemberType.StartsWith("STRUCT(") || upperMemberType.StartsWith("LIST(")) continue;
                            if (!allowedTypes.Contains(mType.ToUpperInvariant())) continue;
                            var fieldName = $"{colName}_{member}";
                            var expr = $"struct_extract(list_extract({colName}, 1), '{member}') as {fieldName}";
                            if (discovered.Add(fieldName))
                            {
                                discoveredSql[fieldName] = expr;
                            }
                        }
                    }
                    else if (upper.StartsWith("STRUCT("))
                    {
                        structCols.Add(colName);
                        // Flatten first-level scalar members: STRUCT(a TYPE, b TYPE, ...)
                        var inner = colType.Substring("STRUCT(".Length);
                        // Trim to the last closing ')' and ignore any trailing characters
                        var closeIdx = inner.LastIndexOf(')');
                        if (closeIdx >= 0)
                            inner = inner.Substring(0, closeIdx);
                        var parts = SplitTopLevelComma(inner);
                        foreach (var p in parts)
                        {
                            // p format: name TYPE
                            var spaceIdx = p.IndexOf(' ');
                            if (spaceIdx <= 0) continue;
                            var member = p.Substring(0, spaceIdx).Trim().Trim('"');
                            var mType = p.Substring(spaceIdx + 1).Trim();
                            if (string.Equals(member, "property", StringComparison.OrdinalIgnoreCase)) continue; // avoid non-scalar/unstable member
                            if (!allowedTypes.Contains(mType.ToUpperInvariant())) continue; // only scalar
                            var fieldName = $"{colName}_{member}"; // e.g., speed_limits_maxspeed
                            // SQL expression using struct_extract
                            var expr = $"struct_extract({colName}, '{member}') as {fieldName}";
                            if (discovered.Add(fieldName))
                            {
                                discoveredSql[fieldName] = expr;
                            }
                        }
                    }
                    // skip LIST/MAP and other complex types for now
                    if (discovered.Count >= 40) break; // broader cap when flattening
                }
                if (discovered.Count > 0)
                {
                    _discoveredFields[theme.Id] = discovered;
                    // Merge into theme.Fields, preserving order and avoiding duplicates
                    foreach (var f in discovered)
                    {
                        if (!theme.Fields.Contains(f))
                        {
                            theme.Fields = theme.Fields.Append(f).ToArray();
                        }
                    }
                    if (discoveredSql.Count > 0)
                    {
                        _discoveredFieldSql[theme.Id] = discoveredSql;
                    }
                    if (structCols.Count > 0)
                    {
                        _structColumns[theme.Id] = structCols;
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Field discovery skipped for theme {theme.Name}: {ex.Message}");
            }
        }

        private static int ComputeObjectId(string seed)
        {
            if (string.IsNullOrEmpty(seed)) return 0;
            // Stable 32-bit hash
            using var sha1 = SHA1.Create();
            var bytes = Encoding.UTF8.GetBytes(seed);
            var hash = sha1.ComputeHash(bytes);
            // fold first 4 bytes into positive int
            int val = (hash[0] | (hash[1] << 8) | (hash[2] << 16) | (hash[3] << 24));
            if (val < 0) val = ~val;
            if (val == 0) val = 1;
            return val;
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
                var buffer = _cacheBuffer; // ~1km buffer
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
                            SELECT id, bbox, geometry
                            FROM read_parquet('{theme.S3Path}', filename=true, hive_partitioning=1)
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
                    // Store cached extent
                    _cachedXmin = boundsXmin;
                    _cachedYmin = boundsYmin;
                    _cachedXmax = boundsXmax;
                    _cachedYmax = boundsYmax;
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

        private async Task LoadViewportCacheAsync(double xmin, double ymin, double xmax, double ymax)
        {
            await _loadSemaphore.WaitAsync();
            try
            {
                var boundsXmin = xmin - _cacheBuffer;
                var boundsYmin = ymin - _cacheBuffer;
                var boundsXmax = xmax + _cacheBuffer;
                var boundsYmax = ymax + _cacheBuffer;

                int successfulTables = 0;
                foreach (var theme in _themes)
                {
                    var tableName = GetTableName(theme);
                    try
                    {
                        var createTableQuery = $@"
                            CREATE OR REPLACE TABLE {tableName} AS 
                            SELECT id, bbox, geometry
                            FROM read_parquet('{theme.S3Path}', filename=true, hive_partitioning=1)
                            WHERE bbox.xmin IS NOT NULL AND bbox.ymin IS NOT NULL 
                              AND bbox.xmax IS NOT NULL AND bbox.ymax IS NOT NULL
                              AND bbox.xmin <= {boundsXmax.ToString("G", CultureInfo.InvariantCulture)} 
                              AND bbox.xmax >= {boundsXmin.ToString("G", CultureInfo.InvariantCulture)}
                              AND bbox.ymin <= {boundsYmax.ToString("G", CultureInfo.InvariantCulture)} 
                              AND bbox.ymax >= {boundsYmin.ToString("G", CultureInfo.InvariantCulture)}";

                        await _dataProcessor.ExecuteQueryAsync(createTableQuery);
                        successfulTables++;
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"⚠️ Cache load failed for {theme.Name}: {ex.Message}");
                    }
                }

                if (successfulTables > 0)
                {
                    _dataLoaded = true;
                    _cachedXmin = boundsXmin;
                    _cachedYmin = boundsYmin;
                    _cachedXmax = boundsXmax;
                    _cachedYmax = boundsYmax;
                    Debug.WriteLine($"📦 Cache reloaded for requested extent: ({_cachedXmin:F4},{_cachedYmin:F4})–({_cachedXmax:F4},{_cachedYmax:F4})");
                }
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
                if (path.StartsWith("/search"))
                {
                    await HandleSearch(context);
                    return;
                }
                else if (path.StartsWith("/aoi/set"))
                {
                    await HandleAoiSet(context);
                    return;
                }
                else if (path.StartsWith("/aoi/clear"))
                {
                    await HandleAoiClear(context);
                    return;
                }
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
                    supportedQueryFormats = "JSON,geoJSON",
                    capabilities = "Query,Extract",
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

        // Simple typeahead search over Overture Divisions
        private async Task HandleSearch(HttpListenerContext context)
        {
            try
            {
                var qs = ParseQueryParameters(context.Request);
                var q = (GetQueryParam(qs, "q") ?? string.Empty).Trim();
                int limit = int.TryParse(GetQueryParam(qs, "limit"), out var l) ? Math.Clamp(l, 1, 25) : 10;
                if (q.Length < 2)
                {
                    await WriteJsonResponse(context, JsonSerializer.Serialize(new { results = Array.Empty<object>() }, _jsonOptions));
                    return;
                }
                var like = q.Replace("'", "''");
                // Overture divisions store names as a STRUCT column 'names' with a 'primary' member
                // Not all releases expose 'admin_level'; to avoid binder errors, surface 'type' as adminLevel
                var sql = $@"SELECT struct_extract(names, 'primary') AS name,
                                     type AS adminLevel,
                                     ST_AsText(geometry) as wkt
                              FROM read_parquet('{_divisionsS3Path}', filename=true, hive_partitioning=1)
                              WHERE LOWER(struct_extract(names, 'primary')) LIKE LOWER('%{like}%')
                              ORDER BY LENGTH(struct_extract(names, 'primary')) ASC
                              LIMIT {limit}";
                var rows = await _dataProcessor.ExecuteQueryAsync(sql);
                var results = rows.Select(r => new
                {
                    name = r.ContainsKey("name") ? r["name"]?.ToString() : null,
                    adminLevel = r.ContainsKey("admin_level") ? r["admin_level"]?.ToString() : null,
                    wkt = r.ContainsKey("wkt") ? r["wkt"]?.ToString() : null
                }).ToArray();
                await WriteJsonResponse(context, JsonSerializer.Serialize(new { results }, _jsonOptions));
            }
            catch (Exception ex)
            {
                context.Response.StatusCode = 500;
                await WriteJsonResponse(context, JsonSerializer.Serialize(new { error = ex.Message }, _jsonOptions));
            }
        }

        // Set AOI from provided WKT; materialize per-theme tables for fast draws/exports
        private async Task HandleAoiSet(HttpListenerContext context)
        {
            try
            {
                var qs = ParseQueryParameters(context.Request);
                var wkt = GetQueryParam(qs, "wkt");
                if (string.IsNullOrWhiteSpace(wkt))
                {
                    context.Response.StatusCode = 400;
                    await WriteJsonResponse(context, JsonSerializer.Serialize(new { error = "Missing wkt" }, _jsonOptions));
                    return;
                }
                await _aoiLock.WaitAsync();
                try
                {
                    _aoiWkt = wkt;
                    _aoiTables.Clear();
                    foreach (var theme in _themes)
                    {
                        var key = $"aoi:{theme.Id}";
                        var table = $"aoi_{theme.Id}_{Math.Abs(key.GetHashCode())}";
                        var geomExpr = "geometry";
                        var create = $"CREATE TEMPORARY TABLE IF NOT EXISTS {table} AS SELECT * FROM read_parquet('{theme.S3Path}', filename=true, hive_partitioning=1) WHERE ST_Intersects({geomExpr}, ST_GeomFromText('{_aoiWkt}', 4326))";
                        await _dataProcessor.ExecuteQueryAsync(create);
                        _aoiTables[theme.Id] = table;
                    }
                }
                finally
                {
                    _aoiLock.Release();
                }
                await WriteJsonResponse(context, JsonSerializer.Serialize(new { ok = true }, _jsonOptions));
            }
            catch (Exception ex)
            {
                context.Response.StatusCode = 500;
                await WriteJsonResponse(context, JsonSerializer.Serialize(new { error = ex.Message }, _jsonOptions));
            }
        }

        private async Task HandleAoiClear(HttpListenerContext context)
        {
            await _aoiLock.WaitAsync();
            try
            {
                _aoiWkt = null;
                _aoiTables.Clear();
            }
            finally
            {
                _aoiLock.Release();
            }
            await WriteJsonResponse(context, JsonSerializer.Serialize(new { ok = true }, _jsonOptions));
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

                // Discover and expose additional scalar attributes for this theme (once per run)
                await EnsureThemeFieldsDiscoveredAsync(theme);

                var layerMetadata = new
                {
                    currentVersion = 11.1,
                    id = theme.Id,
                    name = theme.Name,
                    type = "Feature Layer",
                    description = $"Live cloud-native {theme.Name} data from Overture Maps via DuckDB",
                    objectIdField = "OBJECTID",
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
                    drawingInfo = new
                    {
                        renderer = new
                        {
                            type = "simple",
                            symbol = theme.GeometryType switch
                            {
                                "esriGeometryPoint" => (object)new { type = "esriSMS", style = "esriSMSCircle", color = new[] { 0, 122, 194, 255 }, size = 6, outline = new { color = new[] { 255, 255, 255, 255 }, width = 1 } },
                                "esriGeometryPolyline" => (object)new { type = "esriSLS", style = "esriSLSSolid", color = new[] { 0, 122, 194, 255 }, width = 1.5 },
                                "esriGeometryPolygon" => (object)new { type = "esriSFS", style = "esriSFSSolid", color = new[] { 0, 122, 194, 64 }, outline = new { color = new[] { 0, 122, 194, 255 }, width = 1 } },
                                _ => (object)new { type = "esriSLS", style = "esriSLSSolid", color = new[] { 0, 122, 194, 255 }, width = 1 }
                            }
                        }
                    },
                    canModifyLayer = false,
                    canScaleSymbols = false,
                    hasLabels = false,
                    capabilities = "Query,Extract",
                    maxRecordCount = _maxRecordCount,
                    supportsStatistics = false,
                    supportsAdvancedQueries = false,
                    supportedQueryFormats = "JSON,geoJSON",
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
                        supportsPagination = true,
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

                // Prefer in-memory viewport cache when available for performance; otherwise fallback to S3
                Debug.WriteLine(_dataLoaded
                    ? $"⚡ Using in-memory cache for {theme.Name}"
                    : $"🌐 Using direct S3 pass-through for {theme.Name}");

                var queryParams = ParseQueryParameters(context.Request);
                
                // Extract query parameters
                var whereClause = GetQueryParam(queryParams, "where") ?? "1=1";
                var outFields = GetQueryParam(queryParams, "outFields") ?? "*";
                var geometryParam = GetQueryParam(queryParams, "geometry");
                var spatialRel = GetQueryParam(queryParams, "spatialRel") ?? "esriSpatialRelIntersects";
                var returnGeometry = GetQueryParam(queryParams, "returnGeometry")?.ToLowerInvariant() != "false";
                var maxRecords = int.TryParse(GetQueryParam(queryParams, "resultRecordCount"), out var max) ? max : _maxRecordCount;
                var resultOffset = int.TryParse(GetQueryParam(queryParams, "resultOffset"), out var off) ? off : 0;
                var outWkid = int.TryParse(GetQueryParam(queryParams, "outSR"), out var wk) ? wk : (int?)null;
                var format = GetQueryParam(queryParams, "f") ?? "json";
                var forceMaterialize = (GetQueryParam(queryParams, "forceMaterialize") ?? "false").Equals("true", StringComparison.OrdinalIgnoreCase);
                // Geometry precision and quantization (ArcGIS REST) – used to reduce payload/vertices
                int? geometryPrecision = null; if (int.TryParse(GetQueryParam(queryParams, "geometryPrecision"), out var gp)) geometryPrecision = gp;
                var quantizationParameters = GetQueryParam(queryParams, "quantizationParameters");

                // Heuristic: if client didn't explicitly ask for geometry and provided no geometry filter,
                // avoid returning geometry for outFields='*' preflight calls. For export materialization we
                // will override this and include geometry in the temp table regardless.
                bool explicitReturnGeometry = queryParams.ContainsKey("returnGeometry");
                if (!explicitReturnGeometry && string.IsNullOrEmpty(geometryParam) && outFields == "*")
                {
                    returnGeometry = false;
                }

                Debug.WriteLine($"Layer {layerId} ({theme.Name}) Query: where={whereClause}, outFields={outFields}, geometry={geometryParam}, spatialRel={spatialRel}");

                // Proactively refresh cache if request extent is outside cached extent
                if (_dataLoaded && !string.IsNullOrEmpty(geometryParam))
                {
                    if (TryParseEnvelope(geometryParam, out var gxmin, out var gymin, out var gxmax, out var gymax))
                    {
                        // Hysteresis: extend cached bounds by 2x buffer before triggering reload
                        double margin = _cacheBuffer * 2.0;
                        bool outside = gxmin < (_cachedXmin - margin) || gymin < (_cachedYmin - margin) || gxmax > (_cachedXmax + margin) || gymax > (_cachedYmax + margin);
                        if (outside)
                        {
                            Debug.WriteLine("🔁 Requested extent outside cached viewport. Reloading cache for requested bbox...");
                            await LoadViewportCacheAsync(gxmin, gymin, gxmax, gymax);
                        }
                    }
                }

                // Detect ID-based paging/export
                bool whereHasIds = !string.IsNullOrEmpty(whereClause) &&
                    (whereClause.IndexOf("OBJECTID IN", StringComparison.OrdinalIgnoreCase) >= 0 ||
                     whereClause.IndexOf(" id IN", StringComparison.OrdinalIgnoreCase) >= 0);

                // Build DuckDB query (with optional materialization for export workloads)
                string duckDbQuery;
                // Treat as export ONLY when:
                // - outFields = * (classic export)
                // - ID list is provided (Pro paging by OBJECTID)
                // - caller forces with forceMaterialize=true
                // Do NOT treat normal map draws (OBJECTID-only + geometry) as export; that slows panning.
                bool isExportWorkload =
                    forceMaterialize ||
                    (outFields == "*") ||
                    whereHasIds;
                if (isExportWorkload)
                {
                    Debug.WriteLine($"🚚 Export/materialization workload detected (force={forceMaterialize}, outFields={outFields}, whereHasIds={whereHasIds}, returnGeometry={returnGeometry}, hasGeometry={(string.IsNullOrEmpty(geometryParam)?"false":"true")})");
                }

                // Background pre-warm disabled for normal draws to avoid stepwise rendering.
                // Materialization occurs only for explicit export/attribute-table workflows.

                // If a materialized table already exists for a plausible export key (by bbox or id hash),
                // short-circuit all routing and read from it even if the client passes resultRecordCount.
                {
                    string existingKey = null;
                    // Prefer explicit geometry key when present
                    if (!string.IsNullOrEmpty(geometryParam) && TryParseEnvelope(geometryParam, out var kxmin, out var kymin, out var kxmax, out var kymax))
                    {
                        existingKey = $"{theme.Id}:{Math.Round(kxmin,3)}:{Math.Round(kymin,3)}:{Math.Round(kxmax,3)}:{Math.Round(kymax,3)}";
                    }
                    else
                    {
                        // Use cached viewport as the implicit export key when no geometry was supplied
                        if (_dataLoaded && _cachedXmax > _cachedXmin && _cachedYmax > _cachedYmin)
                        {
                            existingKey = $"{theme.Id}:{Math.Round(_cachedXmin,3)}:{Math.Round(_cachedYmin,3)}:{Math.Round(_cachedXmax,3)}:{Math.Round(_cachedYmax,3)}";
                        }
                        // Or an id-filter hash if present
                        var whereHasIdsNow = !string.IsNullOrEmpty(whereClause) &&
                            (whereClause.IndexOf("OBJECTID IN", StringComparison.OrdinalIgnoreCase) >= 0 ||
                             whereClause.IndexOf(" id IN", StringComparison.OrdinalIgnoreCase) >= 0);
                        if (whereHasIdsNow)
                        {
                            existingKey = existingKey ?? $"{theme.Id}:ids:{whereClause.GetHashCode()}";
                        }
                    }

                    if (!string.IsNullOrEmpty(existingKey) && _materializedExports.TryGetValue(existingKey, out var matExisting))
                    {
                        _materializedTouched[existingKey] = DateTime.UtcNow; // touch
                        System.Diagnostics.Debug.WriteLine($"📖 Using existing materialized table {matExisting} for key {existingKey}");
                        List<string> selectColumns;
                        if (outFields == "*")
                        {
                            selectColumns = GetFieldDefinitions(theme)
                                .Select(f => (string)f.GetType().GetProperty("name")?.GetValue(f))
                                .Where(n => !string.Equals(n, "OBJECTID", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                        }
                        else
                        {
                            var requested = outFields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                                                     .Where(n => !string.Equals(n, "OBJECTID", StringComparison.OrdinalIgnoreCase))
                                                     .ToList();
                            if (!requested.Any(r => r.Equals("id", StringComparison.OrdinalIgnoreCase))) requested.Insert(0, "id");
                            selectColumns = requested;
                        }
                        if (explicitReturnGeometry || returnGeometry)
                        {
                            if (!selectColumns.Contains("geometry_geojson", StringComparer.OrdinalIgnoreCase))
                                selectColumns.Add("geometry_geojson");
                        }
                        var matSelect = string.Join(", ", selectColumns);
                        duckDbQuery = $"SELECT {matSelect} FROM {matExisting} ORDER BY bbox_ymin, bbox_xmin, id LIMIT {maxRecords} OFFSET {resultOffset}";
                        goto ExecuteQuery; // use the materialized table now
                    }
                }

                // Fast-path: if we already materialized an export table for this geometry, always read from it
                // regardless of the current outFields (Pro will follow up with OBJECTID/field-paged requests).
                if (isExportWorkload && !string.IsNullOrEmpty(geometryParam) && TryParseEnvelope(geometryParam, out var mxmin0, out var mymin0, out var mxmax0, out var mymax0))
                {
                    var existingKey = $"{theme.Id}:{Math.Round(mxmin0,3)}:{Math.Round(mymin0,3)}:{Math.Round(mxmax0,3)}:{Math.Round(mymax0,3)}";
                    if (_materializedExports.TryGetValue(existingKey, out var existingMat))
                    {
                        // Decide which columns to project from the materialized table based on the request
                        List<string> selectColumns;
                        if (outFields == "*")
                        {
                            selectColumns = GetFieldDefinitions(theme)
                                .Select(f => (string)f.GetType().GetProperty("name")?.GetValue(f))
                                .Where(n => !string.Equals(n, "OBJECTID", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                        }
                        else
                        {
                            var requested = outFields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                                                     .Where(n => !string.Equals(n, "OBJECTID", StringComparison.OrdinalIgnoreCase))
                                                     .ToList();
                            // Ensure id is present at minimum
                            if (!requested.Any(r => r.Equals("id", StringComparison.OrdinalIgnoreCase))) requested.Insert(0, "id");
                            selectColumns = requested;
                        }

                        // Include geometry from materialized table when requested
                        if (returnGeometry)
                        {
                            // The materialized table stores geometry as 'geometry_geojson'
                            if (!selectColumns.Contains("geometry_geojson", StringComparer.OrdinalIgnoreCase))
                                selectColumns.Add("geometry_geojson");
                        }

                        var matSelect = string.Join(", ", selectColumns);
                        duckDbQuery = $"SELECT {matSelect} FROM {existingMat} ORDER BY bbox_ymin, bbox_xmin, id LIMIT {maxRecords} OFFSET {resultOffset}";
                        _materializedTouched[existingKey] = DateTime.UtcNow; // touch
                        System.Diagnostics.Debug.WriteLine($"📖 Using existing materialized table {existingMat} for key {existingKey}");
                        goto ExecuteQuery; // skip the rest of routing logic
                    }
                }
                if (isExportWorkload && !string.IsNullOrEmpty(geometryParam))
                {
                    // Materialize the current extent + outFields=* into a temp table once; then page from that table.
                    // Key on theme + snapped bbox to ~100m to avoid thrashing
                    if (TryParseEnvelope(geometryParam, out var exmin, out var eymin, out var exmax, out var eymax))
                    {
                        string key = $"{theme.Id}:{Math.Round(exmin,3)}:{Math.Round(eymin,3)}:{Math.Round(exmax,3)}:{Math.Round(eymax,3)}";
                        if (!_materializedExports.TryGetValue(key, out var matTable))
                        {
                            matTable = await EnsureMaterializedForKeyAsync(theme, key, whereClause, geometryParam, spatialRel, outWkid, geometryPrecision, quantizationParameters);
                        }
                        if (!string.IsNullOrEmpty(matTable))
                        {
                            // Build a simple page from the materialized table
                            var selectColsList = GetFieldDefinitions(theme)
                                .Select(f => (string)f.GetType().GetProperty("name")?.GetValue(f))
                                .Where(n => !string.Equals(n, "OBJECTID", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                            if (returnGeometry && !selectColsList.Contains("geometry_geojson", StringComparer.OrdinalIgnoreCase))
                                selectColsList.Add("geometry_geojson");
                            var selectCols = string.Join(", ", selectColsList);
                            duckDbQuery = $"SELECT {selectCols} FROM {matTable} ORDER BY bbox_ymin, bbox_xmin, id LIMIT {maxRecords} OFFSET {resultOffset}";
                            _materializedTouched[key] = DateTime.UtcNow; // touch
                            System.Diagnostics.Debug.WriteLine($"📖 Paging from materialized table {matTable} for key {key}");
                        }
                        else
                        {
                            duckDbQuery = BuildDuckDbQuery(theme, whereClause, geometryParam, spatialRel, outFields, maxRecords, returnGeometry, outWkid, resultOffset, geometryPrecision, quantizationParameters);
                        }
                    }
                    else
                    {
                        duckDbQuery = BuildDuckDbQuery(theme, whereClause, geometryParam, spatialRel, outFields, maxRecords, returnGeometry, outWkid, resultOffset, geometryPrecision, quantizationParameters);
                    }
                }
                else if (isExportWorkload && string.IsNullOrEmpty(geometryParam))
                {
                    // Export with no geometry: scope to cached viewport if available or to WHERE id IN (...) if present
                    string effectiveGeom = geometryParam;
                    if (string.IsNullOrEmpty(effectiveGeom) && _dataLoaded && _cachedXmax > _cachedXmin && _cachedYmax > _cachedYmin)
                    {
                        effectiveGeom = BuildEnvelopeJson(_cachedXmin, _cachedYmin, _cachedXmax, _cachedYmax);
                    }

                    whereHasIds = !string.IsNullOrEmpty(whereClause) &&
                        (whereClause.IndexOf("OBJECTID IN", StringComparison.OrdinalIgnoreCase) >= 0 ||
                         whereClause.IndexOf(" id IN", StringComparison.OrdinalIgnoreCase) >= 0);

                    if (!string.IsNullOrEmpty(effectiveGeom) || whereHasIds)
                    {
                        string key;
                        if (!string.IsNullOrEmpty(effectiveGeom) && TryParseEnvelope(effectiveGeom, out var exmin2, out var eymin2, out var exmax2, out var eymax2))
                            key = $"{theme.Id}:{Math.Round(exmin2,3)}:{Math.Round(eymin2,3)}:{Math.Round(exmax2,3)}:{Math.Round(eymax2,3)}";
                        else
                            key = $"{theme.Id}:ids:{whereClause.GetHashCode()}";

                        if (!_materializedExports.TryGetValue(key, out var matTable))
                        {
                            matTable = await EnsureMaterializedForKeyAsync(theme, key, whereClause, effectiveGeom, spatialRel, outWkid, geometryPrecision, quantizationParameters);
                        }
                        if (!string.IsNullOrEmpty(matTable))
                        {
                            var selectColsList = GetFieldDefinitions(theme)
                                .Select(f => (string)f.GetType().GetProperty("name")?.GetValue(f))
                                .Where(n => !string.Equals(n, "OBJECTID", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                            if (returnGeometry && !selectColsList.Contains("geometry_geojson", StringComparer.OrdinalIgnoreCase))
                                selectColsList.Add("geometry_geojson");
                            var selectCols = string.Join(", ", selectColsList);
                            duckDbQuery = $"SELECT {selectCols} FROM {matTable} ORDER BY bbox_ymin, bbox_xmin, id LIMIT {maxRecords} OFFSET {resultOffset}";
                            _materializedTouched[key] = DateTime.UtcNow; // touch
                            System.Diagnostics.Debug.WriteLine($"📖 Paging from materialized table {matTable} for key {key}");
                        }
                        else
                        {
                            duckDbQuery = BuildDuckDbQuery(theme, whereClause, effectiveGeom, spatialRel, outFields, maxRecords, returnGeometry, outWkid, resultOffset, geometryPrecision, quantizationParameters);
                        }
                    }
                    else
                    {
                        duckDbQuery = BuildDuckDbQuery(theme, whereClause, geometryParam, spatialRel, outFields, maxRecords, returnGeometry, outWkid, resultOffset, geometryPrecision, quantizationParameters);
                    }
                }
                else
                {
                    duckDbQuery = BuildDuckDbQuery(theme, whereClause, geometryParam, spatialRel, outFields, maxRecords, returnGeometry, outWkid, resultOffset, geometryPrecision, quantizationParameters);
                }

ExecuteQuery:
                try { EvictExpiredMaterializations(); } catch { }
                
                // Execute query
                // Fast-paths: return count/IDs only per ArcGIS REST
                if ((GetQueryParam(queryParams, "returnCountOnly") ?? "false").Equals("true", StringComparison.OrdinalIgnoreCase))
                {
                    var countQuery = BuildDuckDbCountQuery(theme, whereClause, geometryParam, spatialRel, geometryPrecision, quantizationParameters);
                    var cntRows = await _dataProcessor.ExecuteQueryAsync(countQuery);
                    var total = (cntRows.FirstOrDefault()? ["cnt"]) ?? 0;
                    var countJson = JsonSerializer.Serialize(new { count = total }, _jsonOptions);
                    await WriteJsonResponse(context, countJson);
                    return;
                }

                if ((GetQueryParam(queryParams, "returnIdsOnly") ?? "false").Equals("true", StringComparison.OrdinalIgnoreCase))
                {
                    var idsQuery = duckDbQuery.Replace("SELECT id,", "SELECT id ")
                                              .Replace(", ST_AsGeoJSON(geometry) as geometry_geojson", "")
                                              .Replace(" LIMIT ", " LIMIT ");
                System.Diagnostics.Debug.WriteLine($"ID-only query SQL: {idsQuery}");
                var idRows = await _dataProcessor.ExecuteQueryAsync(idsQuery);
                    var objectIds = idRows.Select((r, i) => ComputeObjectId(r.ContainsKey("id") ? r["id"]?.ToString() : i.ToString())).ToArray();
                    var idsJson = JsonSerializer.Serialize(new { objectIdFieldName = "OBJECTID", objectIds = objectIds }, _jsonOptions);
                    await WriteJsonResponse(context, idsJson);
                    return;
                }

                var features = await ExecuteDuckDbQuery(duckDbQuery);

                // If cache is loaded but query returned nothing for a new extent, reload cache for current view and retry once
                if (_dataLoaded && (features == null || features.Count == 0) && !string.IsNullOrEmpty(geometryParam))
                {
                    try
                    {
                        Debug.WriteLine("🔁 No features from cache for requested extent. Reloading viewport cache and retrying once...");
                        _dataLoaded = false;
                        await EnsureDataLoadedAsync();
                        duckDbQuery = BuildDuckDbQuery(theme, whereClause, geometryParam, spatialRel, outFields, maxRecords, returnGeometry, outWkid, resultOffset, geometryPrecision, quantizationParameters);
                        features = await ExecuteDuckDbQuery(duckDbQuery);
                    }
                    catch (Exception reloadEx)
                    {
                        Debug.WriteLine($"⚠️ Cache reload attempt failed: {reloadEx.Message}");
                    }
                }

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
                Debug.WriteLine($"Feature count={features.Count}; fields advertised={string.Join(",", GetFieldDefinitions(theme).Select(f => (string)f.GetType().GetProperty("name")?.GetValue(f))) } ");
                
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
        private string BuildDuckDbQuery(ThemeDefinition theme, string whereClause, string geometryParam, string spatialRel, string outFields, int maxRecords, bool returnGeometry, int? outWkid = null, int resultOffset = 0, int? geometryPrecision = null, string quantizationParameters = null)
        {
            var query = new StringBuilder();
            query.Append("SELECT ");

            // Compute tolerances from the cached viewport when available so all pages share one setting
            string simplifyTolerance = ComputeSimplifyTolerance(geometryParam, useCached: _dataLoaded);
            string snapGrid = ComputeSnapGrid(_dataLoaded ? BuildEnvelopeJson(_cachedXmin, _cachedYmin, _cachedXmax, _cachedYmax) : geometryParam, geometryPrecision, quantizationParameters);
            string lengthThreshold = ComputeMinSegmentLength(_dataLoaded ? BuildEnvelopeJson(_cachedXmin, _cachedYmin, _cachedXmax, _cachedYmax) : geometryParam);

            // Decide early whether this request can be served from the in-memory cache (only id/bbox/geometry)
            bool useCacheForFields = _dataLoaded;
            if (useCacheForFields)
            {
                if (outFields == "*")
                {
                    useCacheForFields = false;
                }
                else if (!string.Equals(outFields, "OBJECTID", StringComparison.OrdinalIgnoreCase))
                {
                    var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    { "id", "bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax", "geometry" };
                    foreach (var rf in outFields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                    {
                        if (!allowed.Contains(rf.Trim())) { useCacheForFields = false; break; }
                    }
                }
            }

            // Handle output fields - Updated for theme-specific schemas
            if (outFields == "*")
            {
                // Use theme-specific fields with common bbox; start with id
                var fieldList = new List<string> { "id" };
                
                // Add bbox fields that are common to all themes
                fieldList.AddRange(new[] { "bbox.xmin as bbox_xmin", "bbox.ymin as bbox_ymin", "bbox.xmax as bbox_xmax", "bbox.ymax as bbox_ymax" });
                
                // Add theme-specific fields (expanded on-demand from discovery)
                foreach (var field in theme.Fields.Where(f => f != "id"))
                {
                    // If field is a flattened struct member, render via expression when querying parquet
                    if (_discoveredFieldSql.TryGetValue(theme.Id, out var map) && map.TryGetValue(field, out var expr) && !useCacheForFields)
                        fieldList.Add(expr);
                    else
                    {
                        if (!string.Equals(field, "sources_property", StringComparison.OrdinalIgnoreCase))
                    fieldList.Add(field);
                    }
                }
                
                // Add geometry only if requested - use DuckDB spatial functions to properly convert WKB
                if (returnGeometry)
                {
                    // Use GeoJSON to ensure robust geometry encoding; apply simplification when appropriate
                    var geomExpr = outWkid.HasValue ? $"ST_Transform(geometry, {outWkid.Value})" : "geometry";
                    // Simplify → SnapToGrid
                    var simplified = !string.IsNullOrEmpty(simplifyTolerance) ? $"ST_Simplify({geomExpr}, {simplifyTolerance})" : geomExpr;
                    var snapped = !string.IsNullOrEmpty(snapGrid) ? $"ST_SnapToGrid({simplified}, {snapGrid})" : simplified;
                    // Optionally drop tiny segments when zoomed out (for lines)
                    var filtered = !string.IsNullOrEmpty(lengthThreshold) ? $"CASE WHEN ST_GeometryType({snapped}) IN ('LINESTRING','MULTILINESTRING') AND ST_Length({snapped}) < {lengthThreshold} THEN NULL ELSE {snapped} END" : snapped;
                    fieldList.Add($"ST_AsGeoJSON({filtered}) as geometry_geojson");
                }
                
                query.Append(string.Join(", ", fieldList));
            }
            else if (outFields == "OBJECTID")
            {
                // If returnGeometry is false, return IDs only (faster for Pro paging/selection)
                if (!returnGeometry)
                {
                    query.Append("id");
                }
                else
                {
                    var geomExpr = outWkid.HasValue ? $"ST_Transform(geometry, {outWkid.Value})" : "geometry";
                    var simplified = !string.IsNullOrEmpty(simplifyTolerance) ? $"ST_Simplify({geomExpr}, {simplifyTolerance})" : geomExpr;
                    var snapped = !string.IsNullOrEmpty(snapGrid) ? $"ST_SnapToGrid({simplified}, {snapGrid})" : simplified;
                    var filtered = !string.IsNullOrEmpty(lengthThreshold) ? $"CASE WHEN ST_GeometryType({snapped}) IN ('LINESTRING','MULTILINESTRING') AND ST_Length({snapped}) < {lengthThreshold} THEN NULL ELSE {snapped} END" : snapped;
                    query.Append($"id, ST_AsGeoJSON({filtered}) as geometry_geojson");
                }
            }
            else
            {
                // For specific field requests, ALWAYS include geometry for spatial layers
                // Translate ArcGIS field names to our underlying schema and de-duplicate
                var fieldParts = outFields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                var translatedFields = new List<string>();
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var raw in fieldParts)
                {
                    var f = raw.Trim();
                    if (f.Equals("OBJECTID", StringComparison.OrdinalIgnoreCase))
                    {
                        // OBJECTID is synthesized in attributes; don't project a column for it
                        continue;
                    }

                    string mapped = f;
                    if (f.Equals("bbox_xmin", StringComparison.OrdinalIgnoreCase)) mapped = "bbox.xmin as bbox_xmin";
                    else if (f.Equals("bbox_ymin", StringComparison.OrdinalIgnoreCase)) mapped = "bbox.ymin as bbox_ymin";
                    else if (f.Equals("bbox_xmax", StringComparison.OrdinalIgnoreCase)) mapped = "bbox.xmax as bbox_xmax";
                    else if (f.Equals("bbox_ymax", StringComparison.OrdinalIgnoreCase)) mapped = "bbox.ymax as bbox_ymax";
                    else if (f.Equals("geometry", StringComparison.OrdinalIgnoreCase)) mapped = "geometry"; // handled below
                    else if (f.Equals("OBJECTID", StringComparison.OrdinalIgnoreCase)) mapped = null; // redundant safety

                    // If this is a flattened struct field we discovered, map to its SQL expression when using parquet
                    if (mapped != null && _discoveredFieldSql.TryGetValue(theme.Id, out var map) && map.TryGetValue(f, out var expr) && !useCacheForFields)
                    {
                        mapped = expr;
                    }
                    else if (mapped != null && mapped == f && !useCacheForFields && f.IndexOf('_') > 0)
                    {
                        // If the original requested field is of the form col_member and we didn't map it yet,
                        // synthesize an expression from STRUCT or LIST(STRUCT). Avoid touching bbox_* which we handle above.
                        if (!f.StartsWith("bbox_", StringComparison.OrdinalIgnoreCase))
                        {
                            var idx = f.IndexOf('_');
                            var baseCol = f.Substring(0, idx);
                            var member = f.Substring(idx + 1).Trim().Trim('"');
                            string memberExpr = $"'{member}'";
                            if (_structColumns.TryGetValue(theme.Id, out var structCols) && structCols.Contains(baseCol))
                            {
                                mapped = $"struct_extract({baseCol}, {memberExpr}) as {f}";
                }
                else
                {
                                mapped = $"struct_extract(list_extract({baseCol}, 1), {memberExpr}) as {f}";
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(mapped))
                    {
                        // Prevent duplicate id,id when OBJECTID was also requested
                        var key = mapped;
                        // normalize alias form for duplicate detection
                        if (mapped.Contains(" as ", StringComparison.OrdinalIgnoreCase))
                            key = mapped.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0];
                        if (seen.Add(key)) translatedFields.Add(mapped);
                    }
                }

                try
                {
                    System.Diagnostics.Debug.WriteLine($"outFields map: original='{outFields}', mapped='{string.Join(", ", translatedFields)}'");
                }
                catch { }

                // If specific fields were requested that are known to be absent from the in-memory cache,
                // ensure we will route to parquet by setting a private marker in outFields (handled later)
                // This is defensive; the main routing occurs in CacheCanServe.

                // Append geometry only when requested by the client (respect returnGeometry)
                if (returnGeometry)
                {
                    var geomExpr = outWkid.HasValue ? $"ST_Transform(geometry, {outWkid.Value})" : "geometry";
                    var simplified = !string.IsNullOrEmpty(simplifyTolerance) ? $"ST_Simplify({geomExpr}, {simplifyTolerance})" : geomExpr;
                    var snapped = !string.IsNullOrEmpty(snapGrid) ? $"ST_SnapToGrid({simplified}, {snapGrid})" : simplified;
                    var filtered = !string.IsNullOrEmpty(lengthThreshold) ? $"CASE WHEN ST_GeometryType({snapped}) IN ('LINESTRING','MULTILINESTRING') AND ST_Length({snapped}) < {lengthThreshold} THEN NULL ELSE {snapped} END" : snapped;

                    bool requestedGeometry = fieldParts.Any(p => p.Trim().Equals("geometry", StringComparison.OrdinalIgnoreCase));
                    if (requestedGeometry)
                    {
                        var withGeometry = translatedFields.Select(tf => tf.Equals("geometry", StringComparison.OrdinalIgnoreCase) ? $"ST_AsGeoJSON({filtered}) as geometry_geojson" : tf).ToList();
                        query.Append(string.Join(", ", withGeometry));
                }
                else
                {
                        translatedFields.Add($"ST_AsGeoJSON({filtered}) as geometry_geojson");
                        query.Append(string.Join(", ", translatedFields));
                    }
                }
                else
                {
                    // No geometry requested; just project the translated fields
                    query.Append(string.Join(", ", translatedFields));
                }
            }

                // Note: previous helper CacheCanServe() is no longer used (routing is handled earlier)

            // Check if in-memory table exists, otherwise fallback to S3
            var tableName = GetTableName(theme);
            if (!string.IsNullOrEmpty(_aoiWkt))
            {
                if (_aoiTables.TryGetValue(theme.Id, out var aoiTable))
                {
                    tableName = aoiTable;
                }
            }
            if (useCacheForFields)
            {
                // IN-MEMORY: Query from pre-loaded in-memory table for lightning-fast performance
                query.Append($" FROM {tableName}");
            }
            else
            {
                // FALLBACK: Direct S3 query if in-memory data not loaded yet
                query.Append($" FROM read_parquet('{theme.S3Path}', filename=true, hive_partitioning=1)");
                if (_dataLoaded)
                    Debug.WriteLine($"🔎 Attribute fields requested not present in cache. Using parquet source for {theme.Name}");
                else
                Debug.WriteLine($"🌐 Direct S3 query for {theme.Name} - ensuring geometry data preservation");
            }

            var conditions = new List<string>();

            // Handle WHERE clause
            if (!string.IsNullOrEmpty(whereClause) && whereClause != "1=1")
            {
                // Translate ArcGIS OBJECTID filters to internal id field and sanitize
                var translated = whereClause;
                // OBJECTID = 123 or OBJECTID IN (...)
                translated = Regex.Replace(translated, @"\bOBJECTID\b", "id", RegexOptions.IgnoreCase);
                // ArcGIS sometimes quotes field names
                translated = translated.Replace("\"OBJECTID\"", "id");
                if (!string.Equals(translated, whereClause, StringComparison.Ordinal))
                {
                    System.Diagnostics.Debug.WriteLine($"WHERE translated: '{whereClause}' -> '{translated}'");
                }
                conditions.Add($"({translated})");
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

            // Optional refinement: if we had an envelope filter, also check ST_Intersects to reduce false positives
            // Add as an additional condition so WHERE clause is constructed correctly
            if (returnGeometry && !string.IsNullOrEmpty(geometryParam))
            {
                try
                {
                    using var doc = JsonDocument.Parse(geometryParam);
                    var root = doc.RootElement;
                    if (root.TryGetProperty("xmin", out var xminProp) &&
                        root.TryGetProperty("ymin", out var yminProp) &&
                        root.TryGetProperty("xmax", out var xmaxProp) &&
                        root.TryGetProperty("ymax", out var ymaxProp))
                    {
                        var xmin = xminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymin = yminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var xmax = xmaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymax = ymaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        conditions.Add($"ST_Intersects(geometry, ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))");
                    }
                }
                catch { }
            }

            if (conditions.Count > 0)
            {
                query.Append(" WHERE ");
                query.Append(string.Join(" AND ", conditions));
            }

            // Stable spatial-ish paging for uniform fill: sort by bbox then id
            query.Append(" ORDER BY bbox.ymin, bbox.xmin, id");
            query.Append($" LIMIT {maxRecords}");
            if (resultOffset > 0)
            {
                query.Append($" OFFSET {resultOffset}");
            }

            // Concise query summary
            var src = _dataLoaded ? "in-memory" : "s3";
            if (!string.IsNullOrEmpty(geometryParam) && TryParseEnvelope(geometryParam, out var qxmin, out var qymin, out var qxmax, out var qymax))
            {
                Debug.WriteLine($"🧭 {src} query for {theme.Name}: bbox=({qxmin:G4},{qymin:G4})–({qxmax:G4},{qymax:G4}) limit={maxRecords} offset={resultOffset} tol={(string.IsNullOrEmpty(simplifyTolerance) ? "none" : simplifyTolerance)}");
            }
            else
            {
                Debug.WriteLine($"🧭 {src} query for {theme.Name}: limit={maxRecords} offset={resultOffset} tol={(string.IsNullOrEmpty(simplifyTolerance) ? "none" : simplifyTolerance)}");
            }

            if (_verboseSqlLogging)
            {
            if (_dataLoaded)
                    Debug.WriteLine($"⚡ SQL: {query}");
                else
                    Debug.WriteLine($"📡 SQL: {query}");
            }
            
            return query.ToString();
        }

        private string ComputeSimplifyTolerance(string geometryParam, bool useCached = false)
        {
            try
            {
                double xmin, ymin, xmax, ymax;
                if (useCached && _cachedXmax > _cachedXmin && _cachedYmax > _cachedYmin)
                {
                    xmin = _cachedXmin; ymin = _cachedYmin; xmax = _cachedXmax; ymax = _cachedYmax;
            }
            else
            {
                    if (string.IsNullOrEmpty(geometryParam)) return null;
                    using var doc = JsonDocument.Parse(geometryParam);
                    var root = doc.RootElement;
                    if (root.TryGetProperty("xmin", out var xminProp) &&
                        root.TryGetProperty("ymin", out var yminProp) &&
                        root.TryGetProperty("xmax", out var xmaxProp) &&
                        root.TryGetProperty("ymax", out var ymaxProp))
                    {
                        xmin = xminProp.GetDouble(); ymin = yminProp.GetDouble(); xmax = xmaxProp.GetDouble(); ymax = ymaxProp.GetDouble();
                    }
                    else if (!TryParseEnvelope(geometryParam, out xmin, out ymin, out xmax, out ymax))
                    {
                        return null;
                    }
                }

                var width = Math.Abs(xmax - xmin);
                var height = Math.Abs(ymax - ymin);
                var span = Math.Max(width, height);
                if (span <= 0) return null;

                // Choose a tolerance proportional to span. Tuned to keep detail when zoomed in and reduce vertices when zoomed out.
                var tol = span / 20000.0; // e.g., ~0.00005 degrees for a 1-degree span
                if (tol < 1e-7) return null; // too small, skip
                return tol.ToString("G", CultureInfo.InvariantCulture);
            }
            catch { return null; }
        }

        private static string BuildEnvelopeJson(double xmin, double ymin, double xmax, double ymax)
        {
            // Use string.Format with escaped braces to avoid interpolation escaping issues
            return string.Format(
                CultureInfo.InvariantCulture,
                "{{\"xmin\":{0},\"ymin\":{1},\"xmax\":{2},\"ymax\":{3}}}",
                xmin.ToString("G", CultureInfo.InvariantCulture),
                ymin.ToString("G", CultureInfo.InvariantCulture),
                xmax.ToString("G", CultureInfo.InvariantCulture),
                ymax.ToString("G", CultureInfo.InvariantCulture)
            );
        }

        private string BuildDuckDbCountQuery(ThemeDefinition theme, string whereClause, string geometryParam, string spatialRel, int? geometryPrecision = null, string quantizationParameters = null)
        {
            var query = new StringBuilder();
            var tableName = GetTableName(theme);
            if (!string.IsNullOrEmpty(_aoiWkt))
            {
                if (_aoiTables.TryGetValue(theme.Id, out var aoiTable))
                {
                    tableName = aoiTable;
                }
            }
            query.Append("SELECT COUNT(*) as cnt FROM ");
            if (_dataLoaded)
            {
                query.Append(tableName);
            }
            else
            {
                query.Append($"read_parquet('{theme.S3Path}', filename=true, hive_partitioning=1)");
            }

            var conditions = new List<string>();
            if (!string.IsNullOrEmpty(whereClause) && whereClause != "1=1")
            {
                conditions.Add($"({whereClause})");
            }
            if (!string.IsNullOrEmpty(geometryParam))
            {
                var spatialCondition = ConvertArcGISGeometryToSql(geometryParam, spatialRel);
                if (!string.IsNullOrEmpty(spatialCondition))
                    conditions.Add(spatialCondition);

                // add exact intersects for envelope
                try
                {
                    using var doc = JsonDocument.Parse(geometryParam);
                    var root = doc.RootElement;
                    if (root.TryGetProperty("xmin", out var xminProp) &&
                        root.TryGetProperty("ymin", out var yminProp) &&
                        root.TryGetProperty("xmax", out var xmaxProp) &&
                        root.TryGetProperty("ymax", out var ymaxProp))
                    {
                        var xmin = xminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymin = yminProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var xmax = xmaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        var ymax = ymaxProp.GetDouble().ToString("G", CultureInfo.InvariantCulture);
                        conditions.Add($"ST_Intersects(geometry, ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))");
                    }
                }
                catch { }
            }

            if (conditions.Count > 0)
            {
                query.Append(" WHERE ");
                query.Append(string.Join(" AND ", conditions));
            }
            
            return query.ToString();
        }

        // Quantization / snap-to-grid: derive grid size from geometryPrecision or quantizationParameters
        private string ComputeSnapGrid(string geometryParam, int? geometryPrecision, string quantizationParameters)
        {
            try
            {
                if (geometryPrecision.HasValue && geometryPrecision.Value > 0)
                {
                    // Convert digits into approx degrees grid using bbox width
                    if (TryParseEnvelope(geometryParam, out var xmin, out var ymin, out var xmax, out var ymax))
                    {
                        var span = Math.Max(Math.Abs(xmax - xmin), Math.Abs(ymax - ymin));
                        if (span > 0)
                        {
                            // grid ~ span / 10^precision
                            var grid = span / Math.Pow(10, geometryPrecision.Value);
                            if (grid > 0) return grid.ToString("G", CultureInfo.InvariantCulture);
                        }
                    }
                }
                // Basic support for quantizationParameters: {"tolerance": number}
                if (!string.IsNullOrEmpty(quantizationParameters))
                {
                    using var doc = JsonDocument.Parse(quantizationParameters);
                    if (doc.RootElement.TryGetProperty("tolerance", out var tol) && tol.ValueKind == JsonValueKind.Number)
                    {
                        var val = tol.GetDouble();
                        if (val > 0) return val.ToString("G", CultureInfo.InvariantCulture);
                    }
                }
            }
            catch { }
            return null;
        }

        // Periodically evict old materialized export tables to keep memory bounded
        private void EvictExpiredMaterializations()
        {
            if (_materializedTouched == null || _materializedTouched.Count == 0) return;
            var now = DateTime.UtcNow;
            var expired = _materializedTouched.Where(kv => now - kv.Value > _materializedTtl).Select(kv => kv.Key).ToList();
            foreach (var key in expired)
            {
                if (_materializedExports.TryGetValue(key, out var table))
                {
                    try
                    {
                        _ = _dataProcessor.ExecuteQueryAsync($"DROP TABLE IF EXISTS {table}");
                    }
                    catch { }
                }
                _materializedExports.Remove(key);
                _materializedTouched.Remove(key);
                System.Diagnostics.Debug.WriteLine($"🧹 Evicted materialized export table for key {key}");
            }
        }

        // Drop tiny lines when zoomed out
        private string ComputeMinSegmentLength(string geometryParam)
        {
            try
            {
                if (TryParseEnvelope(geometryParam, out var xmin, out var ymin, out var xmax, out var ymax))
                {
                    var span = Math.Max(Math.Abs(xmax - xmin), Math.Abs(ymax - ymin));
                    if (span <= 0) return null;
                    // threshold ~ 1/100000 of span in degrees (~1m at 1 deg ~ 111km). Tuned conservatively.
                    var thr = span / 100000.0;
                    return thr.ToString("G", CultureInfo.InvariantCulture);
                }
            }
            catch { }
            return null;
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

                    // Polygon rings: compute bbox and return bbox predicate
                    if (root.TryGetProperty("rings", out var ringsElem) && ringsElem.ValueKind == JsonValueKind.Array)
                    {
                        double minx = double.PositiveInfinity, miny = double.PositiveInfinity, maxx = double.NegativeInfinity, maxy = double.NegativeInfinity;
                        foreach (var ring in ringsElem.EnumerateArray())
                        {
                            foreach (var c in ring.EnumerateArray())
                            {
                                double x = c[0].GetDouble();
                                double y = c[1].GetDouble();
                                if (x < minx) minx = x; if (x > maxx) maxx = x;
                                if (y < miny) miny = y; if (y > maxy) maxy = y;
                            }
                        }
                        var xmin = minx.ToString("G", CultureInfo.InvariantCulture);
                        var ymin = miny.ToString("G", CultureInfo.InvariantCulture);
                        var xmax = maxx.ToString("G", CultureInfo.InvariantCulture);
                        var ymax = maxy.ToString("G", CultureInfo.InvariantCulture);
                        return $"(bbox.xmin <= {xmax} AND bbox.xmax >= {xmin} AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin})";
                    }

                    // Polyline paths: compute bbox and return bbox predicate
                    if (root.TryGetProperty("paths", out var pathsElem) && pathsElem.ValueKind == JsonValueKind.Array)
                    {
                        double minx = double.PositiveInfinity, miny = double.PositiveInfinity, maxx = double.NegativeInfinity, maxy = double.NegativeInfinity;
                        foreach (var path in pathsElem.EnumerateArray())
                        {
                            foreach (var c in path.EnumerateArray())
                            {
                                double x = c[0].GetDouble();
                                double y = c[1].GetDouble();
                                if (x < minx) minx = x; if (x > maxx) maxx = x;
                                if (y < miny) miny = y; if (y > maxy) maxy = y;
                            }
                        }
                        var xmin = minx.ToString("G", CultureInfo.InvariantCulture);
                        var ymin = miny.ToString("G", CultureInfo.InvariantCulture);
                        var xmax = maxx.ToString("G", CultureInfo.InvariantCulture);
                        var ymax = maxy.ToString("G", CultureInfo.InvariantCulture);
                        return $"(bbox.xmin <= {xmax} AND bbox.xmax >= {xmin} AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin})";
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error parsing geometry JSON: {ex.Message}");
            }

            return string.Empty;
        }

        private bool TryParseEnvelope(string geometryJson, out double xmin, out double ymin, out double xmax, out double ymax)
        {
            xmin = ymin = xmax = ymax = 0;
            try
            {
                using var doc = JsonDocument.Parse(geometryJson);
                var root = doc.RootElement;
                if (root.TryGetProperty("xmin", out var xminProp) &&
                    root.TryGetProperty("ymin", out var yminProp) &&
                    root.TryGetProperty("xmax", out var xmaxProp) &&
                    root.TryGetProperty("ymax", out var ymaxProp))
                {
                    xmin = xminProp.GetDouble();
                    ymin = yminProp.GetDouble();
                    xmax = xmaxProp.GetDouble();
                    ymax = ymaxProp.GetDouble();
                    return true;
                }

                if (root.TryGetProperty("rings", out var ringsElem) && ringsElem.ValueKind == JsonValueKind.Array)
                {
                    double minx = double.PositiveInfinity, miny = double.PositiveInfinity, maxx = double.NegativeInfinity, maxy = double.NegativeInfinity;
                    foreach (var ring in ringsElem.EnumerateArray())
                    {
                        foreach (var c in ring.EnumerateArray())
                        {
                            double x = c[0].GetDouble();
                            double y = c[1].GetDouble();
                            if (x < minx) minx = x; if (x > maxx) maxx = x; if (y < miny) miny = y; if (y > maxy) maxy = y;
                        }
                    }
                    xmin = minx; ymin = miny; xmax = maxx; ymax = maxy; return true;
                }

                if (root.TryGetProperty("paths", out var pathsElem) && pathsElem.ValueKind == JsonValueKind.Array)
                {
                    double minx = double.PositiveInfinity, miny = double.PositiveInfinity, maxx = double.NegativeInfinity, maxy = double.NegativeInfinity;
                    foreach (var path in pathsElem.EnumerateArray())
                    {
                        foreach (var c in path.EnumerateArray())
                        {
                            double x = c[0].GetDouble();
                            double y = c[1].GetDouble();
                            if (x < minx) minx = x; if (x > maxx) maxx = x; if (y < miny) miny = y; if (y > maxy) maxy = y;
                        }
                    }
                    xmin = minx; ymin = miny; xmax = maxx; ymax = maxy; return true;
                }
            }
            catch { }
            return false;
        }

        /// <summary>
        /// Execute DuckDB query and return features
        /// </summary>
        private async Task<List<object>> ExecuteDuckDbQuery(string query)
        {
            var features = new List<object>();

            try
            {
                if (_verboseSqlLogging)
            {
                Debug.WriteLine($"Executing DuckDB query: {query}");
                    Debug.WriteLine($"🔍 Query includes geometry_geojson: {query.Contains("geometry_geojson")}");
                Debug.WriteLine($"🔍 Full query text: {query}");
                }

                await _duckSemaphore.WaitAsync();
                List<Dictionary<string, object>> queryResults = null;
                int attempts = 0;
                Exception lastEx = null;
                do
                {
                    try
                    {
                        queryResults = await _dataProcessor.ExecuteQueryAsync(query);
                        lastEx = null;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        // Simple transient retry once for DuckDB execution errors (e.g., during cache rebuild)
                        if (++attempts <= 2)
                        {
                            await Task.Delay(300);
                            continue;
                        }
                        throw;
                    }
                } while (lastEx != null);
                
                var objectId = 1;
                foreach (var row in queryResults)
                {
                    var attributes = new Dictionary<string, object>
                    {
                        ["OBJECTID"] = ComputeObjectId(row.ContainsKey("id") ? row["id"]?.ToString() : objectId.ToString())
                    };

                    object geometry = null;

                    // Add all columns from the query result and extract geometry
                    foreach (var kvp in row)
                    {
                        if (kvp.Key == "geometry_geojson")
                        {
                            if (kvp.Value is string gj && !string.IsNullOrWhiteSpace(gj))
                            {
                                geometry = ParseGeoJsonToArcGISGeometry(gj);
                            }
                            continue; // skip adding to attributes
                        }

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

                Debug.WriteLine($"📦 Returned={features.Count}");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error executing DuckDB query: {ex.Message}");
                throw;
            }
            finally
            {
                if (_duckSemaphore.CurrentCount == 0) _duckSemaphore.Release();
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

        private object ParseGeoJsonToArcGISGeometry(string geojson)
        {
            try
            {
                using var doc = JsonDocument.Parse(geojson);
                var root = doc.RootElement;
                if (!root.TryGetProperty("type", out var typeProp)) return null;
                var type = typeProp.GetString();
                if (string.Equals(type, "Point", StringComparison.OrdinalIgnoreCase))
                {
                    var coords = root.GetProperty("coordinates");
                    return new { x = coords[0].GetDouble(), y = coords[1].GetDouble(), spatialReference = _spatialReference };
                }
                if (string.Equals(type, "LineString", StringComparison.OrdinalIgnoreCase))
                {
                    var coords = root.GetProperty("coordinates");
                    var path = new List<double[]>();
                    foreach (var c in coords.EnumerateArray()) path.Add(new[] { c[0].GetDouble(), c[1].GetDouble() });
                    return new { paths = new[] { path.ToArray() }, spatialReference = _spatialReference };
                }
                if (string.Equals(type, "Polygon", StringComparison.OrdinalIgnoreCase))
                {
                    var rings = new List<List<double[]>>();
                    foreach (var ring in root.GetProperty("coordinates").EnumerateArray())
                    {
                        var r = new List<double[]>();
                        foreach (var c in ring.EnumerateArray()) r.Add(new[] { c[0].GetDouble(), c[1].GetDouble() });
                        rings.Add(r);
                    }
                    return new { rings = rings, spatialReference = _spatialReference };
                }
                if (string.Equals(type, "MultiPoint", StringComparison.OrdinalIgnoreCase))
                {
                    var coords = root.GetProperty("coordinates");
                    var enumerator = coords.EnumerateArray();
                    if (enumerator.MoveNext())
                    {
                        var p = enumerator.Current;
                        return new { x = p[0].GetDouble(), y = p[1].GetDouble(), spatialReference = _spatialReference };
                    }
                }
                if (string.Equals(type, "MultiLineString", StringComparison.OrdinalIgnoreCase))
                {
                    var lines = root.GetProperty("coordinates");
                    var lineEnumerator = lines.EnumerateArray();
                    if (lineEnumerator.MoveNext())
                    {
                        var coords = lineEnumerator.Current;
                        var path = new List<double[]>();
                        foreach (var c in coords.EnumerateArray()) path.Add(new[] { c[0].GetDouble(), c[1].GetDouble() });
                        return new { paths = new[] { path.ToArray() }, spatialReference = _spatialReference };
                    }
                }
                if (string.Equals(type, "MultiPolygon", StringComparison.OrdinalIgnoreCase))
                {
                    var polys = root.GetProperty("coordinates");
                    var polyEnumerator = polys.EnumerateArray();
                    if (polyEnumerator.MoveNext())
                    {
                        var firstPoly = polyEnumerator.Current;
                        var rings = new List<List<double[]>>();
                        foreach (var ring in firstPoly.EnumerateArray())
                        {
                            var r = new List<double[]>();
                            foreach (var c in ring.EnumerateArray()) r.Add(new[] { c[0].GetDouble(), c[1].GetDouble() });
                            rings.Add(r);
                        }
                        return new { rings = rings, spatialReference = _spatialReference };
                    }
                }
            }
            catch { }
            return null;
        }

        /// <summary>
        /// Parse WKB geometry binary data into ArcGIS REST API geometry format
        /// Uses the same approach as the working Data Loader
        /// </summary>
        private object ParseWkbToArcGISGeometry(object wkbData)
        {
            // ULTIMATE SAFETY WRAPPER: Prevent ExecutionEngineException from crashing entire service
            try
            {
                return ParseWkbGeometryInternal(wkbData);
            }
            catch (System.Exception ex) when (ex.GetType().Name == "ExecutionEngineException")
            {
                Debug.WriteLine($"🚨 CRITICAL: Critical runtime exception caught and prevented! {ex.Message}");
                return null; // Safe fallback - NEVER let this crash the service
            }
            catch (System.AccessViolationException ex)
            {
                Debug.WriteLine($"🚨 CRITICAL: AccessViolationException caught and prevented! {ex.Message}");
                return null; // Safe fallback
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 WKB parsing exception safely handled: {ex.Message}");
                return null; // Safe fallback
            }
        }

        /// <summary>
        /// Internal WKB geometry parsing with all the detailed validation logic
        /// </summary>
        private object ParseWkbGeometryInternal(object wkbData)
        {
            try
            {
                if (wkbData == null || wkbData == DBNull.Value)
                {
                    Debug.WriteLine($"🔍 WKB data is null or DBNull");
                    return null;
                }

                Debug.WriteLine($"🔍 Processing WKB data of type: {wkbData.GetType()}");
                byte[] wkbBytes;
                
                // Handle different possible types of binary data
                if (wkbData is byte[] bytes)
                {
                    wkbBytes = bytes;
                    Debug.WriteLine($"🔍 Got byte array of length: {bytes.Length}");
                }
                else if (wkbData is string hexString)
                {
                    Debug.WriteLine($"🔍 Got hex string of length: {hexString.Length}");
                    // Convert hex string to bytes
                    wkbBytes = Convert.FromHexString(hexString);
                }
                else if (wkbData is System.IO.UnmanagedMemoryStream memoryStream)
                {
                    Debug.WriteLine($"🔍 Got UnmanagedMemoryStream of length: {memoryStream.Length}");
                    
                    // Safety check for memory stream
                    if (memoryStream.Length <= 0 || memoryStream.Length > 100000000) // 100MB limit
                    {
                        Debug.WriteLine($"🔍 Invalid memory stream length: {memoryStream.Length}");
                        return null;
                    }
                    
                    try
                    {
                        // Read all bytes from the memory stream with comprehensive safety checks
                        int totalBytesRead = 0;
                        int bytesToRead = (int)memoryStream.Length;
                        
                        try
                        {
                            wkbBytes = new byte[memoryStream.Length];
                            memoryStream.Position = 0;
                            
                            while (totalBytesRead < bytesToRead)
                            {
                                int bytesRead = memoryStream.Read(wkbBytes, totalBytesRead, bytesToRead - totalBytesRead);
                                if (bytesRead == 0)
                                    break; // End of stream
                                totalBytesRead += bytesRead;
                            }
                        }
                        catch (System.AccessViolationException ex)
                        {
                            Debug.WriteLine($"🚨 AccessViolationException during UnmanagedMemoryStream read: {ex.Message}");
                            return null;
                        }
                        catch (System.Exception ex) when (ex.GetType().Name == "ExecutionEngineException")
                        {
                            Debug.WriteLine($"🚨 Critical runtime exception during UnmanagedMemoryStream read: {ex.Message}");
                            return null;
                        }
                        
                        // Additional safety check - ensure we read what we expected
                        if (totalBytesRead != bytesToRead)
                        {
                            Debug.WriteLine($"🔍 Partial read warning: expected {bytesToRead}, got {totalBytesRead}");
                        }
                        
                        Debug.WriteLine($"🔍 Successfully read {totalBytesRead} bytes from UnmanagedMemoryStream");
                        Debug.WriteLine($"🔍 First 16 bytes: [{string.Join(",", wkbBytes.Take(Math.Min(16, wkbBytes.Length)).Select(b => b.ToString("X2")))}]");
                    }
                    catch (Exception streamEx)
                    {
                        Debug.WriteLine($"🔍 Error reading UnmanagedMemoryStream: {streamEx.Message}");
                        return null;
                    }
                }
                else
                {
                    Debug.WriteLine($"🔍 Unsupported WKB data type: {wkbData.GetType()}, value: {wkbData}");
                    return null;
                }

                // Comprehensive WKB validation
                if (wkbBytes == null)
                {
                    Debug.WriteLine($"🔍 WKB bytes array is null");
                    return null;
                }
                
                if (wkbBytes.Length < 9) // Minimum WKB size
                {
                    Debug.WriteLine($"🔍 WKB too short: {wkbBytes.Length} bytes (minimum 9 required)");
                    return null;
                }

                Debug.WriteLine($"🔍 WKB total length: {wkbBytes.Length} bytes");
                
                // CRITICAL: Pre-validate data patterns to prevent ExecutionEngineException
                if (!ValidateWkbDataIntegrity(wkbBytes))
                {
                    Debug.WriteLine($"🔍 WKB data failed integrity validation - treating as null geometry");
                    return null;
                }

                // Parse WKB header with safety checks
                int pos = 0;
                byte byteOrder = wkbBytes[pos++];
                bool littleEndian = byteOrder == 1;
                
                Debug.WriteLine($"🔍 WKB byte order: 0x{byteOrder:X2}, little endian: {littleEndian}");
                
                // Handle mixed data types - some "geometries" are actually metadata/IDs
                if (byteOrder != 0 && byteOrder != 1)
                {
                    Debug.WriteLine($"🔍 Invalid WKB byte order: 0x{byteOrder:X2} (expected 0x00 or 0x01) - this appears to be non-geometry data");
                    
                    // CRITICAL: Handle various patterns of non-WKB data
                    if (wkbBytes.Length == 32 && (byteOrder == 0x24 || byteOrder == 0x00))
                    {
                        Debug.WriteLine($"🔍 Detected 32-byte non-WKB data pattern (prefix 0x{byteOrder:X2}) - treating as null geometry");
                        return null; // Safely return null instead of crashing
                    }
                    
                    // Handle other invalid byte order values (like 0x02, 0x03, etc.)
                    if (byteOrder > 1)
                    {
                        Debug.WriteLine($"🔍 Invalid byte order 0x{byteOrder:X2} suggests corrupted or non-WKB data - treating as null geometry");
                        return null;
                    }
                    
                    Debug.WriteLine($"🔍 Unknown data format with byte order 0x{byteOrder:X2} - treating as null geometry for safety");
                    return null;
                }
                
                uint geometryType = ReadUInt32(wkbBytes, ref pos, littleEndian);
                
                Debug.WriteLine($"🔍 WKB geometry type parsed: {geometryType}");
                
                // Handle basic geometry types with enhanced detection
                switch (geometryType)
                {
                    case 0: // NULL geometry - return null safely
                        Debug.WriteLine($"🔍 NULL geometry (type 0) - returning null");
                        return null;
                        
                    case 1: // Point
                        Debug.WriteLine($"🔍 Processing Point geometry");
                        return ParseWkbPoint(wkbBytes, ref pos, littleEndian);
                        
                    case 2: // LineString  
                        Debug.WriteLine($"🔍 Processing LineString geometry");
                        return ParseWkbLineString(wkbBytes, ref pos, littleEndian);
                        
                    case 3: // Polygon
                        Debug.WriteLine($"🔍 Processing Polygon geometry");
                        return ParseWkbPolygon(wkbBytes, ref pos, littleEndian);
                        
                    case 4: // MultiPoint - BUT appears to have non-standard format with padding
                        Debug.WriteLine($"🔍 Processing geometry type 4 (claimed MultiPoint) - total bytes: {wkbBytes.Length}");
                        
                        // CRITICAL: Based on debug logs, these "MultiPoint" geometries have additional padding
                        // Pattern observed: [01,04,00,00,00,00,00,00,coordinate_data...]
                        // Standard WKB would be: [01,04,00,00,00,point_count,coordinate_data...]
                        // But we're seeing 3 extra zero bytes before the coordinate data
                        
                        Debug.WriteLine($"🔍 Attempting non-standard MultiPoint format with padding compensation");
                        return ParseWkbMultiPointWithPadding(wkbBytes, ref pos, littleEndian);
                        
                    default:
                        Debug.WriteLine($"🔍 Unsupported WKB geometry type: {geometryType}");
                        return null;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error parsing WKB geometry: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Parse WKB Point geometry
        /// </summary>
        private object ParseWkbPoint(byte[] wkbBytes, ref int pos, bool littleEndian)
        {
            try
            {
                // Safety bounds checking  
                if (wkbBytes == null)
                {
                    Debug.WriteLine($"🔍 ParseWkbPoint: bytes array is null");
                    return null;
                }
                
                if (pos < 0 || pos + 16 > wkbBytes.Length) // Need 2 doubles (8 bytes each)
                {
                    Debug.WriteLine($"🔍 ParseWkbPoint: Insufficient bytes - pos {pos}, need 16 bytes, have {wkbBytes.Length - pos}");
                    return null;
                }

                // CRITICAL: Enhanced safety - check for obviously corrupted byte patterns before parsing
                Debug.WriteLine($"🔍 ParseWkbPoint: Examining bytes at pos {pos}: [{string.Join(",", wkbBytes.Skip(pos).Take(16).Select(b => $"{b:X2}"))}]");
                
                double x, y;
                
                try
                {
                    x = ReadDouble(wkbBytes, ref pos, littleEndian);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"🔍 ParseWkbPoint: Failed to read X coordinate - {ex.Message}");
                    return null;
                }
                
                try
                {
                    y = ReadDouble(wkbBytes, ref pos, littleEndian);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"🔍 ParseWkbPoint: Failed to read Y coordinate - {ex.Message}");
                    return null;
                }
                
                // Validate coordinate values are reasonable for geographic data
                if (double.IsNaN(x) || double.IsNaN(y) || double.IsInfinity(x) || double.IsInfinity(y))
                {
                    Debug.WriteLine($"🔍 ParseWkbPoint: Invalid coordinates (NaN/Infinity) x={x}, y={y}");
                    return null;
                }
                
                // CRITICAL: Reject astronomical coordinates - indicates wrong format interpretation
                // Expand the checks to catch more edge cases that could cause ExecutionEngineException
                if (Math.Abs(x) > 1e15 || Math.Abs(y) > 1e15 ||   // Extremely large values
                    Math.Abs(x) < 1e-15 || Math.Abs(y) < 1e-15 ||   // Extremely small values (but not zero)
                    (Math.Abs(x) > 360 && Math.Abs(y) > 180))       // Invalid geographic coordinates
                {
                    Debug.WriteLine($"🔍 ParseWkbPoint: INVALID coordinates (astronomical values) x={x}, y={y}");
                    Debug.WriteLine($"🔍 ParseWkbPoint: This suggests wrong WKB format interpretation - rejecting");
                    return null;
                }
                
                Debug.WriteLine($"🔍 ParseWkbPoint: Successfully parsed point ({x}, {y})");
                
                return new
                {
                    x = x,
                    y = y,
                    spatialReference = _spatialReference
                };
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 ParseWkbPoint failed safely: {ex.Message}");
                return null; // Safe fallback
            }
        }

        /// <summary>
        /// Parse WKB LineString geometry
        /// </summary>
        private object ParseWkbLineString(byte[] wkbBytes, ref int pos, bool littleEndian)
        {
            uint numPoints = ReadUInt32(wkbBytes, ref pos, littleEndian);
            var paths = new List<List<double[]>>();
            var path = new List<double[]>();
            
            for (int i = 0; i < numPoints; i++)
            {
                if (wkbBytes.Length < pos + 16)
                    break;
                    
                double x = ReadDouble(wkbBytes, ref pos, littleEndian);
                double y = ReadDouble(wkbBytes, ref pos, littleEndian);
                path.Add(new double[] { x, y });
            }
            
            paths.Add(path);
            
            return new
            {
                paths = paths,
                spatialReference = _spatialReference
            };
        }

        /// <summary>
        /// Parse WKB Polygon geometry
        /// </summary>
        private object ParseWkbPolygon(byte[] wkbBytes, ref int pos, bool littleEndian)
        {
            uint numRings = ReadUInt32(wkbBytes, ref pos, littleEndian);
            var rings = new List<List<double[]>>();
            
            for (int r = 0; r < numRings; r++)
            {
                uint numPoints = ReadUInt32(wkbBytes, ref pos, littleEndian);
                var ring = new List<double[]>();
                
                for (int i = 0; i < numPoints; i++)
                {
                    if (wkbBytes.Length < pos + 16)
                        break;
                        
                    double x = ReadDouble(wkbBytes, ref pos, littleEndian);
                    double y = ReadDouble(wkbBytes, ref pos, littleEndian);
                    ring.Add(new double[] { x, y });
                }
                
                rings.Add(ring);
            }
            
            return new
            {
                rings = rings,
                spatialReference = _spatialReference
            };
        }

        /// <summary>
        /// Parse WKB MultiPoint geometry (simplified to first point)
        /// </summary>
        private object ParseWkbMultiPoint(byte[] wkbBytes, ref int pos, bool littleEndian)
        {
            try
            {
                // Safety bounds check
                if (wkbBytes == null || pos < 0 || pos + 4 > wkbBytes.Length)
                {
                    Debug.WriteLine($"🔍 MultiPoint: Invalid bounds - pos {pos}, array length {wkbBytes?.Length ?? 0}");
                    return null;
                }
                
                uint numPoints = ReadUInt32(wkbBytes, ref pos, littleEndian);
                Debug.WriteLine($"🔍 MultiPoint: Found {numPoints} points");
                
                // CRITICAL: Check for suspicious values that indicate corrupted data
                if (numPoints == 0)
                {
                    Debug.WriteLine($"🔍 MultiPoint: No points - returning null");
                    return null;
                }
                
                if (numPoints > 1000000) // 1M points is unreasonable - indicates corruption
                {
                    Debug.WriteLine($"🔍 MultiPoint: Suspicious point count {numPoints} - likely corrupted data, treating as null");
                    return null;
                }
                
                // Check if we have enough bytes for the first point (5 byte header + 16 bytes for coordinates)
                if (pos + 21 > wkbBytes.Length)
                {
                    Debug.WriteLine($"🔍 MultiPoint: Insufficient bytes for first point - pos {pos}, need 21 more bytes, have {wkbBytes.Length - pos}");
                    return null;
                }
                
                // Skip byte order and geometry type for first point (5 bytes)
                pos += 5;
                return ParseWkbPoint(wkbBytes, ref pos, littleEndian);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 MultiPoint parsing failed safely: {ex.Message}");
                return null; // Safe fallback
            }
        }

        /// <summary>
        /// Validate WKB data integrity to prevent ExecutionEngineException
        /// </summary>
        private bool ValidateWkbDataIntegrity(byte[] wkbBytes)
        {
            try
            {
                if (wkbBytes == null || wkbBytes.Length < 9)
                {
                    Debug.WriteLine($"🔍 ValidateWkbDataIntegrity: Insufficient data length");
                    return false;
                }
                
                // Check byte order
                byte byteOrder = wkbBytes[0];
                if (byteOrder != 0x00 && byteOrder != 0x01)
                {
                    Debug.WriteLine($"🔍 ValidateWkbDataIntegrity: Invalid byte order 0x{byteOrder:X2}");
                    return false;
                }
                
                // Check geometry type (should be reasonable)
                bool littleEndian = byteOrder == 0x01;
                uint geometryType;
                
                if (littleEndian)
                {
                    geometryType = (uint)(wkbBytes[1] | (wkbBytes[2] << 8) | (wkbBytes[3] << 16) | (wkbBytes[4] << 24));
                }
                else
                {
                    geometryType = (uint)((wkbBytes[1] << 24) | (wkbBytes[2] << 16) | (wkbBytes[3] << 8) | wkbBytes[4]);
                }
                
                // Validate geometry type is within reasonable range (0 = NULL is valid)
                if (geometryType > 7)
                {
                    Debug.WriteLine($"🔍 ValidateWkbDataIntegrity: Invalid geometry type {geometryType}");
                    return false;
                }
                
                // For MultiPoint (type 4), check if the structure looks reasonable
                if (geometryType == 4)
                {
                    // Check if we have the expected padding pattern: [00,00,00] after geometry type
                    if (wkbBytes.Length >= 8 && 
                        wkbBytes[5] == 0x00 && wkbBytes[6] == 0x00 && wkbBytes[7] == 0x00)
                    {
                        Debug.WriteLine($"🔍 ValidateWkbDataIntegrity: Found expected padding pattern for type 4");
                        return true;
                    }
                    
                    // Check if the "point count" looks reasonable (if standard WKB)
                    uint pointCount;
                    if (littleEndian)
                    {
                        pointCount = (uint)(wkbBytes[5] | (wkbBytes[6] << 8) | (wkbBytes[7] << 16) | (wkbBytes[8] << 24));
                    }
                    else
                    {
                        pointCount = (uint)((wkbBytes[5] << 24) | (wkbBytes[6] << 16) | (wkbBytes[7] << 8) | wkbBytes[8]);
                    }
                    
                    // If point count is astronomical, this suggests corrupted data
                    if (pointCount > 1000000)
                    {
                        Debug.WriteLine($"🔍 ValidateWkbDataIntegrity: Suspicious point count {pointCount} - likely corrupted");
                        return false;
                    }
                }
                
                Debug.WriteLine($"🔍 ValidateWkbDataIntegrity: Data appears valid (type={geometryType}, length={wkbBytes.Length})");
                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 ValidateWkbDataIntegrity: Exception during validation - {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// Parse WKB MultiPoint geometry with non-standard padding compensation
        /// </summary>
        private object ParseWkbMultiPointWithPadding(byte[] wkbBytes, ref int pos, bool littleEndian)
        {
            try
            {
                Debug.WriteLine($"🔍 ParseWkbMultiPointWithPadding: Starting at pos {pos}, total length {wkbBytes.Length}");
                
                // Based on observed patterns, skip additional padding bytes
                // Pattern: [geometry_type][00,00,00][coordinate_data...]
                if (pos + 3 <= wkbBytes.Length && 
                    wkbBytes[pos] == 0x00 && wkbBytes[pos + 1] == 0x00 && wkbBytes[pos + 2] == 0x00)
                {
                    Debug.WriteLine($"🔍 Found expected padding bytes [00,00,00] at pos {pos} - skipping");
                    pos += 3;
                }
                else
                {
                    Debug.WriteLine($"🔍 No padding found at pos {pos} - bytes: [{wkbBytes[pos]:X2},{wkbBytes[pos + 1]:X2},{wkbBytes[pos + 2]:X2}]");
                }
                
                // Check if we have enough bytes for at least one point (16 bytes for x,y coordinates)
                if (pos + 16 > wkbBytes.Length)
                {
                    Debug.WriteLine($"🔍 Insufficient bytes for coordinate parsing - pos {pos}, need 16 more bytes, have {wkbBytes.Length - pos}");
                    return null;
                }
                
                // Try to parse as a single point with the current position
                Debug.WriteLine($"🔍 Attempting to parse coordinates starting at pos {pos}");
                return ParseWkbPoint(wkbBytes, ref pos, littleEndian);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 ParseWkbMultiPointWithPadding failed safely: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Parse WKB MultiPoint geometry safely with extensive validation and fallback logic
        /// </summary>
        private object ParseWkbMultiPointSafely(byte[] wkbBytes, ref int pos, bool littleEndian)
        {
            try
            {
                Debug.WriteLine($"🔍 MultiPointSafely: Starting parse at pos {pos}, total length {wkbBytes.Length}");
                
                // Safety bounds check
                if (wkbBytes == null || pos < 0 || pos + 4 > wkbBytes.Length)
                {
                    Debug.WriteLine($"🔍 MultiPointSafely: Invalid bounds - pos {pos}, array length {wkbBytes?.Length ?? 0}");
                    return null;
                }
                
                // Read point count with extra validation
                int originalPos = pos;
                uint numPoints = ReadUInt32(wkbBytes, ref pos, littleEndian);
                Debug.WriteLine($"🔍 MultiPointSafely: Raw point count value: {numPoints}");
                
                // CRITICAL: Detect corrupted point count data  
                if (numPoints == 0)
                {
                    Debug.WriteLine($"🔍 MultiPointSafely: Zero points - returning null");
                    return null;
                }
                
                // Check for obviously corrupted values (coordinates misread as point count)
                if (numPoints > 1000000 || (numPoints & 0xFF000000) != 0)
                {
                    Debug.WriteLine($"🔍 MultiPointSafely: Corrupted point count {numPoints} - this appears to be coordinate data!");
                    
                    // FALLBACK STRATEGY: Try interpreting as single Point 
                    Debug.WriteLine($"🔍 MultiPointSafely: Attempting Point fallback interpretation");
                    pos = originalPos - 4; // Reset to just after geometry type
                    
                    // Validate we have enough bytes for Point coordinates (16 bytes for x,y)
                    if (pos + 16 <= wkbBytes.Length)
                    {
                        Debug.WriteLine($"🔍 MultiPointSafely: Sufficient bytes for Point fallback - proceeding");
                        return ParseWkbPoint(wkbBytes, ref pos, littleEndian);
                    }
                    else
                    {
                        Debug.WriteLine($"🔍 MultiPointSafely: Insufficient bytes for Point fallback - giving up");
                        return null;
                    }
                }
                
                // Validate reasonable point count and byte requirements  
                int bytesNeededPerPoint = 21; // 1 byte order + 4 bytes type + 16 bytes coordinates
                int totalBytesNeeded = pos + (int)(numPoints * bytesNeededPerPoint);
                
                if (totalBytesNeeded > wkbBytes.Length)
                {
                    Debug.WriteLine($"🔍 MultiPointSafely: Not enough bytes for {numPoints} points - need {totalBytesNeeded}, have {wkbBytes.Length}");
                    return null;
                }
                
                // Process first point only for simplicity
                Debug.WriteLine($"🔍 MultiPointSafely: Processing first of {numPoints} valid points");
                
                // Check if we have enough bytes for the first point header (5 bytes)
                if (pos + 5 > wkbBytes.Length)
                {
                    Debug.WriteLine($"🔍 MultiPointSafely: Insufficient bytes for first point header");
                    return null;
                }
                
                // Skip byte order and geometry type for first point (5 bytes)
                pos += 5;
                return ParseWkbPoint(wkbBytes, ref pos, littleEndian);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 MultiPointSafely parsing failed safely: {ex.Message}");
                return null; // Safe fallback - never crash
            }
        }

        /// <summary>
        /// Read 32-bit unsigned integer from WKB bytes
        /// </summary>
        private uint ReadUInt32(byte[] bytes, ref int pos, bool littleEndian)
        {
            // Safety bounds checking to prevent ExecutionEngineException
            if (bytes == null)
            {
                Debug.WriteLine($"🔍 ReadUInt32: bytes array is null");
                throw new ArgumentNullException(nameof(bytes));
            }
            
            if (pos < 0 || pos + 4 > bytes.Length)
            {
                Debug.WriteLine($"🔍 ReadUInt32: Invalid position {pos}, array length {bytes.Length}");
                throw new ArgumentOutOfRangeException(nameof(pos), $"Position {pos} out of bounds for array length {bytes.Length}");
            }
            
            try
            {
                uint value;
                if (littleEndian)
                {
                    value = (uint)(bytes[pos] | (bytes[pos + 1] << 8) | (bytes[pos + 2] << 16) | (bytes[pos + 3] << 24));
                }
                else
                {
                    value = (uint)((bytes[pos] << 24) | (bytes[pos + 1] << 16) | (bytes[pos + 2] << 8) | bytes[pos + 3]);
                }
                
                Debug.WriteLine($"🔍 ReadUInt32 at pos {pos}: bytes=[{bytes[pos]:X2},{bytes[pos+1]:X2},{bytes[pos+2]:X2},{bytes[pos+3]:X2}], value={value}, littleEndian={littleEndian}");
                
                pos += 4;
                return value;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 ReadUInt32 exception at pos {pos}: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Read 64-bit double from WKB bytes
        /// </summary>
        private double ReadDouble(byte[] bytes, ref int pos, bool littleEndian)
        {
            // Critical safety bounds checking to prevent ExecutionEngineException
            if (bytes == null)
            {
                Debug.WriteLine($"🔍 ReadDouble: bytes array is null");
                throw new ArgumentNullException(nameof(bytes));
            }
            
            if (pos < 0 || pos + 8 > bytes.Length)
            {
                Debug.WriteLine($"🔍 ReadDouble: Invalid position {pos}, array length {bytes.Length}, need 8 bytes");
                throw new ArgumentOutOfRangeException(nameof(pos), $"Position {pos} out of bounds for array length {bytes.Length}");
            }
            
            try
            {
                // Enhanced safety: validate the bytes before processing to prevent memory corruption
                byte[] doubleBytes = new byte[8];
                
                // Use safer copy method with additional validation
                if (littleEndian)
                {
                    // Validate each byte access before copying to prevent memory issues
                    for (int i = 0; i < 8; i++)
                    {
                        if (pos + i >= bytes.Length)
                        {
                            throw new ArgumentOutOfRangeException($"Byte access out of bounds at position {pos + i}");
                        }
                        doubleBytes[i] = bytes[pos + i];
                    }
                }
                else
                {
                    // Big endian - reverse byte order with validation
                    for (int i = 0; i < 8; i++)
                    {
                        if (pos + 7 - i >= bytes.Length || pos + 7 - i < 0)
                        {
                            throw new ArgumentOutOfRangeException($"Byte access out of bounds at position {pos + 7 - i}");
                        }
                        doubleBytes[i] = bytes[pos + 7 - i];
                    }
                }
                
                // Additional safety: ensure BitConverter doesn't receive corrupted data
                if (doubleBytes.Length != 8)
                {
                    throw new InvalidOperationException($"Double bytes array has invalid length: {doubleBytes.Length}");
                }
                
                double value = BitConverter.ToDouble(doubleBytes, 0);
                
                // Validate the resulting double before proceeding
                if (double.IsNaN(value))
                {
                    Debug.WriteLine($"🔍 ReadDouble at pos {pos}: NaN value detected from bytes [{string.Join(",", doubleBytes.Select(b => $"{b:X2}"))}]");
                }
                else
                {
                    Debug.WriteLine($"🔍 ReadDouble at pos {pos}: value={value}, littleEndian={littleEndian}");
                }
                
                pos += 8;
                return value;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"🔍 ReadDouble exception at pos {pos}: {ex.Message}");
                Debug.WriteLine($"🔍 ReadDouble: Current bytes context: [{string.Join(",", bytes.Skip(Math.Max(0, pos - 4)).Take(16).Select(b => $"{b:X2}"))}]");
                throw;
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

            // Add theme-specific fields (including flattened struct members)
            foreach (var field in theme.Fields.Where(f => f != "id"))
            {
                if (string.Equals(field, "sources_property", StringComparison.OrdinalIgnoreCase)) continue;
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

            // Don't add geometry_wkb as a field - geometry is returned separately in ArcGIS REST API

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

            // If client accepts gzip, compress to reduce payload size drastically
            var acceptEncoding = context.Request.Headers["Accept-Encoding"] ?? string.Empty;
            if (acceptEncoding.IndexOf("gzip", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                response.Headers["Content-Encoding"] = "gzip";
                response.Headers["Vary"] = "Accept-Encoding";
                try
                {
                    using var gzip = new GZipStream(response.OutputStream, CompressionLevel.Fastest, leaveOpen: true);
                    var uncompressed = Encoding.UTF8.GetBytes(json);
                    await gzip.WriteAsync(uncompressed, 0, uncompressed.Length);
                    gzip.Flush();
                    response.OutputStream.Close();
                }
                catch (HttpListenerException)
                {
                    // client disconnected; ignore
                }
                catch (ObjectDisposedException)
                {
                    // output stream closed; ignore
                }
            }
            else
            {
                try
                {
            var buffer = Encoding.UTF8.GetBytes(json);
            await response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
            response.Close();
                }
                catch (HttpListenerException)
                {
                    // client disconnected; ignore
                }
                catch (ObjectDisposedException)
                {
                    // output stream closed; ignore
                }
            }
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