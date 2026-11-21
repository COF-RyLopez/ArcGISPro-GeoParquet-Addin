using DuckDB.NET.Data;
using DuckDBGeoparquet.Models;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Owns the DuckDB connection lifecycle for the add-in: opening the
    /// in-memory database, loading the spatial/httpfs extensions (bundled
    /// first, network INSTALL only when bundled files are absent), applying
    /// httpfs proxy settings, and applying session settings.
    /// Extracted from DataProcessor (Phase 2c stage 2).
    /// </summary>
    public sealed class DuckDBManager : IDisposable
    {
        private readonly DuckDBConnection _connection = new("DataSource=:memory:");
        private bool _isDisposed;
        private readonly DuckDBJobQueue _jobs = new();

        /// <summary>The open connection. Valid after <see cref="InitializeAsync"/> succeeds.</summary>
        public DuckDBConnection Connection => _connection;

        /// <summary>True once extensions are loaded and the connection is usable.</summary>
        public bool IsInitialized { get; private set; }

        /// <summary>Raw DuckDB <c>version()</c> string, e.g. "v1.5.3", when known.</summary>
        public string LibraryVersion { get; private set; }

        /// <summary>Parsed DuckDB library version, or null if it could not be read.</summary>
        public Version ParsedLibraryVersion { get; private set; }


        public Task InitializeAsync(CancellationToken cancellationToken = default)
            => RunAsync(() => InitializeCoreAsync(cancellationToken), cancellationToken);

        public Task RunAsync(Func<Task> work, CancellationToken token = default)
            => _jobs.RunAsync(async () => { await work().ConfigureAwait(false); return true; }, Interrupt, token);

        public Task<T> RunAsync<T>(Func<Task<T>> work, CancellationToken token = default)
            => _jobs.RunAsync(work, Interrupt, token);

        private void Interrupt()
        {
            if (_connection.State != System.Data.ConnectionState.Open) return;
            using var command = _connection.CreateCommand();
            command.Cancel();
        }

        private async Task InitializeCoreAsync(CancellationToken cancellationToken)

        {
            if (IsInitialized) return;

            try
            {
                if (_connection.State != System.Data.ConnectionState.Open)
                    await _connection.OpenAsync();

                await CaptureLibraryVersionAsync(cancellationToken);

                var settings = AddinSettingsStore.Load();
                var proxy = ResolveProxy(settings);
                await ApplyProxySettingsAsync(proxy, cancellationToken);

                // Get the various potential paths where extensions might be found
                string addInFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                string extensionsPath = Path.Combine(addInFolder, "Extensions");

                using var command = _connection.CreateCommand();

                try
                {
                    // Prefer bundled extensions when present. They load from the
                    // add-in's own (trusted) install folder, so they work offline
                    // and on machines where Application Control policies block
                    // DLLs that DuckDB downloads to %USERPROFILE%\.duckdb at
                    // runtime. LOAD takes the explicit file path — setting
                    // extension_directory does not work here because DuckDB then
                    // expects a v<version>/<platform> subfolder structure.
                    // When both bundled files are present, never attempt a network INSTALL.
                    if (DuckDbExtensionPolicy.BundledExtensionsPresent(extensionsPath))
                    {
                        command.CommandText = DuckDbExtensionPolicy.BuildBundledLoadSql(extensionsPath);
                        await command.ExecuteNonQueryAsync(cancellationToken);
                        System.Diagnostics.Debug.WriteLine($"Loaded bundled DuckDB extensions from {extensionsPath}");
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine($"No bundled extensions at {extensionsPath}; trying network INSTALL...");

                        command.CommandText = @"
                            INSTALL spatial;
                            INSTALL httpfs;
                            LOAD spatial;
                            LOAD httpfs;
                        ";

                        bool installSuccess = false;
                        int maxRetries = 3;
                        for (int i = 0; i < maxRetries; i++)
                        {
                            try
                            {
                                await command.ExecuteNonQueryAsync(cancellationToken);
                                System.Diagnostics.Debug.WriteLine($"Successfully installed extensions directly on attempt {i + 1}");
                                installSuccess = true;
                                break; // Success, exit retry loop
                            }
                            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                            {
                                System.Diagnostics.Debug.WriteLine($"Direct installation attempt {i + 1} failed: {ex.Message}");
                                if (i < maxRetries - 1)
                                {
                                    await Task.Delay(2000, cancellationToken); // Wait 2 seconds before retrying
                                }
                            }
                        }

                        if (!installSuccess)
                        {
                            throw new Exception(
                                $"Runtime installation of DuckDB extensions failed and no bundled extensions were found at {extensionsPath}. " +
                                "If the error mentions an Application Control policy, your organization blocks DLLs downloaded to the user profile — " +
                                "bundle the extensions with the add-in instead (see Extensions/README.txt).");
                        }
                    }
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    // If both methods failed, show a more informative error with detailed instructions
                    throw new Exception(
                        $"Failed to load DuckDB extensions: {ex.Message}\n\n" +
                        "To resolve this issue:\n" +
                        "1. Place spatial.duckdb_extension and httpfs.duckdb_extension in the add-in's Extensions folder\n" +
                        "2. The extensions must match the DuckDB version used by the add-in (see Extensions/README.txt)\n" +
                        $"3. Current extension search path: {extensionsPath}", ex);
                }

                // Extensions loaded successfully — mark as initialized so a retry
                // after a settings failure doesn't re-open the connection.
                IsInitialized = true;

                await ApplySessionSettingsAsync(settings, proxy, cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                throw new Exception($"Failed to initialize DuckDB: {ex.Message}", ex);
            }
        }

        private async Task CaptureLibraryVersionAsync(CancellationToken cancellationToken)
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT version();";
                var result = await command.ExecuteScalarAsync(cancellationToken);
                LibraryVersion = result?.ToString();
                if (GeoParquetSql.TryParseDuckDbVersion(LibraryVersion, out var parsed))
                    ParsedLibraryVersion = parsed;
                System.Diagnostics.Debug.WriteLine($"DuckDB version: {LibraryVersion}");
            }
            catch (Exception versionEx)
            {
                System.Diagnostics.Debug.WriteLine($"Could not read DuckDB version: {versionEx.Message}");
            }
        }

        /// <summary>
        /// Re-applies httpfs proxy and memory settings after the user changes add-in settings.
        /// </summary>
        public Task ApplySessionSettingsAsync(CancellationToken cancellationToken = default)
        {
            if (!IsInitialized)
                return Task.CompletedTask;

            var settings = AddinSettingsStore.Load();
            var proxy = ResolveProxy(settings);
            return ApplySessionSettingsAsync(settings, proxy, cancellationToken);
        }

        private async Task ApplySessionSettingsAsync(
            AddinSettings settings,
            HttpProxySettings proxy,
            CancellationToken cancellationToken)
        {
            try
            {
                int threads = Math.Max(1, Environment.ProcessorCount);
                string tempDir = Path.GetTempPath().Replace('\\', '/');
                using var command = _connection.CreateCommand();
                command.CommandText = HttpProxySettingsBuilder.BuildSessionSettingsSql(
                    settings, proxy, threads, tempDir);
                await command.ExecuteNonQueryAsync(cancellationToken);
                System.Diagnostics.Debug.WriteLine(
                    $"DuckDB settings configured successfully (threads={threads}, {proxy.ToLogSafeDescription()}, memory={settings.EffectiveMemoryLimit}/{settings.EffectiveMaxMemory})");
            }
            catch (Exception settingsEx)
            {
                System.Diagnostics.Debug.WriteLine($"Warning: Could not set some DuckDB settings: {proxy.RedactSecrets(settingsEx.Message)}");
                // Continue — these are optimizations, not required for functionality
            }
        }

        private async Task ApplyProxySettingsAsync(HttpProxySettings proxy, CancellationToken cancellationToken)
        {
            string sql = HttpProxySettingsBuilder.BuildDuckDbSetSql(proxy);
            if (string.IsNullOrEmpty(sql))
            {
                System.Diagnostics.Debug.WriteLine("DuckDB httpfs proxy: none configured");
                return;
            }

            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = sql;
                await command.ExecuteNonQueryAsync(cancellationToken);
                System.Diagnostics.Debug.WriteLine($"DuckDB httpfs proxy applied: {proxy.ToLogSafeDescription()}");
            }
            catch (Exception ex)
            {
                // http_proxy may require httpfs to be loaded; session settings retry after LOAD.
                System.Diagnostics.Debug.WriteLine($"DuckDB httpfs proxy not applied yet ({proxy.ToLogSafeDescription()}): {proxy.RedactSecrets(ex.Message)}");
            }
        }

        internal static HttpProxySettings ResolveProxy(AddinSettings settings)
        {
            HttpProxySettings detected = null;
            try
            {
                detected = SystemProxyDetector.DetectForS3();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"System proxy detection failed: {ex.Message}");
            }

            return HttpProxySettingsBuilder.Resolve(settings, detected);
        }

        public void Dispose()
        {
            if (_isDisposed) return;
            _jobs.DisposeAfterJobs(() => _connection.Dispose());
            _isDisposed = true;
        }
    }
}
