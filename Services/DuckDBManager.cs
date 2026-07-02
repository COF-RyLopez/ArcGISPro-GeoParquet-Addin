using DuckDB.NET.Data;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Owns the DuckDB connection lifecycle for the add-in: opening the
    /// in-memory database, loading the spatial/httpfs extensions (bundled
    /// first, network INSTALL as fallback), and applying session settings.
    /// Extracted from DataProcessor (Phase 2c stage 2).
    /// </summary>
    public sealed class DuckDBManager : IDisposable
    {
        private readonly DuckDBConnection _connection = new("DataSource=:memory:");
        private bool _isDisposed;

        /// <summary>The open connection. Valid after <see cref="InitializeAsync"/> succeeds.</summary>
        public DuckDBConnection Connection => _connection;

        /// <summary>True once extensions are loaded and the connection is usable.</summary>
        public bool IsInitialized { get; private set; }

        public async Task InitializeAsync(CancellationToken cancellationToken = default)
        {
            if (IsInitialized) return;

            try
            {
                await _connection.OpenAsync();

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
                    string spatialBundled = Path.Combine(extensionsPath, "spatial.duckdb_extension");
                    string httpfsBundled = Path.Combine(extensionsPath, "httpfs.duckdb_extension");

                    if (File.Exists(spatialBundled) && File.Exists(httpfsBundled))
                    {
                        command.CommandText = $@"
                            LOAD '{spatialBundled.Replace('\\', '/')}';
                            LOAD '{httpfsBundled.Replace('\\', '/')}';
                        ";
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
                            catch (Exception ex)
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
                catch (Exception ex)
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

                // Configure DuckDB settings for optimal performance.
                // Batched into a single multi-statement execution to reduce round trips.
                try
                {
                    int threads = Math.Max(1, Environment.ProcessorCount);
                    string tempDir = Path.GetTempPath().Replace('\\', '/');
                    command.CommandText = $@"
                        SET s3_region='us-west-2';
                        SET enable_http_metadata_cache=true;
                        SET enable_object_cache=true;
                        SET enable_progress_bar=true;
                        SET memory_limit='2GB';
                        SET max_memory='4GB';
                        SET temp_directory='{tempDir}';
                        SET threads={threads};
                    ";
                    await command.ExecuteNonQueryAsync(cancellationToken);
                    System.Diagnostics.Debug.WriteLine($"DuckDB settings configured successfully (threads={threads})");
                }
                catch (Exception settingsEx)
                {
                    System.Diagnostics.Debug.WriteLine($"Warning: Could not set some DuckDB settings: {settingsEx.Message}");
                    // Continue — these are optimizations, not required for functionality
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to initialize DuckDB: {ex.Message}", ex);
            }
        }

        public void Dispose()
        {
            if (_isDisposed) return;
            _connection?.Dispose();
            _isDisposed = true;
        }
    }
}
