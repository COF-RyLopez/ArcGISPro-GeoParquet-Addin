using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Manages ArcGIS Pro Parquet cache location and operations
    /// </summary>
    public static class CacheManager
    {
        private static IReadOnlyList<string> GetKnownCacheDirectories()
        {
            var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            var documents = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

            var candidates = new[]
            {
                // ArcGIS Pro current docs (newer versions)
                Path.Combine(localAppData, "ESRI", "Local Caches", "ParquetCacheV1"),
                // ArcGIS Pro 3.5-era cache location used in this add-in previously
                Path.Combine(localAppData, "Esri", "ArcGISPro", "Cache", "Parquet"),
                // Alternate location seen in some builds
                Path.Combine(localAppData, "Esri", "ArcGISPro", "Cache", "GeoParquet"),
                // Older docs/examples
                Path.Combine(documents, "ArcGIS", "ParquetCache")
            };

            return candidates
                .Where(p => !string.IsNullOrWhiteSpace(p))
                .Select(p => Path.GetFullPath(p))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static IReadOnlyList<string> GetExistingCacheDirectories() =>
            GetKnownCacheDirectories().Where(Directory.Exists).ToList();

        private static long GetDirectorySizeSafe(string path)
        {
            try { return GetDirectorySize(path); } catch { return 0; }
        }

        private static int GetDirectoryFileCountSafe(string path)
        {
            try { return Directory.GetFiles(path, "*", SearchOption.AllDirectories).Length; } catch { return 0; }
        }

        /// <summary>
        /// Gets the cache directory path for ArcGIS Pro Parquet files
        /// </summary>
        public static string GetCacheDirectory()
        {
            try
            {
                var existing = GetExistingCacheDirectories();
                if (existing.Count > 0)
                {
                    // Prefer the cache that appears active (more files / larger size).
                    return existing
                        .OrderByDescending(GetDirectoryFileCountSafe)
                        .ThenByDescending(GetDirectorySizeSafe)
                        .First();
                }

                // If nothing exists yet, return the primary modern location.
                return GetKnownCacheDirectories().FirstOrDefault() ?? string.Empty;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error getting cache directory: {ex.Message}");
                return string.Empty;
            }
        }

        /// <summary>
        /// Gets the cache size in bytes
        /// </summary>
        public static long GetCacheSize()
        {
            try
            {
                var existingDirs = GetExistingCacheDirectories();
                if (existingDirs.Count == 0)
                {
                    return 0;
                }

                return existingDirs.Sum(GetDirectorySizeSafe);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error calculating cache size: {ex.Message}");
                return 0;
            }
        }

        /// <summary>
        /// Gets a human-readable cache size string
        /// </summary>
        public static string GetCacheSizeString()
        {
            var size = GetCacheSize();
            return FormatBytes(size);
        }

        /// <summary>
        /// Clears the Parquet cache
        /// </summary>
        public static bool ClearCache()
        {
            try
            {
                var existingDirs = GetExistingCacheDirectories();
                if (existingDirs.Count == 0)
                {
                    return false;
                }

                bool anySuccess = false;
                foreach (var cacheDir in existingDirs)
                {
                    anySuccess |= ClearCacheDirectory(cacheDir);
                }

                return anySuccess;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error clearing cache: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Gets the number of cache files
        /// </summary>
        public static int GetCacheFileCount()
        {
            try
            {
                var existingDirs = GetExistingCacheDirectories();
                if (existingDirs.Count == 0)
                {
                    return 0;
                }

                return existingDirs.Sum(GetDirectoryFileCountSafe);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error counting cache files: {ex.Message}");
                return 0;
            }
        }

        private static bool ClearCacheDirectory(string cacheDir)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(cacheDir) || !Directory.Exists(cacheDir))
                    return false;

                bool hadErrors = false;
                var files = Directory.GetFiles(cacheDir, "*", SearchOption.AllDirectories);
                var dirs = Directory.GetDirectories(cacheDir, "*", SearchOption.AllDirectories);

                foreach (var file in files)
                {
                    try
                    {
                        File.Delete(file);
                    }
                    catch (Exception ex)
                    {
                        hadErrors = true;
                        System.Diagnostics.Debug.WriteLine($"Error deleting cache file {file}: {ex.Message}");
                    }
                }

                foreach (var dir in dirs.OrderByDescending(d => d.Length))
                {
                    try
                    {
                        Directory.Delete(dir);
                    }
                    catch (Exception ex)
                    {
                        hadErrors = true;
                        System.Diagnostics.Debug.WriteLine($"Error deleting cache directory {dir}: {ex.Message}");
                    }
                }

                if (hadErrors)
                {
                    System.Diagnostics.Debug.WriteLine($"Cache clear completed with warnings for {cacheDir}");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"Cache clear completed for {cacheDir}");
                }

                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error clearing cache directory {cacheDir}: {ex.Message}");
                return false;
            }
        }

        private static long GetDirectorySize(string directory)
        {
            long size = 0;
            try
            {
                var files = Directory.GetFiles(directory, "*", SearchOption.AllDirectories);
                foreach (var file in files)
                {
                    try
                    {
                        var fileInfo = new FileInfo(file);
                        size += fileInfo.Length;
                    }
                    catch
                    {
                        // Skip files we can't access
                    }
                }
            }
            catch
            {
                // Return 0 if we can't calculate
            }
            return size;
        }

        private static string FormatBytes(long bytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB" };
            double len = bytes;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len = len / 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }
    }
}

