using System;
using System.IO;
using System.Linq;
using ArcGIS.Desktop.Core;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Manages ArcGIS Pro Parquet cache location and operations
    /// </summary>
    public static class CacheManager
    {
        /// <summary>
        /// Gets the cache directory path for ArcGIS Pro Parquet files
        /// </summary>
        public static string GetCacheDirectory()
        {
            try
            {
                // ArcGIS Pro 3.6 changed cache location
                // Pro 3.5: %LOCALAPPDATA%\Esri\ArcGISPro\Cache\Parquet
                // Pro 3.6: %LOCALAPPDATA%\Esri\ArcGISPro\Cache\Parquet (same location, but structure may differ)
                
                var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
                var cachePath = Path.Combine(localAppData, "Esri", "ArcGISPro", "Cache", "Parquet");
                
                // Check if Pro 3.6+ has a different structure
                if (ArcGISProVersionHelper.IsPro36OrLater)
                {
                    // Pro 3.6 may use a subdirectory structure
                    // Try the standard location first
                    if (Directory.Exists(cachePath))
                    {
                        return cachePath;
                    }
                    
                    // Try alternative 3.6 location if it exists
                    var altPath = Path.Combine(localAppData, "Esri", "ArcGISPro", "Cache", "GeoParquet");
                    if (Directory.Exists(altPath))
                    {
                        return altPath;
                    }
                }
                
                return cachePath;
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
                var cacheDir = GetCacheDirectory();
                if (string.IsNullOrEmpty(cacheDir) || !Directory.Exists(cacheDir))
                {
                    return 0;
                }

                return GetDirectorySize(cacheDir);
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
                var cacheDir = GetCacheDirectory();
                if (string.IsNullOrEmpty(cacheDir) || !Directory.Exists(cacheDir))
                {
                    return false;
                }

                // Delete all files and subdirectories
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
                        System.Diagnostics.Debug.WriteLine($"Error deleting cache file {file}: {ex.Message}");
                    }
                }

                // Delete directories in reverse order (deepest first)
                foreach (var dir in dirs.OrderByDescending(d => d.Length))
                {
                    try
                    {
                        Directory.Delete(dir);
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"Error deleting cache directory {dir}: {ex.Message}");
                    }
                }

                return true;
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
                var cacheDir = GetCacheDirectory();
                if (string.IsNullOrEmpty(cacheDir) || !Directory.Exists(cacheDir))
                {
                    return 0;
                }

                return Directory.GetFiles(cacheDir, "*", SearchOption.AllDirectories).Length;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error counting cache files: {ex.Message}");
                return 0;
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

