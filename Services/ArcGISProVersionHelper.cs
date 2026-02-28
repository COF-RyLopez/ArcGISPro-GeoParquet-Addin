using System;
using System.Reflection;
using ArcGIS.Desktop.Core;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Helper class to detect ArcGIS Pro version and determine feature availability
    /// </summary>
    public static class ArcGISProVersionHelper
    {
        private static Version _proVersion;
        private static bool _versionDetected = false;

        /// <summary>
        /// Gets the ArcGIS Pro version
        /// </summary>
        public static Version ProVersion
        {
            get
            {
                if (!_versionDetected)
                {
                    DetectVersion();
                }
                return _proVersion;
            }
        }

        /// <summary>
        /// Checks if ArcGIS Pro 3.6 or later is installed
        /// </summary>
        public static bool IsPro36OrLater
        {
            get
            {
                var version = ProVersion;
                return version != null && (version.Major > 3 || (version.Major == 3 && version.Minor >= 6));
            }
        }

        /// <summary>
        /// Checks if ArcGIS Pro 3.5 is installed
        /// </summary>
        public static bool IsPro35
        {
            get
            {
                var version = ProVersion;
                return version != null && version.Major == 3 && version.Minor == 5;
            }
        }

        /// <summary>
        /// Gets a user-friendly version string
        /// </summary>
        public static string VersionString
        {
            get
            {
                var version = ProVersion;
                return version != null ? $"ArcGIS Pro {version.Major}.{version.Minor}" : "Unknown";
            }
        }

        private static void DetectVersion()
        {
            try
            {
                try
                {
                    var coreAssemblyPath = @"C:\Program Files\ArcGIS\Pro\bin\ArcGIS.Core.dll";
                    Assembly.LoadFrom(coreAssemblyPath);
                    if (TryParseVersionFromFile(coreAssemblyPath, "ProductVersion"))
                        return;
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error reading ProductVersion from ArcGIS.Core.dll: {ex.Message}");
                }

                try
                {
                    var frameworkAssembly = Assembly.GetAssembly(typeof(Project));
                    if (frameworkAssembly != null)
                    {
                        if (TryParseVersionFromFile(frameworkAssembly.Location, "Framework ProductVersion"))
                            return;
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error reading version from Framework assembly: {ex.Message}");
                }

                _proVersion = new Version(3, 5);
                _versionDetected = true;
                System.Diagnostics.Debug.WriteLine("Could not detect ArcGIS Pro version, defaulting to 3.5");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error detecting ArcGIS Pro version: {ex.Message}");
                _proVersion = new Version(3, 5);
                _versionDetected = true;
            }
        }

        private static bool TryParseVersionFromFile(string filePath, string debugSource)
        {
            if (!System.IO.File.Exists(filePath))
                return false;

            var fileVersionInfo = System.Diagnostics.FileVersionInfo.GetVersionInfo(filePath);
            if (fileVersionInfo == null || string.IsNullOrEmpty(fileVersionInfo.ProductVersion))
                return false;

            var productVersion = fileVersionInfo.ProductVersion;
            var versionParts = productVersion.Split('.');
            if (versionParts.Length < 2)
                return false;

            if (!int.TryParse(versionParts[0], out int major) || !int.TryParse(versionParts[1], out int minor))
                return false;

            // Esri internal versioning: 13.x = Pro 3.x, 14.x = Pro 4.x, etc.
            if (major >= 13 && major < 20)
                major -= 10;

            _proVersion = new Version(major, minor);
            _versionDetected = true;
            System.Diagnostics.Debug.WriteLine($"Detected ArcGIS Pro version from {debugSource}: {_proVersion} (ProductVersion: {productVersion}, mapped from internal version)");
            return true;
        }

        /// <summary>
        /// Resets version detection (useful for testing or if version changes)
        /// </summary>
        public static void ResetDetection()
        {
            _versionDetected = false;
            _proVersion = null;
        }
    }
}

