using System.IO;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Decides how DuckDB spatial/httpfs extensions are loaded. When both
    /// bundled files are present, network INSTALL is never attempted so the
    /// add-in works offline and behind locked-down proxies.
    /// </summary>
    public static class DuckDbExtensionPolicy
    {
        public const string SpatialFileName = "spatial.duckdb_extension";
        public const string HttpfsFileName = "httpfs.duckdb_extension";

        public static string SpatialPath(string extensionsPath) =>
            Path.Combine(extensionsPath ?? string.Empty, SpatialFileName);

        public static string HttpfsPath(string extensionsPath) =>
            Path.Combine(extensionsPath ?? string.Empty, HttpfsFileName);

        public static bool BundledExtensionsPresent(string extensionsPath) =>
            File.Exists(SpatialPath(extensionsPath)) && File.Exists(HttpfsPath(extensionsPath));

        /// <summary>
        /// Network INSTALL is only a fallback when the bundled files are missing.
        /// </summary>
        public static bool ShouldAttemptNetworkInstall(string extensionsPath) =>
            !BundledExtensionsPresent(extensionsPath);

        public static string BuildBundledLoadSql(string extensionsPath)
        {
            string spatial = SpatialPath(extensionsPath).Replace('\\', '/');
            string httpfs = HttpfsPath(extensionsPath).Replace('\\', '/');
            return $"LOAD '{spatial}';\nLOAD '{httpfs}';";
        }
    }
}
