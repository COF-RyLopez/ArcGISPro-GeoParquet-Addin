using System.IO;
using System.Linq;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Pure filesystem helpers for locating the add-in's loaded data on disk.
    /// No ArcGIS dependencies (the project-root lookup lives in
    /// <see cref="ProjectDataLocator"/>), so this is unit-testable.
    /// </summary>
    public static class OvertureDataPaths
    {
        /// <summary>
        /// Returns the most recently modified immediate subfolder of
        /// <paramref name="dataDirectory"/> — i.e. the newest loaded Overture
        /// release folder, which is the one holding the per-type subfolders
        /// that MFC generation consumes. Returns null when the directory is
        /// missing or has no subfolders.
        /// </summary>
        public static string ResolveNewestReleaseFolder(string dataDirectory)
        {
            if (string.IsNullOrWhiteSpace(dataDirectory) || !Directory.Exists(dataDirectory))
                return null;

            return new DirectoryInfo(dataDirectory)
                .GetDirectories()
                .OrderByDescending(d => d.LastWriteTimeUtc)
                .Select(d => d.FullName)
                .FirstOrDefault();
        }
    }
}
