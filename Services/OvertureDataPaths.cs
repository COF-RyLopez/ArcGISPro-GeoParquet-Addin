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

            string current = DownloadSnapshot.ResolveCurrent(dataDirectory);
            if (current != null) return current;

            // Select only committed snapshots or usable legacy data. Creating a staging
            // directory for a newer release must not change discovery after a failed job.
            return new DirectoryInfo(dataDirectory).GetDirectories()
                .Where(d => !d.Name.StartsWith(".") && !File.Exists(Path.Combine(d.FullName, DownloadSnapshot.ManifestName)))
                .Select(d => new { Directory = d, Current = DownloadSnapshot.ResolveCurrent(d.FullName) })
                .Where(d => d.Current != null || HasLegacyData(d.Directory.FullName))
                .OrderByDescending(d => d.Current == null ? d.Directory.LastWriteTimeUtc
                    : File.GetLastWriteTimeUtc(Path.Combine(d.Current, DownloadSnapshot.ManifestName)))
                .Select(d => d.Current ?? d.Directory.FullName)
                .FirstOrDefault();
        }
        private static bool HasLegacyData(string folder) => Directory.EnumerateDirectories(folder)
            .Where(d => !Path.GetFileName(d).StartsWith("."))
            .Any(d => Directory.EnumerateFiles(d, "*.parquet", SearchOption.TopDirectoryOnly).Any());

    }
}
