using System;
using System.IO;

namespace DuckDBGeoparquet.Services
{
    public static class GersifyOutputPaths
    {
        public static bool FeatureClassExists(string featureClassPath)
        {
            if (string.IsNullOrWhiteSpace(featureClassPath))
                return false;

            string normalized = Path.GetFullPath(featureClassPath);
            if (File.Exists(normalized))
                return true;

            string parentDirectory = Path.GetDirectoryName(normalized);
            return !string.IsNullOrWhiteSpace(parentDirectory) &&
                   parentDirectory.EndsWith(".gdb", StringComparison.OrdinalIgnoreCase) &&
                   Directory.Exists(parentDirectory);
        }
    }
}
