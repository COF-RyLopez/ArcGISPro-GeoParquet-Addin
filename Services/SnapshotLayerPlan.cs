using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace DuckDBGeoparquet.Services
{
    /// <summary>Identify prior display files by manifest and dataset, never by a user-editable layer name.</summary>
    public static class SnapshotLayerPlan
    {
        public static HashSet<string> GetSupersededFiles(string currentFolder)
        {
            currentFolder = Path.GetFullPath(currentFolder);
            var current = JsonSerializer.Deserialize<SnapshotManifest>(File.ReadAllText(Path.Combine(currentFolder, DownloadSnapshot.ManifestName)));
            var types = current.Datasets.Select(d => (d.Theme, d.Type)).ToHashSet();
            var files = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string root = Path.GetDirectoryName(currentFolder);
            // Default output is Data/<release>/<snapshot>; custom roots may omit the release level.
            if (Path.GetFileName(root) == current.Release) root = Path.GetDirectoryName(root);
            var folders = Directory.EnumerateDirectories(root).ToList();
            foreach (string releaseFolder in folders.Where(f => Regex.IsMatch(Path.GetFileName(f), @"^\d{4}-\d{2}-\d{2}\.\d+$")).ToList())
            {
                folders.AddRange(Directory.EnumerateDirectories(releaseFolder));
                // Legacy releases predate manifests and store generated layers in per-type folders.
                foreach (var type in types.Select(t => t.Type).Where(t => !string.IsNullOrEmpty(t) && Path.GetFileName(t) == t))
                {
                    string typeFolder = Path.Combine(releaseFolder, type);
                    if (Directory.Exists(typeFolder))
                        foreach (string file in Directory.EnumerateFiles(typeFolder, "*.parquet")) files.Add(Path.GetFullPath(file));
                }
            }
            foreach (string folder in folders)
            {
                if (string.Equals(folder, currentFolder, StringComparison.OrdinalIgnoreCase)) continue;
                string manifestPath = Path.Combine(folder, DownloadSnapshot.ManifestName);
                if (!File.Exists(manifestPath)) continue;
                try
                {
                    var manifest = JsonSerializer.Deserialize<SnapshotManifest>(File.ReadAllText(manifestPath));
                    foreach (string relative in manifest.Datasets.Where(d => types.Contains((d.Theme, d.Type))).SelectMany(d => d.DisplayFiles))
                    {
                        string file = Path.GetFullPath(Path.Combine(folder, relative));
                        if (file.StartsWith(folder + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)) files.Add(file);
                    }
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
                {
                    // An unreadable prior manifest does not authorize changing its map layers.
                }
            }
            return files;
        }
    }
}
