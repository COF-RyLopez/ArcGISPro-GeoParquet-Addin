using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace DuckDBGeoparquet.Services
{
    public sealed record SnapshotManifest
    {
        public int FormatVersion { get; init; } = 1;
        public string Release { get; init; }
        public DateTime CreatedUtc { get; init; } = DateTime.UtcNow;
        public ExtentBounds Area { get; init; }
        public string GeometryPolicy { get; init; } = "whole_intersecting_features";
        public string EngineVersion { get; set; }
        public string Compression { get; init; }
        public List<DatasetLoadResult> Datasets { get; init; } = new();
    }

    /// <summary>A failed or cancelled download never replaces the previous working snapshot.</summary>
    public sealed class DownloadSnapshot : IDisposable
    {
        public const string ManifestName = "snapshot.json";
        public const string CurrentName = "current-snapshot.json";
        private readonly string _dataDirectory;
        private readonly string _id;
        private bool _committed;
        public string StagingPath { get; }
        public string FinalPath { get; }
        public SnapshotManifest Manifest { get; }

        public DownloadSnapshot(string dataDirectory, string release, ExtentBounds area, string compression, string providerTag = "Overture")
        {
            // Folder-name safety backstop; real tags come from SelectableThemeItem.ComputeProviderTag.
            providerTag = Regex.Replace(providerTag ?? "", "[^A-Za-z0-9]", "");
            if (providerTag.Length == 0) providerTag = "Overture";
            bool usesOverture = providerTag is "Overture" or "Mixed";
            if (usesOverture && !Regex.IsMatch(release ?? "", ReleasePattern))
                throw new ArgumentException("Choose a dated Overture release before downloading.", nameof(release));
            _dataDirectory = Path.GetFullPath(dataDirectory);
            string stamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
            string hash = Guid.NewGuid().ToString("N")[..8];
            _id = usesOverture ? $"{providerTag}_{release}_{stamp}_{hash}" : $"{providerTag}_{stamp}_{hash}";
            StagingPath = Path.Combine(_dataDirectory, ".staging", _id);
            FinalPath = Path.Combine(_dataDirectory, _id);
            Directory.CreateDirectory(StagingPath);
            Manifest = new SnapshotManifest { Release = usesOverture ? release : null, Area = area, Compression = compression };
        }

        private const string ReleasePattern = @"^\d{4}-\d{2}-\d{2}\.\d+$";

        /// <summary>
        /// Human name for a snapshot folder, e.g. "OSM — 2026-11-22 04:34" or
        /// "Overture 2026-11-18.0 — 2026-11-22 04:34" (UTC, same stamp as the
        /// folder). Pre-provider-tag folders ({release}_{stamp}_{hash}) read as
        /// Overture; unrecognized names return unchanged.
        /// </summary>
        public static string DescribeFolder(string folderName)
        {
            if (string.IsNullOrWhiteSpace(folderName)) return folderName;
            string[] parts = folderName.Split('_');
            int stampIndex = Array.FindIndex(parts, p => p.Length == 14 && p.All(char.IsDigit));
            if (stampIndex < 1 || !DateTime.TryParseExact(parts[stampIndex], "yyyyMMddHHmmss",
                    System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var stamp))
                return folderName;

            string label;
            if (Regex.IsMatch(parts[0], ReleasePattern))
                label = "Overture " + parts[0];
            else
            {
                label = parts[0] == "SourceCoop" ? "Source Coop" : parts[0];
                if (stampIndex > 1 && Regex.IsMatch(parts[1], ReleasePattern))
                    label += " " + parts[1];
            }
            return $"{label} — {stamp:yyyy-MM-dd HH:mm}";
        }

        public string Commit()
        {
            if (_committed) throw new InvalidOperationException("Snapshot already committed.");
            if (Manifest.Datasets.Count == 0 || Manifest.Datasets.Any(d => d.Status is DatasetLoadStatus.Failed or DatasetLoadStatus.Cancelled))
                throw new InvalidOperationException("All requested datasets must finish before adopting a snapshot.");
            foreach (var dataset in Manifest.Datasets.Where(d => d.Status == DatasetLoadStatus.Completed))
            {
                if (dataset.RowCount <= 0 || dataset.DisplayFiles.Count == 0 || string.IsNullOrWhiteSpace(dataset.SourceFile))
                    throw new InvalidDataException($"Incomplete output for {dataset.Type}.");
                foreach (var file in dataset.DisplayFiles.Append(dataset.SourceFile))
                {
                    var path = Path.GetFullPath(Path.Combine(StagingPath, file));
                    if (!path.StartsWith(StagingPath + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase) ||
                        !File.Exists(path) || new FileInfo(path).Length == 0)
                        throw new InvalidDataException($"Missing or invalid snapshot file: {file}");
                }
            }
            File.WriteAllText(Path.Combine(StagingPath, ManifestName), JsonSerializer.Serialize(Manifest, new JsonSerializerOptions { WriteIndented = true }));
            Directory.Move(StagingPath, FinalPath);
            // Same-directory rename publishes the pointer only after the complete snapshot exists.
            string pointerTemp = Path.Combine(_dataDirectory, _id + ".tmp");
            try
            {
                File.WriteAllText(pointerTemp, JsonSerializer.Serialize(_id));
                File.Move(pointerTemp, Path.Combine(_dataDirectory, CurrentName), true);
                _committed = true;
                return FinalPath;
            }
            finally { if (File.Exists(pointerTemp)) File.Delete(pointerTemp); }
        }

        public static string ResolveCurrent(string dataDirectory)
        {
            string pointer = Path.Combine(dataDirectory, CurrentName);
            if (!File.Exists(pointer)) return null;
            // Invalid pointers must not fall back to an older/random folder silently.
            string id = JsonSerializer.Deserialize<string>(File.ReadAllText(pointer));
            if (string.IsNullOrWhiteSpace(id) || Path.GetFileName(id) != id || id.Contains('/') || id.Contains('\\'))
                throw new InvalidDataException("Invalid current snapshot pointer.");
            string path = Path.Combine(dataDirectory, id);
            if (!File.Exists(Path.Combine(path, ManifestName)))
                throw new InvalidDataException("The current snapshot is missing. Restore it or load a new snapshot.");
            return path;
        }

        public static string ReadRelease(string folder)
        {
            string manifest = Path.Combine(folder, ManifestName);
            return File.Exists(manifest)
                ? JsonSerializer.Deserialize<SnapshotManifest>(File.ReadAllText(manifest)).Release
                : Path.GetFileName(folder.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        }

        public void Dispose()
        {
            if (!_committed && Directory.Exists(StagingPath))
            {
                try { Directory.Delete(StagingPath, true); }
                catch (IOException) { /* A locked staging file is never discoverable as active data. */ }
                catch (UnauthorizedAccessException) { }
            }
        }
    }
}
