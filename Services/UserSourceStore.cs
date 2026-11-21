using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Persistence and pure helpers for user-defined GeoParquet sources
    /// (the "Custom Sources" node in the wizard). The store is one plain JSON
    /// file under the add-in's config folder, so users can hand-edit, back up
    /// or share it directly — no import/export UI needed.
    ///
    /// No ArcGIS or DuckDB dependencies: everything here is unit-testable
    /// without ArcGIS Pro running (see DuckDBGeoparquet.Tests).
    /// </summary>
    public static class UserSourceStore
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = true
        };

        public static string GetStorePath(string baseDirectory) =>
            Path.Combine(baseDirectory, "config", "user_sources.json");

        /// <summary>Loads saved sources; a missing or corrupt file is an empty list.</summary>
        public static List<UserDataSource> Load(string path)
        {
            try
            {
                if (!File.Exists(path)) return new List<UserDataSource>();
                var sources = JsonSerializer.Deserialize<List<UserDataSource>>(File.ReadAllText(path), JsonOptions);
                return sources?.Where(s =>
                        !string.IsNullOrWhiteSpace(s?.Name) && !string.IsNullOrWhiteSpace(s.Url))
                    .ToList() ?? new List<UserDataSource>();
            }
            catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
            {
                System.Diagnostics.Debug.WriteLine($"UserSourceStore.Load: {ex.Message}");
                return new List<UserDataSource>();
            }
        }

        /// <summary>Atomically saves the full source list (temp file + move).</summary>
        public static void Save(string path, IReadOnlyList<UserDataSource> sources)
        {
            string directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
            string temp = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
            try
            {
                File.WriteAllText(temp, JsonSerializer.Serialize(sources, JsonOptions));
                File.Move(temp, path, overwrite: true);
            }
            finally
            {
                if (File.Exists(temp)) File.Delete(temp);
            }
        }

        /// <summary>DESCRIBE query that validates a URL and returns its columns.</summary>
        public static string BuildProbeQuery(string url) =>
            $"DESCRIBE SELECT * FROM read_parquet('{GeoParquetSql.EscapeSqlLiteral(url)}', union_by_name=1)";

        /// <summary>
        /// Picks the geometry column from a DESCRIBE result: a GEOMETRY-typed
        /// column first, then conventional names carrying WKB bytes.
        /// Returns (null, false) when the file has no usable geometry.
        /// </summary>
        public static (string GeometryColumn, bool IsWkb) PickGeometryColumn(
            IReadOnlyList<(string Name, string Type)> columns)
        {
            if (columns == null) return (null, false);

            foreach (var (name, type) in columns)
                if (string.Equals(type, "GEOMETRY", StringComparison.OrdinalIgnoreCase))
                    return (name, false);

            foreach (string candidate in new[] { "geometry", "geom", "wkb_geometry", "shape" })
            {
                foreach (var (name, type) in columns)
                {
                    if (!string.Equals(name, candidate, StringComparison.OrdinalIgnoreCase)) continue;
                    bool isWkb = type != null && type.ToUpperInvariant().Contains("BLOB");
                    return (name, isWkb);
                }
            }
            return (null, false);
        }

        /// <summary>
        /// Derives a filesystem/SQL-safe type key from a source's display name
        /// ("My Parcels (2026)" → "my_parcels_2026"). Used for output folder
        /// names, so it must be stable for a given name.
        /// </summary>
        public static string SanitizeTypeName(string name)
        {
            var builder = new StringBuilder();
            bool lastWasUnderscore = false;
            foreach (char ch in (name ?? string.Empty).Trim().ToLowerInvariant())
            {
                if (char.IsAscii(ch) && char.IsLetterOrDigit(ch))
                {
                    builder.Append(ch);
                    lastWasUnderscore = false;
                }
                else if (!lastWasUnderscore && builder.Length > 0)
                {
                    builder.Append('_');
                    lastWasUnderscore = true;
                }
            }
            string result = builder.ToString().TrimEnd('_');
            return result.Length == 0 ? "custom_source" : result;
        }

        /// <summary>
        /// Shared add-time validation (wizard panel and ribbon dialog):
        /// returns an error message, or null when the candidate is addable.
        /// </summary>
        public static string ValidateNewSource(string name, string url, IEnumerable<UserDataSource> existing)
        {
            name = name?.Trim();
            url = url?.Trim();
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(url))
                return "Enter a name and a URL.";
            if (!IsSupportedUrl(url))
                return "Enter an http(s):// or s3:// URL pointing at GeoParquet data.";
            string typeKey = SanitizeTypeName(name);
            if (existing != null && existing.Any(s =>
                    s.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                    SanitizeTypeName(s.Name) == typeKey))
                return $"A custom source named '{name}' already exists.";
            return null;
        }

        /// <summary>Basic shape check before probing: http(s) or s3 URL.</summary>
        public static bool IsSupportedUrl(string url)
        {
            if (string.IsNullOrWhiteSpace(url)) return false;
            string trimmed = url.Trim();
            return trimmed.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
                   trimmed.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
                   trimmed.StartsWith("s3://", StringComparison.OrdinalIgnoreCase);
        }
    }
}
