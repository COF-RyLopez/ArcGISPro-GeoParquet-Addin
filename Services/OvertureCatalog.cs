using DuckDBGeoparquet.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public sealed record CatalogAsset(string Url, ExtentBounds Bounds);

    /// <summary>Cache a complete release/type manifest, then prune files before opening Parquet footers.</summary>
    public sealed class OvertureCatalog
    {
        private readonly Func<Uri, CancellationToken, Task<string>> _fetch;
        public OvertureCatalog(Func<Uri, CancellationToken, Task<string>> fetch = null)
            => _fetch = fetch ?? OvertureHttp.GetStringAsync;

        public async Task<IReadOnlyList<string>> FindFilesAsync(string release, string theme, string type,
            ExtentBounds area, string cacheDirectory, CancellationToken token = default)
        {
            GeographicBounds.Validate(area);
            foreach (string component in new[] { release, theme, type })
                if (!Regex.IsMatch(component ?? "", @"^[a-zA-Z0-9_.-]+$") || component.Contains(".."))
                    throw new ArgumentException("Invalid catalog identifier.");
            string cache = Path.Combine(cacheDirectory, $"{release}_{theme}_{type}.json");
            List<CatalogAsset> assets = null;
            if (File.Exists(cache))
            {
                try { assets = JsonSerializer.Deserialize<List<CatalogAsset>>(await File.ReadAllTextAsync(cache, token)); }
                catch (JsonException) { /* A corrupt cache is rebuilt from the catalog. */ }
            }
            if (assets == null)
            {
                var collectionUri = new Uri($"https://stac.overturemaps.org/{release}/{theme}/{type}/collection.json");
                using var collection = JsonDocument.Parse(await _fetch(collectionUri, token));
                var items = collection.RootElement.GetProperty("links").EnumerateArray()
                    .Where(l => l.GetProperty("rel").GetString() == "item")
                    .Select(l => new Uri(collectionUri, l.GetProperty("href").GetString())).Distinct().ToList();
                if (items.Count == 0) throw new InvalidDataException("The catalog has no file inventory for this dataset.");
                var found = new ConcurrentBag<CatalogAsset>();
                await Parallel.ForEachAsync(items, new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = token }, async (uri, ct) =>
                {
                    if (uri.Host != collectionUri.Host || uri.Scheme != "https")
                        throw new InvalidDataException("Unexpected catalog item location.");
                    using var item = JsonDocument.Parse(await _fetch(uri, ct));
                    found.Add(ParseAsset(item.RootElement));
                });
                assets = found.OrderBy(a => a.Url, StringComparer.Ordinal).ToList();
                Directory.CreateDirectory(cacheDirectory);
                string temp = cache + "." + Guid.NewGuid().ToString("N") + ".tmp";
                try
                {
                    await File.WriteAllTextAsync(temp, JsonSerializer.Serialize(assets), token);
                    token.ThrowIfCancellationRequested();
                    File.Move(temp, cache, true);
                }
                finally { if (File.Exists(temp)) File.Delete(temp); }
            }
            // Reapply normalization to older cached inventories as well as freshly fetched items.
            assets = assets.Select(NormalizeAsset).ToList();
            return assets.Where(a => Overlaps(a.Bounds, area)).Select(a => a.Url).Distinct().ToList();
        }

        public static CatalogAsset ParseAsset(JsonElement item)
        {
            var bounds = ReadBounds(item);
            string url = item.GetProperty("assets").GetProperty("aws").GetProperty("href").GetString();
            return NormalizeAsset(new CatalogAsset(url, bounds));
        }

        private static ExtentBounds ReadBounds(JsonElement item)
        {
            if (!item.TryGetProperty("bbox", out var bbox) || bbox.ValueKind != JsonValueKind.Array) return null;
            var values = bbox.EnumerateArray().ToArray();
            if (values.Length != 4 && values.Length != 6) return null;
            var coordinates = new double[values.Length];
            for (int i = 0; i < values.Length; i++)
                if (values[i].ValueKind != JsonValueKind.Number || !values[i].TryGetDouble(out coordinates[i])) return null;
            int upper = values.Length / 2;
            return new ExtentBounds(coordinates[0], coordinates[1], coordinates[upper], coordinates[upper + 1]);
        }

        private static CatalogAsset NormalizeAsset(CatalogAsset asset)
        {
            if (!Uri.TryCreate(asset?.Url, UriKind.Absolute, out var uri) || uri.Scheme != "https" ||
                uri.Host != "overturemaps-us-west-2.s3.us-west-2.amazonaws.com")
                throw new InvalidDataException("Unexpected Overture asset URL.");

            // File bounds are pruning hints, not the user's requested area. In particular,
            // some land-cover items extend slightly beyond +/-180. Do not discard the file
            // or blame the user's extent: cover the entire uncertain axis and let DuckDB
            // apply the row-level spatial filter. Never clamp or rewrite source geometry.
            var bounds = asset.Bounds;
            bool longitudeKnown = bounds != null && double.IsFinite(bounds.XMin) && double.IsFinite(bounds.XMax)
                && Math.Abs(bounds.XMin) <= 180 && Math.Abs(bounds.XMax) <= 180;
            bool latitudeKnown = bounds != null && double.IsFinite(bounds.YMin) && double.IsFinite(bounds.YMax)
                && Math.Abs(bounds.YMin) <= 90 && Math.Abs(bounds.YMax) <= 90 && bounds.YMin <= bounds.YMax;
            return asset with { Bounds = new ExtentBounds(
                longitudeKnown ? bounds.XMin : -180, latitudeKnown ? bounds.YMin : -90,
                longitudeKnown ? bounds.XMax : 180, latitudeKnown ? bounds.YMax : 90) };
        }

        private static bool Overlaps(ExtentBounds file, ExtentBounds area)
        {
            if (file.YMin > area.YMax || file.YMax < area.YMin) return false;
            // RFC 7946 permits west > east for a bounding box crossing the antimeridian.
            return file.XMin <= file.XMax
                ? file.XMin <= area.XMax && file.XMax >= area.XMin
                : area.XMax >= file.XMin || area.XMin <= file.XMax;
        }
    }
}
