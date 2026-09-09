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
            foreach (var asset in assets) ValidateAsset(asset);
            return assets.Where(a => Overlaps(a.Bounds, area)).Select(a => a.Url).Distinct().ToList();
        }

        public static CatalogAsset ParseAsset(JsonElement item)
        {
            var bounds = item.GetProperty("bbox").EnumerateArray().Select(v => v.GetDouble()).ToArray();
            if (bounds.Length != 4) throw new InvalidDataException("Unsupported catalog bounds; use the full source scan.");
            string url = item.GetProperty("assets").GetProperty("aws").GetProperty("href").GetString();
            var asset = new CatalogAsset(url, new ExtentBounds(bounds[0], bounds[1], bounds[2], bounds[3]));
            ValidateAsset(asset);
            return asset;
        }

        private static void ValidateAsset(CatalogAsset asset)
        {
            GeographicBounds.Validate(asset.Bounds);
            if (!Uri.TryCreate(asset.Url, UriKind.Absolute, out var uri) || uri.Scheme != "https" ||
                uri.Host != "overturemaps-us-west-2.s3.us-west-2.amazonaws.com")
                throw new InvalidDataException("Unexpected Overture asset URL.");
        }

        private static bool Overlaps(ExtentBounds file, ExtentBounds area) =>
            file.XMin <= area.XMax && file.XMax >= area.XMin && file.YMin <= area.YMax && file.YMax >= area.YMin;
    }
}
