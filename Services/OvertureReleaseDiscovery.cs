using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DuckDBGeoparquet.Services
{
    public sealed record OvertureReleaseSelection(string Release, string Source);

    /// <summary>Discover published releases without treating a cached date as proof of availability.</summary>
    public sealed class OvertureReleaseDiscovery
    {
        public const string CatalogUrl = "https://stac.overturemaps.org/catalog.json";
        private const string BucketUrl = "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/";
        private readonly Func<Uri, CancellationToken, Task<string>> _fetch;
        public OvertureReleaseDiscovery(Func<Uri, CancellationToken, Task<string>> fetch = null)
            => _fetch = fetch ?? OvertureHttp.GetStringAsync;

        public async Task<OvertureReleaseSelection> GetLatestAsync(string catalogUrl = null, CancellationToken token = default)
        {
            Exception catalogError;
            try
            {
                using var document = JsonDocument.Parse(await _fetch(new Uri(catalogUrl ?? CatalogUrl), token));
                string release = document.RootElement.GetProperty("latest").GetString();
                ValidateRelease(release);
                return new(release, "Overture catalog");
            }
            catch (Exception ex) when (!token.IsCancellationRequested)
            {
                catalogError = ex;
            }

            try
            {
                var releases = new List<string>();
                var seenCursors = new HashSet<string>(StringComparer.Ordinal);
                string cursor = null;
                do
                {
                    string url = BucketUrl + "?list-type=2&prefix=release%2F&delimiter=%2F";
                    if (cursor != null) url += "&continuation-token=" + Uri.EscapeDataString(cursor);
                    var root = ParseListing(await _fetch(new Uri(url), token));
                    XNamespace ns = root.Name.Namespace;
                    foreach (var prefix in root.Elements(ns + "CommonPrefixes").Elements(ns + "Prefix"))
                    {
                        string value = prefix.Value;
                        if (!value.StartsWith("release/", StringComparison.Ordinal) || !value.EndsWith('/')) continue;
                        string candidate = value[8..^1];
                        if (IsValidRelease(candidate)) releases.Add(candidate);
                    }
                    bool truncated = string.Equals(root.Element(ns + "IsTruncated")?.Value, "true", StringComparison.OrdinalIgnoreCase);
                    cursor = truncated ? root.Element(ns + "NextContinuationToken")?.Value : null;
                    if (truncated && (string.IsNullOrWhiteSpace(cursor) || !seenCursors.Add(cursor)))
                        throw new InvalidDataException("The release inventory is incomplete.");
                } while (cursor != null);
                string latest = releases.OrderByDescending(r => r[..10], StringComparer.Ordinal)
                    .ThenByDescending(r => int.Parse(r[11..], CultureInfo.InvariantCulture)).FirstOrDefault();
                if (latest == null) throw new InvalidDataException("The public bucket lists no dated releases.");
                return new(latest, "public S3 release inventory");
            }
            catch (Exception ex) when (!token.IsCancellationRequested)
            {
                throw new InvalidOperationException(
                    $"Release discovery failed. Catalog: {catalogError.Message} S3 inventory: {ex.Message} " +
                    "Enter a published release date or retry when network access is restored.", ex);
            }
        }

        public async Task EnsureAvailableAsync(string release, CancellationToken token = default)
        {
            ValidateRelease(release);
            string prefix = $"release/{release}/theme=";
            var uri = new Uri(BucketUrl + "?list-type=2&prefix=" + Uri.EscapeDataString(prefix) + "&max-keys=1");
            XElement root;
            try { root = ParseListing(await _fetch(uri, token)); }
            catch (Exception ex) when (!token.IsCancellationRequested)
            {
                throw new InvalidOperationException($"Could not verify release {release} in public S3. Check network access and retry. {ex.Message}", ex);
            }
            XNamespace ns = root.Name.Namespace;
            if (!root.Elements(ns + "Contents").Elements(ns + "Key").Any(k => k.Value.StartsWith(prefix, StringComparison.Ordinal)))
                throw new InvalidOperationException($"Release {release} has no published dataset files in public S3. It may have been retired. Refresh the release or apply a currently published date before downloading.");
        }

        private static XElement ParseListing(string xml)
        {
            var root = XDocument.Parse(xml).Root;
            if (root?.Name.LocalName != "ListBucketResult") throw new InvalidDataException("Unexpected S3 inventory response.");
            return root;
        }

        public static void ValidateRelease(string release)
        {
            if (!IsValidRelease(release)) throw new ArgumentException("Enter a dated release such as YYYY-MM-DD.0; use a date listed in the Overture catalog.");
        }

        private static bool IsValidRelease(string release) =>
            Regex.IsMatch(release ?? "", @"^\d{4}-\d{2}-\d{2}\.\d+$") &&
            DateTime.TryParseExact(release[..10], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _) &&
            int.TryParse(release[11..], NumberStyles.None, CultureInfo.InvariantCulture, out _);
    }
}
