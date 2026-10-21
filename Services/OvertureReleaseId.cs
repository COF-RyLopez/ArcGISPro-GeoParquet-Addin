using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Parses and ranks Overture release folder names (<c>YYYY-MM-DD.N</c>).
    /// Pure string/JSON/XML helpers — no HTTP — so the test project can
    /// exercise discovery without ArcGIS or the network.
    /// </summary>
    public static class OvertureReleaseId
    {
        public const string PlaceholderLatest = "latest";

        private const string DateFormat = "yyyy-MM-dd";
        private const string S3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/";

        private static readonly Regex ExactReleaseRegex = new(
            @"^(?<date>\d{4}-\d{2}-\d{2})\.(?<rev>\d+)$",
            RegexOptions.CultureInvariant | RegexOptions.Compiled);

        private static readonly Regex EmbeddedReleaseRegex = new(
            @"\d{4}-\d{2}-\d{2}\.\d+",
            RegexOptions.CultureInvariant | RegexOptions.Compiled);

        public static bool IsPlaceholder(string value)
        {
            return !string.IsNullOrWhiteSpace(value) &&
                   value.Trim().Equals(PlaceholderLatest, StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsUsable(string value)
        {
            return TryNormalize(value, out _);
        }

        /// <summary>
        /// Strict parse of a release folder name. Accepts optional
        /// <c>release/</c> prefix and trailing slash; rejects
        /// <c>latest</c> and any other non-matching text.
        /// </summary>
        public static bool TryParse(string value, out DateTime date, out int revision)
        {
            date = default;
            revision = 0;
            if (!TryNormalize(value, out var normalized))
                return false;

            var match = ExactReleaseRegex.Match(normalized);
            if (!match.Success)
                return false;
            if (!DateTime.TryParseExact(
                    match.Groups["date"].Value,
                    DateFormat,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out date))
                return false;
            return int.TryParse(
                match.Groups["rev"].Value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out revision);
        }

        /// <summary>
        /// Extracts a canonical <c>YYYY-MM-DD.N</c> from a folder name,
        /// S3 prefix, STAC href, or title. Returns false for placeholders
        /// and unrelated prefixes such as <c>theme=buildings</c>.
        /// </summary>
        public static bool TryNormalize(string value, out string releaseId)
        {
            releaseId = null;
            if (string.IsNullOrWhiteSpace(value))
                return false;

            var trimmed = value.Trim();
            if (IsPlaceholder(trimmed))
                return false;

            trimmed = trimmed.Trim('/');
            if (trimmed.StartsWith("release/", StringComparison.OrdinalIgnoreCase))
                trimmed = trimmed.Substring("release/".Length).Trim('/');

            // Prefer a path segment that is exactly YYYY-MM-DD.N
            var segments = trimmed.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = segments.Length - 1; i >= 0; i--)
            {
                var segment = segments[i];
                if (ExactReleaseRegex.IsMatch(segment) && TryParseExact(segment, out _))
                {
                    releaseId = segment;
                    return true;
                }
            }

            var embedded = EmbeddedReleaseRegex.Match(trimmed);
            if (embedded.Success && TryParseExact(embedded.Value, out var extracted))
            {
                releaseId = extracted;
                return true;
            }

            return false;
        }

        public static int Compare(string left, string right)
        {
            bool leftOk = TryParse(left, out var leftDate, out var leftRev);
            bool rightOk = TryParse(right, out var rightDate, out var rightRev);
            if (leftOk && rightOk)
            {
                int dateCmp = leftDate.CompareTo(rightDate);
                return dateCmp != 0 ? dateCmp : leftRev.CompareTo(rightRev);
            }

            if (leftOk) return 1;
            if (rightOk) return -1;
            return string.Compare(left, right, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns unique normalized release IDs, newest first. Non-release
        /// prefixes are dropped.
        /// </summary>
        public static IReadOnlyList<string> NormalizeMany(IEnumerable<string> candidates)
        {
            var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var parsed = new List<(string Id, DateTime Date, int Revision)>();

            if (candidates == null)
                return parsed.ConvertAll(p => p.Id);

            foreach (var candidate in candidates)
            {
                if (!TryNormalize(candidate, out var id) || !unique.Add(id))
                    continue;
                if (!TryParse(id, out var date, out var revision))
                    continue;
                parsed.Add((id, date, revision));
            }

            return parsed
                .OrderByDescending(p => p.Date)
                .ThenByDescending(p => p.Revision)
                .Select(p => p.Id)
                .ToList();
        }

        public static string SelectNewest(IEnumerable<string> candidates)
        {
            var normalized = NormalizeMany(candidates);
            return normalized.Count == 0 ? null : normalized[0];
        }

        public static IReadOnlyList<string> ParseStacCatalog(string json)
        {
            var found = new List<string>();
            if (string.IsNullOrWhiteSpace(json))
                return found;

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("latest", out var latestEl) &&
                latestEl.ValueKind == JsonValueKind.String)
            {
                found.Add(latestEl.GetString());
            }

            if (root.TryGetProperty("links", out var links) &&
                links.ValueKind == JsonValueKind.Array)
            {
                foreach (var link in links.EnumerateArray())
                {
                    if (link.ValueKind != JsonValueKind.Object)
                        continue;
                    if (link.TryGetProperty("rel", out var relEl) &&
                        !string.Equals(relEl.GetString(), "child", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (link.TryGetProperty("href", out var hrefEl) &&
                        hrefEl.ValueKind == JsonValueKind.String)
                    {
                        found.Add(hrefEl.GetString());
                    }

                    if (link.TryGetProperty("title", out var titleEl) &&
                        titleEl.ValueKind == JsonValueKind.String)
                    {
                        found.Add(titleEl.GetString());
                    }
                }
            }

            return NormalizeMany(found);
        }

        public static IReadOnlyList<string> ParseS3ListXml(string xml)
        {
            var found = new List<string>();
            if (string.IsNullOrWhiteSpace(xml))
                return found;

            var doc = new XmlDocument();
            doc.LoadXml(xml);

            var nsmgr = new XmlNamespaceManager(doc.NameTable);
            nsmgr.AddNamespace("s3", S3Namespace);

            XmlNodeList nodes = doc.SelectNodes("//s3:CommonPrefixes/s3:Prefix", nsmgr);
            if (nodes == null || nodes.Count == 0)
                nodes = doc.SelectNodes("//*[local-name()='CommonPrefixes']/*[local-name()='Prefix']");
            if (nodes == null || nodes.Count == 0)
                nodes = doc.SelectNodes("//*[local-name()='Prefix']");

            if (nodes != null)
            {
                foreach (XmlNode node in nodes)
                {
                    if (!string.IsNullOrWhiteSpace(node.InnerText))
                        found.Add(node.InnerText);
                }
            }

            return NormalizeMany(found);
        }

        private static bool TryParseExact(string value, out string releaseId)
        {
            releaseId = null;
            var match = ExactReleaseRegex.Match(value);
            if (!match.Success)
                return false;
            if (!DateTime.TryParseExact(
                    match.Groups["date"].Value,
                    DateFormat,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out _))
                return false;
            if (!int.TryParse(
                    match.Groups["rev"].Value,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out _))
                return false;
            releaseId = value;
            return true;
        }
    }
}
