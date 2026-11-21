using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace DuckDBGeoparquet.Services
{
    public sealed record GeocodeDecision(string Query, string Status, string Reason, GeocodeCandidate Match,
        IReadOnlyList<GeocodeCandidate> Candidates);

    /// <summary>Suggestions may be fuzzy; an unattended address assignment must be unambiguous.</summary>
    public static class GeocodeAcceptance
    {
        public static GeocodeDecision Evaluate(string query, IReadOnlyList<GeocodeCandidate> candidates)
        {
            string normalized = GeocodeTextNormalizer.NormalizeForSearch(query);
            string number = HouseNumber(query);
            var compatible = candidates.Where(c =>
            {
                if (!double.IsFinite(c.Longitude) || !double.IsFinite(c.Latitude) ||
                    Math.Abs(c.Longitude) > 180 || Math.Abs(c.Latitude) > 90 ||
                    number.Length == 0 || !number.Equals(HouseNumber(c.DisplayLabel), StringComparison.OrdinalIgnoreCase)) return false;
                string label = GeocodeTextNormalizer.NormalizeForSearch(c.DisplayLabel);
                return label == normalized || label.StartsWith(normalized + " ", StringComparison.Ordinal);
            }).ToList();
            if (compatible.Count == 1)
                return new(query, "matched", "One candidate preserves the complete supplied address.", compatible[0], candidates);
            return new(query, candidates.Count == 0 ? "unmatched" : "review",
                compatible.Count > 1 ? "Multiple compatible locations; choose a candidate." : "No unique full-address match; review suggestions.", null, candidates);
        }

        private static string HouseNumber(string value) => Regex.Match(value ?? "",
            @"^\s*(\d+(?:[a-z]|\s+\d+/\d+|-\d+)?)(?=\s|$)", RegexOptions.IgnoreCase).Groups[1].Value.Trim();
    }
}
