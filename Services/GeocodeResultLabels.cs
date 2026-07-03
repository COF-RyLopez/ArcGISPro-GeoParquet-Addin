using System;

namespace DuckDBGeoparquet.Services
{
    public static class GeocodeResultLabels
    {
        public static string GetMatchSummary(string matchTier, string sourceType)
        {
            bool isAddress = string.Equals(sourceType, "Address", StringComparison.OrdinalIgnoreCase);
            bool isPlace = string.Equals(sourceType, "Place", StringComparison.OrdinalIgnoreCase);

            return (matchTier ?? string.Empty).Trim().ToLowerInvariant() switch
            {
                "exact" => isPlace ? "Exact place match" : "Exact address match",
                "prefix" => isPlace ? "Strong place match" : "Strong address match",
                "contains" => isPlace ? "Partial place match" : "Partial address match",
                "token" => "Approximate text match",
                "locator" => "Matched by ArcGIS locator",
                _ => "Matched result"
            };
        }

        public static string GetConfidenceSummary(string confidenceTier)
        {
            return (confidenceTier ?? string.Empty).Trim().ToLowerInvariant() switch
            {
                "high" => "High confidence",
                "medium" => "Good candidate",
                "low" => "Needs review",
                _ => "Candidate"
            };
        }
    }
}
