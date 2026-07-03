using System;
using System.Collections.Generic;
using System.Linq;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Builds progressively broader search queries for file-geocoding rows.
    /// This mirrors how a geocoder treats locality fields as helpful context
    /// rather than mandatory text that every dataset row must contain.
    /// </summary>
    public static class GeocodeFileQueryBuilder
    {
        public static IReadOnlyList<string> BuildQueryVariants(string address, string city, string state, string zip)
        {
            var variants = new List<string>();
            string cleanAddress = CleanPart(address);
            string cleanCity = CleanPart(city);
            string cleanState = CleanPart(state);
            string cleanZip = CleanPart(zip);

            if (string.IsNullOrWhiteSpace(cleanAddress))
            {
                return [];
            }

            string cityState = Join(", ", cleanCity, cleanState);
            string fullLocality = cityState;
            if (!string.IsNullOrWhiteSpace(cleanZip))
            {
                fullLocality = string.IsNullOrWhiteSpace(fullLocality)
                    ? cleanZip
                    : $"{fullLocality} {cleanZip}";
            }

            AddVariant(variants, cleanAddress, fullLocality);
            AddVariant(variants, cleanAddress, cleanZip);
            AddVariant(variants, cleanAddress, cleanCity);
            AddVariant(variants, cleanAddress, cityState);
            AddVariant(variants, cleanAddress, null);

            return variants;
        }

        private static void AddVariant(List<string> variants, string address, string locality)
        {
            string candidate = string.IsNullOrWhiteSpace(locality)
                ? address
                : $"{address}, {locality}";

            if (!variants.Contains(candidate, StringComparer.OrdinalIgnoreCase))
            {
                variants.Add(candidate);
            }
        }

        private static string Join(string separator, params string[] values)
        {
            return string.Join(separator, values.Where(value => !string.IsNullOrWhiteSpace(value)));
        }

        private static string CleanPart(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            return string.Join(" ",
                value.Trim().Trim('"')
                    .Split((char[])null, StringSplitOptions.RemoveEmptyEntries));
        }
    }
}
