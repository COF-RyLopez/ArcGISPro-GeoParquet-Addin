using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Shared text normalization for address/place search input so manual
    /// searches and file-geocode queries behave consistently.
    /// </summary>
    public static class GeocodeTextNormalizer
    {
        public static string NormalizeForSearch(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            var normalized = new StringBuilder(value.Length);
            bool previousWasSpace = true;

            foreach (char ch in value.Trim().ToLowerInvariant())
            {
                if (char.IsLetterOrDigit(ch))
                {
                    normalized.Append(ch);
                    previousWasSpace = false;
                }
                else if (!previousWasSpace)
                {
                    normalized.Append(' ');
                    previousWasSpace = true;
                }
            }

            return normalized.ToString().Trim();
        }

        public static IReadOnlyList<string> Tokenize(string value)
        {
            string normalized = NormalizeForSearch(value);
            if (string.IsNullOrWhiteSpace(normalized))
            {
                return [];
            }

            return normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
    }
}
