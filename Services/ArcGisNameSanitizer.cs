using System.Text;

namespace DuckDBGeoparquet.Services
{
    public static class ArcGisNameSanitizer
    {
        public static string ToFileGeodatabaseFeatureClassName(string value, string fallback = "GERSified_Output", int maxLength = 50)
        {
            string safeFallback = string.IsNullOrWhiteSpace(fallback) ? "Output" : fallback;
            int safeMaxLength = maxLength <= 0 ? 50 : maxLength;
            var builder = new StringBuilder();
            bool previousWasUnderscore = false;

            foreach (char ch in value ?? string.Empty)
            {
                if (char.IsLetterOrDigit(ch) || ch == '_')
                {
                    builder.Append(ch);
                    previousWasUnderscore = false;
                }
                else if (!previousWasUnderscore)
                {
                    builder.Append('_');
                    previousWasUnderscore = true;
                }
            }

            string sanitized = builder.ToString().Trim('_');
            if (string.IsNullOrWhiteSpace(sanitized))
            {
                sanitized = safeFallback;
            }

            if (!char.IsLetter(sanitized[0]))
            {
                sanitized = $"fc_{sanitized}";
            }

            return sanitized.Length > safeMaxLength
                ? sanitized[..safeMaxLength].TrimEnd('_')
                : sanitized;
        }
    }
}
