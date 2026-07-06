using System;
using System.Collections.Generic;
using System.Globalization;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// SQL fragments for map definition queries against GERSified output layers.
    /// Kept ArcGIS-free so unit tests can validate review filters.
    /// </summary>
    public static class GersifyMapReviewSql
    {
        public const double WeakLinkScoreBuffer = 8.0;

        public static string BuildUnmatchedWhereClause(string gersIdField, string linkReviewField = null)
        {
            if (!string.IsNullOrWhiteSpace(linkReviewField))
            {
                return $"{RequireFieldName(linkReviewField)} = 'unmatched'";
            }

            string field = RequireFieldName(gersIdField);
            return $"({field} IS NULL OR TRIM({field}) = '')";
        }

        public static string BuildWeakLinksWhereClause(
            string gersIdField,
            string scoreField,
            double acceptScoreThreshold,
            double weakLinkScoreBuffer = WeakLinkScoreBuffer,
            string linkReviewField = null)
        {
            if (!string.IsNullOrWhiteSpace(linkReviewField))
            {
                return $"{RequireFieldName(linkReviewField)} = 'weak'";
            }

            string gersField = RequireFieldName(gersIdField);
            string matchScoreField = RequireFieldName(scoreField);
            double weakCeiling = acceptScoreThreshold + weakLinkScoreBuffer;
            return
                $"({gersField} IS NOT NULL AND TRIM({gersField}) <> '' " +
                $"AND {matchScoreField} >= {FormatScore(acceptScoreThreshold)} " +
                $"AND {matchScoreField} < {FormatScore(weakCeiling)})";
        }

        public static string BuildSourceKeysWhereClause(string sourceIdField, IReadOnlyList<string> keys)
        {
            string field = RequireFieldName(sourceIdField);
            if (keys == null || keys.Count == 0)
                return "1=0";

            var literals = new List<string>(keys.Count);
            foreach (string key in keys)
            {
                if (string.IsNullOrWhiteSpace(key))
                    continue;

                literals.Add($"'{EscapeSqlLiteral(key)}'");
            }

            if (literals.Count == 0)
                return "1=0";

            return $"{field} IN ({string.Join(", ", literals)})";
        }

        private static string RequireFieldName(string fieldName)
        {
            if (string.IsNullOrWhiteSpace(fieldName))
                throw new ArgumentException("Field name is required.", nameof(fieldName));

            return fieldName.Trim();
        }

        private static string FormatScore(double value) =>
            value.ToString("0.###", CultureInfo.InvariantCulture);

        private static string EscapeSqlLiteral(string value) =>
            value.Replace("'", "''", StringComparison.Ordinal);
    }
}
