using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DuckDBGeoparquet.Services
{
    public static class GersifyRecordIdPolicy
    {
        public const string GeneratedLinkIdFieldName = "gersify_record_id";
        public const string SourceRecordKeyFieldName = "source_record_key";

        private static readonly string[] PreferredIdFieldCandidates =
        [
            "globalid",
            "guid",
            "uuid",
            "uniqueid",
            "unique_id",
            "site_id",
            "siteid",
            "address_id",
            "addressid",
            "facility_id",
            "facilityid",
            "place_id",
            "placeid",
            "asset_id",
            "assetid",
            "record_id",
            "recordid",
            "feature_id",
            "featureid",
            "unit_id",
            "unitid",
            "ng911_id",
            "ng911id",
            "loc_id",
            "locid",
            "id"
        ];

        private static readonly string[] UnstableIdFieldCandidates =
        [
            "objectid",
            "oid",
            "fid",
            "rowid",
            "esri_oid"
        ];

        public static string SelectPreferredIdField(IEnumerable<string> fields)
        {
            var fieldList = fields?
                .Where(field => !string.IsNullOrWhiteSpace(field))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList() ?? [];

            foreach (string candidate in PreferredIdFieldCandidates)
            {
                string match = FindField(fieldList, candidate);
                if (!string.IsNullOrWhiteSpace(match) && !IsUnstableArcGisRowId(match))
                    return match;
            }

            foreach (string field in fieldList.OrderBy(value => value, StringComparer.OrdinalIgnoreCase))
            {
                if (IsUnstableArcGisRowId(field))
                    continue;

                string normalized = NormalizeFieldName(field);
                if (normalized.Contains("guid", StringComparison.Ordinal) ||
                    normalized.Contains("uuid", StringComparison.Ordinal) ||
                    normalized.EndsWith("id", StringComparison.Ordinal))
                {
                    return field;
                }
            }

            return null;
        }

        public static bool IsUnstableArcGisRowId(string fieldName)
        {
            if (string.IsNullOrWhiteSpace(fieldName))
                return false;

            string normalized = NormalizeFieldName(fieldName);
            return UnstableIdFieldCandidates.Any(candidate =>
                string.Equals(normalized, candidate, StringComparison.Ordinal));
        }

        public static bool ShouldGenerateStableLinkId(string selectedIdField, bool generateStableLinkId) =>
            generateStableLinkId || IsUnstableArcGisRowId(selectedIdField);

        public static string ResolveOutputRelateField(string selectedIdField, bool generateStableLinkId)
        {
            if (ShouldGenerateStableLinkId(selectedIdField, generateStableLinkId))
                return SourceRecordKeyFieldName;

            return "record_id";
        }

        public static IReadOnlyList<string> BuildIdFieldWarnings(string selectedIdField, bool generateStableLinkId)
        {
            var warnings = new List<string>();
            if (string.IsNullOrWhiteSpace(selectedIdField))
            {
                warnings.Add("Unique ID is required. Choose a durable business key, or enable Generate stable link IDs.");
                return warnings;
            }

            if (IsUnstableArcGisRowId(selectedIdField))
            {
                if (generateStableLinkId)
                {
                    warnings.Add(
                        $"Selected field '{selectedIdField}' is an ArcGIS row pointer (OBJECTID/FID) and is not stable across reloads. " +
                        "GERSify will write stable UUIDs to record_id and keep the current row value in source_record_key for map relates.");
                }
                else
                {
                    warnings.Add(
                        $"Selected field '{selectedIdField}' is an ArcGIS row pointer (OBJECTID/FID). It changes when data is reloaded or copied and is a poor bridge/relate key. " +
                        "Choose a business key such as GlobalID or site_id, or enable Generate stable link IDs.");
                }
            }
            else if (generateStableLinkId)
            {
                warnings.Add(
                    "Generate stable link IDs is enabled. record_id will use new UUIDs for bridge output; source_record_key keeps the selected field value for audit and map relates.");
            }

            return warnings;
        }

        private static string FindField(IReadOnlyList<string> fields, string candidate)
        {
            string normalizedCandidate = NormalizeFieldName(candidate);
            return fields.FirstOrDefault(field =>
                string.Equals(NormalizeFieldName(field), normalizedCandidate, StringComparison.Ordinal));
        }

        private static string NormalizeFieldName(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            var sb = new StringBuilder(value.Length);
            foreach (char ch in value)
            {
                if (char.IsLetterOrDigit(ch))
                    sb.Append(char.ToLowerInvariant(ch));
            }

            return sb.ToString();
        }
    }
}
