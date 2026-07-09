using System;
using System.Collections.Generic;
using System.Linq;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Shared Overture Maps schema metadata used by both DataProcessor (column exclusions
    /// during S3 ingest) and MfcUtility (field exclusions during MFC generation).
    /// </summary>
    public static class OvertureSchema
    {
        public const string ReferenceVersion = "v1.17.0";

        /// <summary>
        /// Columns to exclude per Overture dataset type. These columns are either deeply nested
        /// structs that cannot be represented in GeoParquet/MFC, or are large arrays that add
        /// significant transfer cost without benefit in a GIS context.
        /// </summary>
        public static readonly Dictionary<string, HashSet<string>> ColumnExclusions = new(StringComparer.OrdinalIgnoreCase)
        {
            { "address", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "address_levels", "sources" } },
            { "building", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "building_part", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "connector", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "division", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "local_type", "hierarchies", "capital_division_ids", "capital_of_divisions" } },
            { "division_area", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "infrastructure", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } },
            { "land", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } },
            { "land_cover", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources" } },
            { "land_use", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } },
            { "place", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "addresses", "brand", "emails", "phones", "socials", "sources", "websites" } },
            { "segment", new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
                "access_restrictions", "connectors", "destinations", "level_rules",
                "prohibited_transitions", "road_flags", "road_surface", "routes", "sources",
                "speed_limits", "subclass_rules", "width_rules"
              }
            },
            { "water", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sources", "source_tags" } }
        };

        public static readonly Dictionary<string, HashSet<string>> ExpectedColumns = new(StringComparer.OrdinalIgnoreCase)
        {
            { "address", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "address_levels", "country", "number", "postal_city", "postcode", "street", "unit") },
            { "building", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "subtype", "class", "has_parts", "names", "level", "height", "is_underground", "num_floors", "num_floors_underground", "min_height", "min_floor", "facade_color", "facade_material", "roof_material", "roof_shape", "roof_direction", "roof_orientation", "roof_color", "roof_height") },
            { "building_part", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "building_id", "names", "level", "height", "is_underground", "num_floors", "num_floors_underground", "min_height", "min_floor", "facade_color", "facade_material", "roof_material", "roof_shape", "roof_direction", "roof_orientation", "roof_color", "roof_height") },
            { "connector", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources") },
            { "division", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "cartography", "names", "subtype", "country", "hierarchies", "parent_division_id", "admin_level", "class", "local_type", "region", "perspectives", "norms", "population", "capital_division_ids", "capital_of_divisions", "wikidata") },
            { "division_area", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "division_id", "subtype", "class", "country", "region", "names", "is_land", "is_territorial") },
            { "infrastructure", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "source_tags", "subtype", "class", "names", "level", "height", "surface", "wikidata") },
            { "land", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "source_tags", "subtype", "class", "names", "level", "surface", "wikidata", "elevation") },
            { "land_cover", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "cartography", "subtype") },
            { "land_use", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "source_tags", "subtype", "class", "names", "level", "surface", "wikidata") },
            { "place", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "operating_status", "categories", "taxonomy", "basic_category", "confidence", "websites", "socials", "emails", "phones", "brand", "addresses", "names") },
            { "segment", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "subtype", "access_restrictions", "connectors", "level_rules", "routes", "subclass_rules", "names", "class", "destinations", "prohibited_transitions", "road_flags", "road_surface", "speed_limits", "subclass", "width_rules", "rail_flags") },
            { "water", ColumnSet("id", "bbox", "geometry", "theme", "type", "version", "filename", "sources", "source_tags", "subtype", "class", "names", "level", "wikidata", "is_salt", "is_intermittent") }
        };

        public sealed class SchemaCompatibilityReport
        {
            public string DatasetType { get; init; }
            public string ReferenceSchemaVersion { get; init; } = ReferenceVersion;
            public IReadOnlyList<string> MissingExpectedColumns { get; init; } = Array.Empty<string>();
            public IReadOnlyList<string> UnknownColumns { get; init; } = Array.Empty<string>();
            public IReadOnlyList<string> FlattenedFields { get; init; } = Array.Empty<string>();

            public bool HasFindings =>
                MissingExpectedColumns.Count > 0 ||
                UnknownColumns.Count > 0 ||
                FlattenedFields.Count > 0;
        }

        public static SchemaCompatibilityReport BuildCompatibilityReport(
            string actualS3Type,
            IReadOnlyCollection<string> columnNames,
            IReadOnlyCollection<string> flattenedFields = null)
        {
            var columns = new HashSet<string>(columnNames ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
            string type = actualS3Type ?? string.Empty;

            if (!ExpectedColumns.TryGetValue(type, out var expected))
            {
                return new SchemaCompatibilityReport
                {
                    DatasetType = actualS3Type,
                    FlattenedFields = Sorted(flattenedFields)
                };
            }

            return new SchemaCompatibilityReport
            {
                DatasetType = actualS3Type,
                MissingExpectedColumns = expected.Where(c => !columns.Contains(c)).OrderBy(c => c, StringComparer.OrdinalIgnoreCase).ToList(),
                UnknownColumns = columns.Where(c => !expected.Contains(c)).OrderBy(c => c, StringComparer.OrdinalIgnoreCase).ToList(),
                FlattenedFields = Sorted(flattenedFields)
            };
        }

        private static HashSet<string> ColumnSet(params string[] columns) =>
            new(columns, StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<string> Sorted(IReadOnlyCollection<string> values) =>
            values == null
                ? Array.Empty<string>()
                : values.Where(v => !string.IsNullOrWhiteSpace(v)).OrderBy(v => v, StringComparer.OrdinalIgnoreCase).ToList();
    }
}
