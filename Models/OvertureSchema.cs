using System;
using System.Collections.Generic;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Shared Overture Maps schema metadata used by both DataProcessor (column exclusions
    /// during S3 ingest) and MfcUtility (field exclusions during MFC generation).
    /// </summary>
    public static class OvertureSchema
    {
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
    }
}
