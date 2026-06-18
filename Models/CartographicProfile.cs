using System;
using System.Collections.Generic;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Describes a user-facing map purpose that resolves to a concrete map style plus
    /// profile-specific rules for labels, scale visibility, and drawing hierarchy.
    /// </summary>
    public class CartographicProfile
    {
        public string Id { get; set; }
        public string DisplayName { get; set; }
        public string Description { get; set; }
        public string Category { get; set; }
        public string ThumbnailUri { get; set; }
        public string BaseStyleId { get; set; }

        /// <summary>
        /// Concrete style resolved from the base style plus this profile's overrides.
        /// </summary>
        public MapStyleDefinition Style { get; set; }

        /// <summary>
        /// Optional per-type minimum visible scales for dense point layers.
        /// </summary>
        public Dictionary<string, double> PointMinScales { get; set; } = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Optional allow-list of Overture types that should receive labels.
        /// Null means use the default label behavior.
        /// </summary>
        public HashSet<string> EnabledLabelTypes { get; set; }

        public bool ShowLocalRoadLabels { get; set; } = true;
    }
}
