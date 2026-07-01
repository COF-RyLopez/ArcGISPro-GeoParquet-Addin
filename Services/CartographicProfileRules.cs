using System;
using System.Linq;
using DuckDBGeoparquet.Models;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Resolves profile-level cartographic behavior while keeping rendering code focused.
    /// </summary>
    public static class CartographicProfileRules
    {
        public static MapStyleDefinition ResolveStyle(CartographicProfile profile, MapStyleDefinition fallbackStyle = null)
        {
            if (profile?.Style != null)
                return profile.Style;

            if (!string.IsNullOrWhiteSpace(profile?.BaseStyleId))
            {
                var baseStyle = MapStyleCatalog.AllStyles.FirstOrDefault(style =>
                    string.Equals(style.Id, profile.BaseStyleId, StringComparison.OrdinalIgnoreCase));
                if (baseStyle != null)
                    return baseStyle;
            }

            if (fallbackStyle != null)
                return fallbackStyle;

            return null;
        }

        public static string GetDisplayName(CartographicProfile profile, MapStyleDefinition fallbackStyle = null)
        {
            if (!string.IsNullOrWhiteSpace(profile?.DisplayName))
                return profile.DisplayName;

            if (!string.IsNullOrWhiteSpace(fallbackStyle?.DisplayName))
                return fallbackStyle.DisplayName;

            return "default";
        }

        public static bool ShouldLabel(CartographicProfile profile, string actualType)
        {
            if (string.IsNullOrWhiteSpace(actualType))
                return false;

            if (profile?.EnabledLabelTypes == null)
                return true;

            return profile.EnabledLabelTypes.Contains(actualType);
        }

        public static bool ShowLocalRoadLabels(CartographicProfile profile)
        {
            return profile?.ShowLocalRoadLabels ?? true;
        }

        public static double GetPointMinScale(CartographicProfile profile, string actualType)
        {
            if (!string.IsNullOrWhiteSpace(actualType) &&
                profile?.PointMinScales != null &&
                profile.PointMinScales.TryGetValue(actualType, out double profileScale))
            {
                return profileScale;
            }

            if (string.IsNullOrWhiteSpace(actualType))
                return 25000;

            return actualType.ToLowerInvariant() switch
            {
                "address" => 6000,
                "connector" => 12000,
                "place" => 10000,
                "division" => 40000,
                "infrastructure" => 22000,
                "land" => 28000,
                _ => 25000
            };
        }
    }
}
