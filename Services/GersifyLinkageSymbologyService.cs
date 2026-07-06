using ArcGIS.Core.CIM;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace DuckDBGeoparquet.Services
{
    public static class GersifyLinkageSymbologyService
    {
        public const string ReviewFieldName = "gers_link_review";
        public const string LinkedValue = "linked";
        public const string WeakValue = "weak";
        public const string UnmatchedValue = "unmatched";

        public static bool TryApplyLinkageReviewSymbology(FeatureLayer layer)
        {
            if (layer == null)
                return false;

            try
            {
                using var featureClass = layer.GetFeatureClass();
                bool hasReviewField = featureClass.GetDefinition()
                    .GetFields()
                    .Any(field => string.Equals(field.Name, ReviewFieldName, StringComparison.OrdinalIgnoreCase));
                if (!hasReviewField)
                    return false;

                var renderer = CreateLinkageReviewRenderer(ReviewFieldName);
                layer.SetRenderer(renderer);
                layer.SetVisibility(true);
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"GERSify symbology skipped: {ex.Message}");
                return false;
            }
        }

        public static CIMUniqueValueRenderer CreateLinkageReviewRenderer(string fieldName)
        {
            var classes = new List<CIMUniqueValueClass>
            {
                CreatePointClass(LinkedValue, "#2E7D32", 7.5, "Linked — stable GERS ID attached"),
                CreatePointClass(WeakValue, "#F9A825", 8.5, "Weak link — review before adoption"),
                CreatePointClass(UnmatchedValue, "#C62828", 7.0, "Unmatched — no GERS link yet")
            };

            return new CIMUniqueValueRenderer
            {
                Fields = new[] { fieldName },
                Groups = new[]
                {
                    new CIMUniqueValueGroup
                    {
                        Classes = classes.ToArray(),
                        Heading = "GERSify linkage"
                    }
                },
                DefaultLabel = "Other",
                DefaultSymbol = CreatePointSymbol("#757575", 6.0).MakeSymbolReference()
            };
        }

        private static CIMUniqueValueClass CreatePointClass(string value, string color, double size, string label)
        {
            return new CIMUniqueValueClass
            {
                Label = label,
                Values = new[] { new CIMUniqueValue { FieldValues = new[] { value } } },
                Symbol = CreatePointSymbol(color, size).MakeSymbolReference()
            };
        }

        private static CIMPointSymbol CreatePointSymbol(string hexColor, double size)
        {
            CIMColor color = ParseColor(hexColor);
            return SymbolFactory.Instance.ConstructPointSymbol(color, size, SimpleMarkerStyle.Circle);
        }

        private static CIMColor ParseColor(string hex, int alpha = 100)
        {
            if (string.IsNullOrEmpty(hex))
                return ColorFactory.Instance.BlackRGB;

            hex = hex.TrimStart('#');
            if (hex.Length < 6)
                return ColorFactory.Instance.BlackRGB;

            int r = int.Parse(hex[..2], NumberStyles.HexNumber);
            int g = int.Parse(hex.Substring(2, 2), NumberStyles.HexNumber);
            int b = int.Parse(hex.Substring(4, 2), NumberStyles.HexNumber);
            int clampedAlpha = Math.Max(0, Math.Min(100, alpha));

            return new CIMRGBColor { R = r, G = g, B = b, Alpha = clampedAlpha };
        }
    }
}
