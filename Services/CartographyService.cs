using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using ArcGIS.Core.CIM;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Models;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Applies professional cartographic symbology to Overture Maps feature layers
    /// based on a selected <see cref="MapStyleDefinition"/>.
    /// </summary>
    public static class CartographyService
    {
        /// <summary>
        /// Create a CIM renderer for the given Overture data type, geometry, and style.
        /// Returns null if the type is not recognized.
        /// </summary>
        public static CIMRenderer CreateRendererForLayer(string actualType, string geometryType, MapStyleDefinition style)
        {
            if (style == null || string.IsNullOrEmpty(actualType))
                return null;

            return CreateRenderer(actualType, geometryType, style);
        }

        /// <summary>
        /// Create a CIM renderer for the given layer info and style.
        /// Returns null if the type is not recognized.
        /// </summary>
        public static CIMRenderer CreateRendererForLayer(LayerCreationInfo layerInfo, MapStyleDefinition style)
        {
            if (style == null || layerInfo == null)
                return null;

            return CreateRenderer(layerInfo.ActualType, layerInfo.GeometryType, style);
        }

        private static CIMRenderer CreateRenderer(string actualType, string geometryType, MapStyleDefinition style)
        {
            return actualType?.ToLowerInvariant() switch
            {
                "building" or "building_part" => CreateBuildingRenderer(style),
                "water" => CreatePolygonOrLineRenderer(geometryType, style.WaterFillColor, style.WaterOutlineColor, style.WaterLineColor, 1.0),
                "land" => CreateSimplePolygonRenderer(style.LandFillColor, style.LandOutlineColor),
                "land_use" => CreateLandUseRenderer(style),
                "land_cover" => CreateLandCoverRenderer(style),
                "segment" => CreateSegmentRenderer(geometryType, style),
                "connector" => CreateConnectorRenderer(style),
                "place" => CreatePlaceRenderer(style),
                "division" => IsLineGeometry(geometryType) ? null : (geometryType != null && (geometryType.Equals("POINT", StringComparison.OrdinalIgnoreCase) || geometryType.Equals("MULTIPOINT", StringComparison.OrdinalIgnoreCase)) ? CreateSimplePointRenderer(style.PlaceDefaultColor, style.PlaceDefaultSize) : CreateDivisionPolygonRenderer(style)),
                "division_boundary" => CreateBoundaryRenderer(style),
                "division_area" => CreateDivisionPolygonRenderer(style),
                "infrastructure" => CreateInfrastructureRenderer(geometryType, style),
                "bathymetry" => CreateSimplePolygonRenderer(style.BathymetryFillColor, style.WaterOutlineColor),
                "address" => CreateAddressRenderer(style),
                _ => null,
            };
        }

        private static CIMSimpleRenderer CreateBuildingRenderer(MapStyleDefinition style)
        {
            var symbol = SymbolFactory.Instance.ConstructPolygonSymbol(
                ParseColor(style.BuildingFillColor),
                SimpleFillStyle.Solid,
                SymbolFactory.Instance.ConstructStroke(
                    ParseColor(style.BuildingOutlineColor), 0.5, SimpleLineStyle.Solid));

            return new CIMSimpleRenderer { Symbol = symbol.MakeSymbolReference() };
        }

        private static CIMRenderer CreatePolygonOrLineRenderer(string geometryType, string fillColor, string outlineColor, string lineColor, double lineWidth)
        {
            if (IsLineGeometry(geometryType))
            {
                return CreateSimpleLineRenderer(lineColor, lineWidth);
            }
            return CreateSimplePolygonRenderer(fillColor, outlineColor);
        }

        private static CIMSimpleRenderer CreateSimplePolygonRenderer(string fillColor, string outlineColor)
        {
            var symbol = SymbolFactory.Instance.ConstructPolygonSymbol(
                ParseColor(fillColor),
                SimpleFillStyle.Solid,
                SymbolFactory.Instance.ConstructStroke(
                    ParseColor(outlineColor ?? fillColor), 0.3, SimpleLineStyle.Solid));

            return new CIMSimpleRenderer { Symbol = symbol.MakeSymbolReference() };
        }

        /// <summary>
        /// Division/division_area polygon with semi-transparent fill so it does not obscure layers beneath.
        /// </summary>
        private static CIMSimpleRenderer CreateDivisionPolygonRenderer(MapStyleDefinition style)
        {
            const int fillAlpha = 20; // Semi-transparent so roads and other features show through
            var symbol = SymbolFactory.Instance.ConstructPolygonSymbol(
                ParseColor(style.DivisionFillColor, fillAlpha),
                SimpleFillStyle.Solid,
                SymbolFactory.Instance.ConstructStroke(
                    ParseColor(style.BoundaryLineColor), 0.5, SimpleLineStyle.Solid));

            return new CIMSimpleRenderer { Symbol = symbol.MakeSymbolReference() };
        }

        private static CIMSimpleRenderer CreateSimpleLineRenderer(string color, double width)
        {
            var symbol = SymbolFactory.Instance.ConstructLineSymbol(
                ParseColor(color), width, SimpleLineStyle.Solid);

            return new CIMSimpleRenderer { Symbol = symbol.MakeSymbolReference() };
        }

        private static CIMSimpleRenderer CreateSimplePointRenderer(string color, double size)
        {
            var symbol = SymbolFactory.Instance.ConstructPointSymbol(
                ParseColor(color), size, SimpleMarkerStyle.Circle);

            return new CIMSimpleRenderer { Symbol = symbol.MakeSymbolReference() };
        }

        private static CIMUniqueValueRenderer CreateLandUseRenderer(MapStyleDefinition style)
        {
            var defaultSymbol = SymbolFactory.Instance.ConstructPolygonSymbol(
                ParseColor(style.LandFillColor), SimpleFillStyle.Solid,
                SymbolFactory.Instance.ConstructStroke(ParseColor(style.LandOutlineColor), 0.2, SimpleLineStyle.Solid));

            var classes = new List<CIMUniqueValueClass>();
            var categoryMap = new Dictionary<string, string>
            {
                { "park", style.ParkFillColor },
                { "garden", style.ParkFillColor },
                { "recreation_ground", style.ParkFillColor },
                { "nature_reserve", style.ParkFillColor },
                { "residential", style.ResidentialFillColor },
                { "commercial", style.CommercialFillColor },
                { "retail", style.CommercialFillColor },
                { "industrial", style.IndustrialFillColor },
                { "education", style.EducationFillColor },
                { "school", style.EducationFillColor },
                { "university", style.EducationFillColor },
                { "hospital", style.HospitalFillColor },
                { "cemetery", style.CemeteryFillColor },
                { "military", style.MilitaryFillColor },
            };

            foreach (var kvp in categoryMap)
            {
                var sym = SymbolFactory.Instance.ConstructPolygonSymbol(
                    ParseColor(kvp.Value), SimpleFillStyle.Solid,
                    SymbolFactory.Instance.ConstructStroke(ParseColor(style.LandOutlineColor), 0.2, SimpleLineStyle.Solid));

                classes.Add(new CIMUniqueValueClass
                {
                    Label = kvp.Key,
                    Values = new[] { new CIMUniqueValue { FieldValues = new[] { kvp.Key } } },
                    Symbol = sym.MakeSymbolReference(),
                    Visible = true,
                });
            }

            return new CIMUniqueValueRenderer
            {
                Fields = new[] { "subtype" },
                Groups = new[]
                {
                    new CIMUniqueValueGroup
                    {
                        Classes = classes.ToArray(),
                        Heading = "Land Use",
                    }
                },
                DefaultSymbol = defaultSymbol.MakeSymbolReference(),
                DefaultLabel = "Other",
                UseDefaultSymbol = true,
            };
        }

        private static CIMSimpleRenderer CreateLandCoverRenderer(MapStyleDefinition style)
        {
            return CreateSimplePolygonRenderer(style.ForestFillColor, style.LandOutlineColor);
        }

        private static CIMRenderer CreateSegmentRenderer(string geometryType, MapStyleDefinition style)
        {
            if (!IsLineGeometry(geometryType))
                return CreateSimplePointRenderer(style.ConnectorColor, style.ConnectorSize);

            var defaultSymbol = SymbolFactory.Instance.ConstructLineSymbol(
                ParseColor(style.ResidentialRoadColor), style.ResidentialRoadWidth, SimpleLineStyle.Solid);

            var classes = new List<CIMUniqueValueClass>();

            foreach (var kvp in style.RoadClassColors)
            {
                double width = style.RoadClassWidths.TryGetValue(kvp.Key, out double w) ? w : 1.0;
                var sym = SymbolFactory.Instance.ConstructLineSymbol(
                    ParseColor(kvp.Value), width, SimpleLineStyle.Solid);

                classes.Add(new CIMUniqueValueClass
                {
                    Label = kvp.Key,
                    Values = new[] { new CIMUniqueValue { FieldValues = new[] { kvp.Key } } },
                    Symbol = sym.MakeSymbolReference(),
                    Visible = true,
                });
            }

            return new CIMUniqueValueRenderer
            {
                Fields = new[] { "class" },
                Groups = new[]
                {
                    new CIMUniqueValueGroup
                    {
                        Classes = classes.ToArray(),
                        Heading = "Road Class",
                    }
                },
                DefaultSymbol = defaultSymbol.MakeSymbolReference(),
                DefaultLabel = "Other",
                UseDefaultSymbol = true,
            };
        }

        private static CIMSimpleRenderer CreateConnectorRenderer(MapStyleDefinition style)
        {
            return CreateSimplePointRenderer(style.ConnectorColor, style.ConnectorSize);
        }

        private static CIMSimpleRenderer CreatePlaceRenderer(MapStyleDefinition style)
        {
            return CreateSimplePointRenderer(style.PlaceDefaultColor, style.PlaceDefaultSize);
        }

        private static CIMSimpleRenderer CreateBoundaryRenderer(MapStyleDefinition style)
        {
            var symbol = SymbolFactory.Instance.ConstructLineSymbol(
                ParseColor(style.BoundaryLineColor), style.BoundaryLineWidth, SimpleLineStyle.Dash);

            return new CIMSimpleRenderer { Symbol = symbol.MakeSymbolReference() };
        }

        private static CIMRenderer CreateInfrastructureRenderer(string geometryType, MapStyleDefinition style)
        {
            if (IsLineGeometry(geometryType))
                return CreateSimpleLineRenderer(style.InfrastructureLineColor, 0.8);
            return CreateSimplePolygonRenderer(style.InfrastructureFillColor, style.InfrastructureLineColor);
        }

        private static CIMSimpleRenderer CreateAddressRenderer(MapStyleDefinition style)
        {
            return CreateSimplePointRenderer(style.AddressPointColor, style.AddressPointSize);
        }

        private static bool IsLineGeometry(string geometryType) =>
            geometryType != null &&
            (geometryType.Equals("LINESTRING", StringComparison.OrdinalIgnoreCase) ||
             geometryType.Equals("MULTILINESTRING", StringComparison.OrdinalIgnoreCase));

        /// <summary>
        /// Apply a style to an existing feature layer by inferring the Overture type from the layer name.
        /// Updates only labels in the CIM definition, then applies symbology via SetRenderer so that
        /// changing style multiple times does not break feature drawing (avoids SetDefinition with a new renderer).
        /// Must be called on the MCT (inside QueuedTask.Run).
        /// </summary>
        public static bool ApplyStyleToExistingLayer(FeatureLayer featureLayer, MapStyleDefinition style)
        {
            if (style == null || featureLayer == null)
                return false;

            try
            {
                string actualType = InferOvertureType(featureLayer.Name);
                if (actualType == null)
                    return false;

                string geometryType = featureLayer.ShapeType switch
                {
                    esriGeometryType.esriGeometryPoint => "POINT",
                    esriGeometryType.esriGeometryPolyline => "LINESTRING",
                    esriGeometryType.esriGeometryPolygon => "POLYGON",
                    esriGeometryType.esriGeometryMultipoint => "POINT",
                    _ => "POLYGON"
                };

                if (featureLayer.Name.Contains("multipolygon", StringComparison.OrdinalIgnoreCase))
                    geometryType = "MULTIPOLYGON";
                else if (featureLayer.Name.Contains("multiline", StringComparison.OrdinalIgnoreCase))
                    geometryType = "MULTILINESTRING";

                var renderer = CreateRenderer(actualType, geometryType, style);
                if (renderer != null)
                {
                    var layerDef = featureLayer.GetDefinition() as CIMFeatureLayer;
                    if (layerDef != null)
                    {
                        // Update only labels in the definition; do not set layerDef.Renderer here.
                        // Setting the renderer in the definition and then SetDefinition causes features to
                        // stop drawing on the second and later style changes (labels still update).
                        bool labelsAdded = ApplyLabelClasses(layerDef, actualType, geometryType, style);
                        featureLayer.SetDefinition(layerDef);

                        // Apply symbology only via SetRenderer so style can be changed repeatedly.
                        try
                        {
                            featureLayer.SetRenderer(renderer);
                        }
                        catch (Exception rendererEx)
                        {
                            System.Diagnostics.Debug.WriteLine($"CartographyService: SetRenderer failed for '{featureLayer.Name}': {rendererEx.Message}");
                        }
                        if (labelsAdded)
                            featureLayer.SetLabelVisibility(true);
                        System.Diagnostics.Debug.WriteLine(
                            $"CartographyService: Re-styled '{featureLayer.Name}' with '{style.DisplayName}' (inferred type={actualType}, geom={geometryType})");
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(
                    $"CartographyService: Failed to re-style '{featureLayer.Name}': {ex.Message}");
            }
            return false;
        }

        private static readonly Dictionary<string, string> _layerNameToType = new(StringComparer.OrdinalIgnoreCase)
        {
            { "Address", "address" },
            { "Land Cover", "land_cover" },
            { "Land Use", "land_use" },
            { "Land", "land" },
            { "Water", "water" },
            { "Infrastructure", "infrastructure" },
            { "Building Part", "building_part" },
            { "Building", "building" },
            { "Division Boundary", "division_boundary" },
            { "Division Area", "division_area" },
            { "Division", "division" },
            { "Place", "place" },
            { "Connector", "connector" },
            { "Segment", "segment" },
            { "Bathymetry", "bathymetry" },
        };

        /// <summary>
        /// Adds basemap-style label classes to the layer definition for point layers that have name fields.
        /// Call this before SetDefinition so labels are applied in the same CIM update as the renderer.
        /// Returns true if at least one label class was added (caller should call featureLayer.SetLabelVisibility(true)).
        /// </summary>
        public static bool ApplyLabelClasses(CIMFeatureLayer layerDef, string actualType, string geometryType, MapStyleDefinition style)
        {
            if (layerDef == null || string.IsNullOrEmpty(actualType))
                return false;

            bool isPoint = geometryType != null &&
                (geometryType.Equals("POINT", StringComparison.OrdinalIgnoreCase) ||
                 geometryType.Equals("MULTIPOINT", StringComparison.OrdinalIgnoreCase));

            string arcadeExpression = GetLabelExpressionForType(actualType, isPoint);
            if (arcadeExpression == null)
                return false;

            CIMColor labelColor = style?.PlaceDefaultColor != null ? ParseColor(style.PlaceDefaultColor) : new CIMRGBColor { R = 50, G = 50, B = 50, Alpha = 100 };
            CIMTextSymbol textSymbol = SymbolFactory.Instance.ConstructTextSymbol(labelColor, 8.5, "Arial");
            CIMLabelClass labelClass = new CIMLabelClass
            {
                Name = "Overture Labels",
                Expression = arcadeExpression,
                ExpressionEngine = LabelExpressionEngine.Arcade,
                ExpressionTitle = "Arcade",
                TextSymbol = textSymbol.MakeSymbolReference(),
                Visibility = true
            };

            layerDef.LabelClasses = new[] { labelClass };
            return true;
        }

        /// <summary>
        /// Returns Arcade expression for the layer type, or null if no labels.
        /// </summary>
        private static string GetLabelExpressionForType(string actualType, bool isPoint)
        {
            if (string.IsNullOrEmpty(actualType))
                return null;

            switch (actualType.ToLowerInvariant())
            {
                case "place":
                    return "var n = $feature['names_primary']; return DefaultValue(n, '');";
                case "address":
                    return "var num = DefaultValue($feature['number'], ''); var street = DefaultValue($feature['street'], ''); if (IsEmpty(num)) return street; if (IsEmpty(street)) return Text(num); return num + ' ' + street;";
                case "division":
                    if (!isPoint) return null;
                    return "var n = $feature['names_primary']; return DefaultValue(n, '');";
                case "infrastructure":
                case "land":
                    if (!isPoint) return null;
                    return "var n = $feature['names_primary']; return DefaultValue(n, '');";
                default:
                    return null; // connector and others: no labels
            }
        }

        /// <summary>
        /// Infer the Overture S3 type from a layer name following the "Theme - Type (geometry)" convention.
        /// </summary>
        internal static string InferOvertureType(string layerName)
        {
            if (string.IsNullOrEmpty(layerName))
                return null;

            int dashIdx = layerName.IndexOf(" - ", StringComparison.Ordinal);
            string typePart = dashIdx >= 0 ? layerName.Substring(dashIdx + 3) : layerName;

            int parenIdx = typePart.IndexOf(" (", StringComparison.Ordinal);
            if (parenIdx >= 0)
                typePart = typePart.Substring(0, parenIdx);

            typePart = typePart.Trim();

            foreach (var kvp in _layerNameToType)
            {
                if (typePart.Equals(kvp.Key, StringComparison.OrdinalIgnoreCase))
                    return kvp.Value;
            }

            return null;
        }

        /// <summary>
        /// Parse a hex color string (e.g., "#FAFAE8") into a CIMRGBColor.
        /// </summary>
        private static CIMColor ParseColor(string hex, int alpha = 100)
        {
            if (string.IsNullOrEmpty(hex))
                return ColorFactory.Instance.BlackRGB;

            hex = hex.TrimStart('#');
            if (hex.Length < 6)
                return ColorFactory.Instance.BlackRGB;

            int r = int.Parse(hex.Substring(0, 2), NumberStyles.HexNumber);
            int g = int.Parse(hex.Substring(2, 2), NumberStyles.HexNumber);
            int b = int.Parse(hex.Substring(4, 2), NumberStyles.HexNumber);
            int clampedAlpha = Math.Max(0, Math.Min(100, alpha));

            return new CIMRGBColor { R = r, G = g, B = b, Alpha = clampedAlpha };
        }
    }
}
