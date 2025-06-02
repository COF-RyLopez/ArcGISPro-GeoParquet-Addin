using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Manages styling for Overture Maps data layers
    /// </summary>
    public class OvertureStyleManager
    {
        private const string OVERTURE_STYLE_NAME = "OvertureStyle";
        
        /// <summary>
        /// Applies appropriate Overture Maps styling to a feature layer
        /// </summary>
        /// <param name="layer">The feature layer to style</param>
        /// <param name="theme">The Overture Maps theme (e.g., "transportation", "buildings")</param>
        /// <param name="type">The Overture Maps type (e.g., "segment", "building")</param>
        public static async Task ApplyOvertureMapsStyling(FeatureLayer layer, string theme, string type)
        {
            if (layer == null || string.IsNullOrEmpty(theme))
                return;

            try
            {
                await QueuedTask.Run(() =>
                {
                    // Try to find custom Overture style first
                    var styleProjectItem = Project.Current.GetItems<StyleProjectItem>()
                        .FirstOrDefault(item => item.Name.Contains(OVERTURE_STYLE_NAME));

                    if (styleProjectItem != null)
                    {
                        ApplyDictionaryRenderer(layer, theme, type, styleProjectItem);
                    }
                    else
                    {
                        // Fallback to programmatic styling
                        ApplyDefaultOvertureSymbology(layer, theme, type);
                    }
                });

                System.Diagnostics.Debug.WriteLine($"Applied Overture styling to layer: {layer.Name} (Theme: {theme}, Type: {type})");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error applying Overture styling: {ex.Message}");
                // Don't throw - styling failure shouldn't break layer creation
            }
        }

        /// <summary>
        /// Applies dictionary renderer using custom Overture style
        /// </summary>
        private static void ApplyDictionaryRenderer(FeatureLayer layer, string theme, string type, StyleProjectItem styleItem)
        {
            try
            {
                var layerDefinition = layer.GetDefinition() as CIMFeatureLayer;
                if (layerDefinition == null) return;

                // Create dictionary renderer based on theme
                var dictionaryRenderer = CreateDictionaryRenderer(theme, type);
                if (dictionaryRenderer != null)
                {
                    layerDefinition.Renderer = dictionaryRenderer;
                    layer.SetDefinition(layerDefinition);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error applying dictionary renderer: {ex.Message}");
            }
        }

        /// <summary>
        /// Creates appropriate dictionary renderer for the given theme
        /// </summary>
        private static CIMRenderer CreateDictionaryRenderer(string theme, string type)
        {
            // This would reference actual dictionary files when implemented
            switch (theme.ToLower())
            {
                case "transportation":
                    return CreateTransportationRenderer();
                case "buildings":
                    return CreateBuildingsRenderer();
                case "places":
                    return CreatePlacesRenderer();
                case "divisions":
                    return CreateDivisionsRenderer();
                case "base":
                    return CreateBaseRenderer(type);
                case "addresses":
                    return CreateAddressesRenderer();
                default:
                    return null;
            }
        }

        /// <summary>
        /// Fallback method for programmatic styling when no custom style dictionary is available
        /// </summary>
        private static void ApplyDefaultOvertureSymbology(FeatureLayer layer, string theme, string type)
        {
            try
            {
                var layerDefinition = layer.GetDefinition() as CIMFeatureLayer;
                if (layerDefinition == null) return;

                // Apply simple default symbology based on geometry type and theme
                var geometryType = layer.GetFeatureClass().GetDefinition().GetShapeType();
                
                // Special handling for transportation theme to use dictionary renderer
                if (theme.ToLower() == "transportation" && geometryType == GeometryType.Polyline)
                {
                    var transportationRenderer = CreateDictionaryRenderer(theme, type); // This should lead to our unique value renderer
                    if (transportationRenderer != null)
                    {
                        layerDefinition.Renderer = transportationRenderer;
                    }
                    else
                    {
                        // Fallback to a default line symbol if specific transportation renderer fails
                        ApplyLineSymbology(layerDefinition, theme, type); 
                    }
                }
                else
                {
                    switch (geometryType)
                    {
                        case GeometryType.Point:
                            ApplyPointSymbology(layerDefinition, theme, type);
                            break;
                        case GeometryType.Polyline: // For non-transportation polylines
                            ApplyLineSymbology(layerDefinition, theme, type);
                            break;
                        case GeometryType.Polygon:
                            ApplyPolygonSymbology(layerDefinition, theme, type);
                            break;
                    }
                }

                // Apply scale-dependent visibility for certain themes
                ApplyScaleDependentVisibility(layerDefinition, theme, type);

                layer.SetDefinition(layerDefinition);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error applying default symbology: {ex.Message}");
            }
        }

        /// <summary>
        /// Sets appropriate scale ranges for different Overture Maps themes
        /// This is the correct way to handle zoom-level dependent display in ArcGIS Pro
        /// </summary>
        private static void ApplyScaleDependentVisibility(CIMFeatureLayer layerDef, string theme, string type)
        {
            switch (theme.ToLower())
            {
                case "addresses":
                    // Only show addresses when zoomed in to city/neighborhood level
                    layerDef.MinScale = 25000;  // Hide when zoomed out past 1:25,000 (neighborhood level)
                    layerDef.MaxScale = 0;      // No limit on how far zoomed in
                    break;
                    
                case "base" when type?.ToLower() == "infrastructure":
                    // Infrastructure details only at larger scales
                    layerDef.MinScale = 100000; // Hide when zoomed out past 1:100,000 (regional level)
                    layerDef.MaxScale = 0;      // No limit on how far zoomed in
                    break;
                    
                // Other themes remain visible at all scales by default
                default:
                    // layerDef.MinScale = 0;   // Default: visible at all scales
                    // layerDef.MaxScale = 0;   // Default: no maximum scale limit
                    break;
            }
        }

        #region Dictionary Renderer Creation Methods

        private static CIMRenderer CreateTransportationRenderer()
        {
            // Create a comprehensive transportation renderer that handles all subtypes
            // We'll use a combination of subtype and class for the most detailed styling
            return CreateTransportationUniqueValueRenderer();
        }

        private static CIMRenderer CreateTransportationUniqueValueRenderer()
        {
            var transportationSymbols = GetTransportationSymbols(); // This gets our base symbols

            var renderer = new CIMUniqueValueRenderer
            {
                Fields = new string[] { "subtype", "class" },
                DefaultSymbol = CreateDefaultTransportationSymbol().MakeSymbolReference(), // Default if no match
                Groups = new CIMUniqueValueGroup[]
                {
                    new CIMUniqueValueGroup
                    {
                        Classes = transportationSymbols.Select(kvp =>
                        {
                            var symbol = kvp.Value; // The base CIMSymbol
                            var symbolRef = symbol?.MakeSymbolReference();
                            string label = GetTransportationLabel(kvp.Key);
                            string[] keyParts = kvp.Key.Split('|');
                            string subtypeKey = keyParts[0];
                            string classKey = keyParts.Length > 1 ? keyParts[1] : string.Empty;

                            if (symbolRef != null)
                            {
                                // Apply scale ranges based on road class (kvp.Key)
                                // Larger MinScale = visible when more zoomed out.
                                // MaxScale = 0 means visible all the way zoomed in.
                                switch (subtypeKey)
                                {
                                    case "road":
                                        switch (classKey)
                                        {
                                            case "motorway":
                                            case "motorway_link":
                                            case "trunk":
                                            case "trunk_link":
                                                symbolRef.MinScale = 5000000; // Visible from 1:5,000,000 and closer
                                                symbolRef.MaxScale = 0;
                                                break;
                                            case "primary":
                                            case "primary_link":
                                                symbolRef.MinScale = 2000000; // Visible from 1:2,000,000 and closer
                                                symbolRef.MaxScale = 0;
                                                break;
                                            case "secondary":
                                            case "secondary_link":
                                                symbolRef.MinScale = 750000;
                                                symbolRef.MaxScale = 0;
                                                break;
                                            case "tertiary":
                                            case "tertiary_link":
                                                symbolRef.MinScale = 250000;
                                                symbolRef.MaxScale = 0;
                                                break;
                                            case "residential":
                                            case "living_street":
                                            case "unclassified":
                                                symbolRef.MinScale = 100000; 
                                                symbolRef.MaxScale = 0;
                                                break;
                                            case "service":
                                                symbolRef.MinScale = 75000;
                                                symbolRef.MaxScale = 0;
                                                break;
                                            case "pedestrian":
                                            case "footway":
                                            case "cycleway":
                                            case "path":
                                            case "steps":
                                            case "track":
                                                symbolRef.MinScale = 50000; 
                                                symbolRef.MaxScale = 0;
                                                break;
                                            default: // Other road classes
                                                symbolRef.MinScale = 50000; 
                                                symbolRef.MaxScale = 0;
                                                break;
                                        }
                                        break;
                                    case "rail":
                                        symbolRef.MinScale = 1000000; // Rails visible from 1:1,000,000
                                        symbolRef.MaxScale = 0;
                                        break;
                                    case "water": // Ferry routes
                                        symbolRef.MinScale = 1000000; 
                                        symbolRef.MaxScale = 0;
                                        break;
                                    default:
                                        // Default for unknown subtypes
                                        symbolRef.MinScale = 250000; 
                                        symbolRef.MaxScale = 0;
                                        break;
                                }
                            }

                            return new CIMUniqueValueClass
                            {
                                Values = new CIMUniqueValue[]
                                {
                                    new CIMUniqueValue
                                    {
                                        FieldValues = kvp.Key.Split('|')
                                    }
                                },
                                Symbol = symbolRef, // Use the (potentially scale-modified) symbol reference
                                Label = label
                            };
                        }).ToArray()
                    }
                }
            };
            return renderer;
        }

        private static string GetTransportationLabel(string key)
        {
            var parts = key.Split('|');
            if (parts.Length == 2)
            {
                var subtype = parts[0];
                var classType = parts[1];
                
                return subtype switch
                {
                    "road" => classType switch
                    {
                        "motorway" => "Motorway/Highway",
                        "trunk" => "Trunk Road",
                        "primary" => "Primary Road",
                        "secondary" => "Secondary Road",
                        "tertiary" => "Tertiary Road",
                        "residential" => "Residential Street",
                        "service" => "Service Road",
                        "unclassified" => "Unclassified Road",
                        "track" => "Track/Trail",
                        "path" => "Walking Path",
                        "footway" => "Footway",
                        "cycleway" => "Bike Path",
                        "steps" => "Steps",
                        "pedestrian" => "Pedestrian Area",
                        "living_street" => "Living Street",
                        _ => $"Road - {classType}"
                    },
                    "rail" => "Railway",
                    "water" => "Ferry Route",
                    _ => key
                };
            }
            return key;
        }

        private static Dictionary<string, CIMSymbol> GetTransportationSymbols()
        {
            var symbols = new Dictionary<string, CIMSymbol>();
            
            // Road subtypes with hierarchical styling
            // Major highways and motorways - thick, prominent
            symbols["road|motorway"] = CreateMotorwaySymbol();
            symbols["road|motorway_link"] = CreateMotorwayLinkSymbol();
            symbols["road|trunk"] = CreateTrunkRoadSymbol();
            symbols["road|trunk_link"] = CreateTrunkLinkSymbol();
            
            // Primary roads - medium thickness, important
            symbols["road|primary"] = CreatePrimaryRoadSymbol();
            symbols["road|primary_link"] = CreatePrimaryLinkSymbol();
            symbols["road|secondary"] = CreateSecondaryRoadSymbol();
            symbols["road|secondary_link"] = CreateSecondaryLinkSymbol();
            symbols["road|tertiary"] = CreateTertiaryRoadSymbol();
            symbols["road|tertiary_link"] = CreateTertiaryLinkSymbol();
            
            // Local roads - thinner, less prominent
            symbols["road|residential"] = CreateResidentialRoadSymbol();
            symbols["road|unclassified"] = CreateUnclassifiedRoadSymbol();
            symbols["road|service"] = CreateServiceRoadSymbol();
            symbols["road|living_street"] = CreateLivingStreetSymbol();
            
            // Pedestrian and cycling infrastructure
            symbols["road|pedestrian"] = CreatePedestrianSymbol();
            symbols["road|footway"] = CreateFootwaySymbol();
            symbols["road|cycleway"] = CreateCyclewaySymbol();
            symbols["road|path"] = CreatePathSymbol();
            symbols["road|steps"] = CreateStepsSymbol();
            symbols["road|track"] = CreateTrackSymbol();
            
            // Rail infrastructure
            symbols["rail|"] = CreateRailwaySymbol(); // Some rail entries might have empty class
            symbols["rail|rail"] = CreateRailwaySymbol();
            symbols["rail|subway"] = CreateSubwaySymbol();
            symbols["rail|light_rail"] = CreateLightRailSymbol();
            symbols["rail|tram"] = CreateTramSymbol();
            
            // Water transportation
            symbols["water|"] = CreateFerrySymbol();
            symbols["water|ferry"] = CreateFerrySymbol();
            
            return symbols;
        }

        private static CIMRenderer CreateBuildingsRenderer()
        {
            // Buildings specific dictionary renderer
            // Could use subtype for building categories, facade_material for patterns
            return CreateUniqueValueRenderer("subtype", GetBuildingSymbols());
        }

        private static CIMRenderer CreatePlacesRenderer()
        {
            // Places specific renderer based on categories
            return CreateUniqueValueRenderer("categories_primary", GetPlaceSymbols());
        }

        private static CIMRenderer CreateDivisionsRenderer()
        {
            // Administrative boundaries renderer
            return CreateUniqueValueRenderer("subtype", GetDivisionSymbols());
        }

        private static CIMRenderer CreateBaseRenderer(string type)
        {
            // Base theme renderer (land, water, infrastructure)
            switch (type?.ToLower())
            {
                case "water":
                    return CreateWaterRenderer();
                case "land":
                    return CreateLandRenderer();
                case "infrastructure":
                    return CreateInfrastructureRenderer();
                default:
                    return null;
            }
        }

        private static CIMRenderer CreateAddressesRenderer()
        {
            // Simple point symbols for addresses
            return CreateSimpleRenderer(CreateAddressPointSymbol());
        }

        #endregion

        #region Helper Methods for Symbology

        private static void ApplyPointSymbology(CIMFeatureLayer layerDef, string theme, string type)
        {
            CIMSymbol symbol = theme.ToLower() switch
            {
                "places" => CreatePlacePointSymbol(),
                "addresses" => CreateAddressPointSymbol(),
                _ => CreateDefaultPointSymbol()
            };

            layerDef.Renderer = CreateSimpleRenderer(symbol);
        }

        private static void ApplyLineSymbology(CIMFeatureLayer layerDef, string theme, string type)
        {
            CIMSymbol symbol = theme.ToLower() switch
            {
                "transportation" => CreateTransportationLineSymbol(),
                "divisions" => CreateDivisionBoundarySymbol(),
                _ => CreateDefaultLineSymbol()
            };

            layerDef.Renderer = CreateSimpleRenderer(symbol);
        }

        private static void ApplyPolygonSymbology(CIMFeatureLayer layerDef, string theme, string type)
        {
            CIMSymbol symbol = theme.ToLower() switch
            {
                "buildings" => CreateBuildingPolygonSymbol(),
                "base" => CreateBasePolygonSymbol(type),
                "divisions" => CreateDivisionAreaSymbol(),
                _ => CreateDefaultPolygonSymbol()
            };

            layerDef.Renderer = CreateSimpleRenderer(symbol);
        }

        private static CIMRenderer CreateSimpleRenderer(CIMSymbol symbol)
        {
            return new CIMSimpleRenderer
            {
                Symbol = symbol?.MakeSymbolReference()
            };
        }

        /// <summary>
        /// Creates a renderer with scale-dependent visibility
        /// Alternative approach for handling zoom levels using layer properties
        /// </summary>
        private static CIMRenderer CreateScaleAwareRenderer(CIMSymbol symbol, double minScale = 0, double maxScale = 0)
        {
            // Note: Scale-dependent visibility is better handled at the layer level
            // using layerDefinition.MinScale and layerDefinition.MaxScale
            return new CIMSimpleRenderer
            {
                Symbol = symbol?.MakeSymbolReference()
            };
        }

        private static CIMRenderer CreateUniqueValueRenderer(string fieldName, Dictionary<string, CIMSymbol> symbols)
        {
            var renderer = new CIMUniqueValueRenderer
            {
                Fields = new string[] { fieldName },
                Groups = new CIMUniqueValueGroup[]
                {
                    new CIMUniqueValueGroup
                    {
                        Classes = symbols.Select(kvp => new CIMUniqueValueClass
                        {
                            Values = new CIMUniqueValue[] { new CIMUniqueValue { FieldValues = new string[] { kvp.Key } } },
                            Symbol = kvp.Value?.MakeSymbolReference()
                        }).ToArray()
                    }
                }
            };

            return renderer;
        }

        private static CIMRenderer CreateClassBreakRenderer(string fieldName, Dictionary<string, CIMSymbol> classBreaks)
        {
            // Implementation for class break rendering (e.g., road hierarchy)
            return new CIMClassBreaksRenderer
            {
                Field = fieldName,
                // Additional implementation needed based on specific requirements
            };
        }

        #endregion

        #region Symbol Creation Methods

        private static CIMSymbol CreateDefaultPointSymbol()
        {
            return CreateScaleDependentPointSymbol(4, 2);
        }

        /// <summary>
        /// Creates a point symbol with proportional scaling enabled
        /// </summary>
        private static CIMSymbol CreateScaleDependentPointSymbol(double baseSize, double smallSize)
        {
            var symbol = new CIMPointSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMVectorMarker
                    {
                        Size = baseSize,
                        ScaleSymbolsProportionally = true, // Enable proportional scaling
                        RespectFrame = true,
                        MarkerGraphics = new CIMMarkerGraphic[]
                        {
                            new CIMMarkerGraphic
                            {
                                Geometry = CreateCircleGeometry(baseSize / 2),
                                Symbol = CreateSimpleCircleSymbol()
                            }
                        }
                    }
                }
            };

            return symbol;
        }

        private static CIMSymbol CreateDefaultLineSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.5,
                        Color = CIMColor.CreateRGBColor(100, 100, 100)
                    }
                }
            };
        }

        private static CIMSymbol CreateDefaultPolygonSymbol()
        {
            return new CIMPolygonSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidFill { Color = CIMColor.CreateRGBColor(200, 200, 200, 50) },
                    new CIMSolidStroke 
                    { 
                        Width = 0.8, 
                        Color = CIMColor.CreateRGBColor(100, 100, 100) 
                    }
                }
            };
        }

        // Theme-specific symbol creators
        private static CIMSymbol CreatePlacePointSymbol()
        {
            return new CIMPointSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMVectorMarker
                    {
                        Size = 6, // Base size for places (larger than default)
                        ScaleSymbolsProportionally = true,
                        RespectFrame = true,
                        MarkerGraphics = new CIMMarkerGraphic[]
                        {
                            new CIMMarkerGraphic
                            {
                                Geometry = CreateCircleGeometry(3),
                                Symbol = new CIMPolygonSymbol
                                {
                                    SymbolLayers = new CIMSymbolLayer[]
                                    {
                                        new CIMSolidFill { Color = CIMColor.CreateRGBColor(255, 127, 0) } // Orange for places
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }

        private static CIMSymbol CreateAddressPointSymbol()
        {
            return new CIMPointSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMVectorMarker
                    {
                        Size = 2, // Small base size for addresses
                        ScaleSymbolsProportionally = true,
                        RespectFrame = true,
                        MarkerGraphics = new CIMMarkerGraphic[]
                        {
                            new CIMMarkerGraphic
                            {
                                Geometry = CreateCircleGeometry(1),
                                Symbol = new CIMPolygonSymbol
                                {
                                    SymbolLayers = new CIMSymbolLayer[]
                                    {
                                        new CIMSolidFill { Color = CIMColor.CreateRGBColor(128, 128, 128) } // Gray for addresses
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }
        private static CIMSymbol CreateTransportationLineSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.0, // Thinner lines for better readability
                        Color = CIMColor.CreateRGBColor(70, 70, 70) // Dark gray for roads
                    }
                }
            };
        }

        private static CIMSymbol CreateDivisionBoundarySymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 0.8, // Thin lines for boundaries
                        Color = CIMColor.CreateRGBColor(150, 150, 150) // Light gray for boundaries
                    }
                }
            };
        }
        private static CIMSymbol CreateBuildingPolygonSymbol()
        {
            return new CIMPolygonSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidFill { Color = CIMColor.CreateRGBColor(220, 220, 220, 120) }, // Light gray with transparency
                    new CIMSolidStroke 
                    { 
                        Width = 0.4, 
                        Color = CIMColor.CreateRGBColor(180, 180, 180) 
                    }
                }
            };
        }
        private static CIMSymbol CreateDivisionAreaSymbol() => CreateDefaultPolygonSymbol();
        
        private static CIMSymbol CreateBasePolygonSymbol(string type)
        {
            return type?.ToLower() switch
            {
                "water" => CreateWaterPolygonSymbol(),
                "land" => CreateLandPolygonSymbol(),
                _ => CreateDefaultPolygonSymbol()
            };
        }

        private static CIMSymbol CreateWaterPolygonSymbol()
        {
            return new CIMPolygonSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidFill { Color = CIMColor.CreateRGBColor(151, 219, 242) }
                }
            };
        }

        private static CIMSymbol CreateLandPolygonSymbol()
        {
            return new CIMPolygonSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidFill { Color = CIMColor.CreateRGBColor(223, 242, 186) }
                }
            };
        }

        #endregion

        #region Symbol Dictionary Helpers

        private static Dictionary<string, CIMSymbol> GetBuildingSymbols()
        {
            return new Dictionary<string, CIMSymbol>
            {
                ["residential"] = CreateResidentialBuildingSymbol(),
                ["commercial"] = CreateCommercialBuildingSymbol(),
                ["industrial"] = CreateIndustrialBuildingSymbol()
            };
        }

        private static Dictionary<string, CIMSymbol> GetPlaceSymbols()
        {
            return new Dictionary<string, CIMSymbol>
            {
                ["retail"] = CreateRetailSymbol(),
                ["food_and_drink"] = CreateFoodSymbol(),
                ["accommodation"] = CreateAccommodationSymbol()
            };
        }

        private static Dictionary<string, CIMSymbol> GetDivisionSymbols()
        {
            return new Dictionary<string, CIMSymbol>
            {
                ["country"] = CreateCountryBoundarySymbol(),
                ["region"] = CreateRegionBoundarySymbol(),
                ["locality"] = CreateLocalityBoundarySymbol()
            };
        }

        // Placeholder methods for specific symbol creation
        private static CIMSymbol CreateDefaultTransportationSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.0,
                        Color = CIMColor.CreateRGBColor(128, 128, 128) // Neutral gray for unknown types
                    }
                }
            };
        }

        // Major Highway and Motorway Symbols - Thick, prominent styling
        private static CIMSymbol CreateMotorwaySymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    // Outer casing
                    new CIMSolidStroke
                    {
                        Width = 6.0,
                        Color = CIMColor.CreateRGBColor(80, 80, 80),
                        CapStyle = LineCapStyle.Butt
                    },
                    // Inner fill
                    new CIMSolidStroke
                    {
                        Width = 4.5,
                        Color = CIMColor.CreateRGBColor(255, 140, 0),
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreateMotorwayLinkSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 4.0,
                        Color = CIMColor.CreateRGBColor(80, 80, 80),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 2.5,
                        Color = CIMColor.CreateRGBColor(255, 165, 0), // Lighter orange for links
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreateTrunkRoadSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 5.0,
                        Color = CIMColor.CreateRGBColor(80, 80, 80),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 3.5,
                        Color = CIMColor.CreateRGBColor(255, 215, 0), // Gold for trunk roads
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreateTrunkLinkSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 3.5,
                        Color = CIMColor.CreateRGBColor(80, 80, 80),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 2.0,
                        Color = CIMColor.CreateRGBColor(255, 230, 80),
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        // Primary Road Symbols - Medium prominence
        private static CIMSymbol CreatePrimaryRoadSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 4.0,
                        Color = CIMColor.CreateRGBColor(70, 70, 70),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 2.8,
                        Color = CIMColor.CreateRGBColor(255, 255, 255), // White for primary roads
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreatePrimaryLinkSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 3.0,
                        Color = CIMColor.CreateRGBColor(70, 70, 70),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 1.8,
                        Color = CIMColor.CreateRGBColor(248, 248, 248),
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreateSecondaryRoadSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 3.5,
                        Color = CIMColor.CreateRGBColor(70, 70, 70),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 2.3,
                        Color = CIMColor.CreateRGBColor(255, 255, 0), // Yellow for secondary roads
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreateSecondaryLinkSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.5,
                        Color = CIMColor.CreateRGBColor(70, 70, 70),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 1.5,
                        Color = CIMColor.CreateRGBColor(255, 255, 120),
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreateTertiaryRoadSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 3.0,
                        Color = CIMColor.CreateRGBColor(60, 60, 60),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 2.0,
                        Color = CIMColor.CreateRGBColor(255, 255, 255), // White for tertiary
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        private static CIMSymbol CreateTertiaryLinkSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.2,
                        Color = CIMColor.CreateRGBColor(60, 60, 60),
                        CapStyle = LineCapStyle.Butt
                    },
                    new CIMSolidStroke
                    {
                        Width = 1.3,
                        Color = CIMColor.CreateRGBColor(248, 248, 248),
                        CapStyle = LineCapStyle.Butt
                    }
                }
            };
        }

        // Local Road Symbols - Thinner, more subtle
        private static CIMSymbol CreateResidentialRoadSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.5,
                        Color = CIMColor.CreateRGBColor(255, 255, 255) // Simple white for residential
                    }
                }
            };
        }

        private static CIMSymbol CreateUnclassifiedRoadSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.0,
                        Color = CIMColor.CreateRGBColor(240, 240, 240) // Light gray
                    }
                }
            };
        }

        private static CIMSymbol CreateServiceRoadSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.5,
                        Color = CIMColor.CreateRGBColor(220, 220, 220) // Lighter gray for service roads
                    }
                }
            };
        }

        private static CIMSymbol CreateLivingStreetSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.0,
                        Color = CIMColor.CreateRGBColor(230, 230, 250) // Lavender for living streets
                    }
                }
            };
        }

        // Pedestrian and Cycling Infrastructure - Distinctive patterns
        private static CIMSymbol CreatePedestrianSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 3.0,
                        Color = CIMColor.CreateRGBColor(180, 180, 180) // Light gray for pedestrian areas
                    }
                }
            };
        }

        private static CIMSymbol CreateFootwaySymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.5,
                        Color = CIMColor.CreateRGBColor(139, 69, 19) // Brown for footways
                    }
                }
            };
        }

        private static CIMSymbol CreateCyclewaySymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.0,
                        Color = CIMColor.CreateRGBColor(34, 139, 34) // Forest green for bike paths
                    }
                }
            };
        }

        private static CIMSymbol CreatePathSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.2,
                        Color = CIMColor.CreateRGBColor(160, 82, 45) // Saddle brown for paths
                    }
                }
            };
        }

        private static CIMSymbol CreateStepsSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.0,
                        Color = CIMColor.CreateRGBColor(105, 105, 105) // Dim gray for steps
                    }
                }
            };
        }

        private static CIMSymbol CreateTrackSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.5,
                        Color = CIMColor.CreateRGBColor(210, 180, 140) // Tan for tracks
                    }
                }
            };
        }

        // Rail Infrastructure - Railway-specific styling
        private static CIMSymbol CreateRailwaySymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    // Railway bed
                    new CIMSolidStroke
                    {
                        Width = 3.0,
                        Color = CIMColor.CreateRGBColor(128, 128, 128) // Gray bed
                    },
                    // Rails on top
                    new CIMSolidStroke
                    {
                        Width = 1.5,
                        Color = CIMColor.CreateRGBColor(50, 50, 50) // Dark rails
                    }
                }
            };
        }

        private static CIMSymbol CreateSubwaySymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.5,
                        Color = CIMColor.CreateRGBColor(0, 100, 200) // Blue for subway
                    }
                }
            };
        }

        private static CIMSymbol CreateLightRailSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.0,
                        Color = CIMColor.CreateRGBColor(0, 150, 100) // Teal for light rail
                    }
                }
            };
        }

        private static CIMSymbol CreateTramSymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 1.8,
                        Color = CIMColor.CreateRGBColor(255, 20, 147) // Deep pink for trams
                    }
                }
            };
        }

        // Water Transportation
        private static CIMSymbol CreateFerrySymbol()
        {
            return new CIMLineSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidStroke
                    {
                        Width = 2.0,
                        Color = CIMColor.CreateRGBColor(0, 191, 255) // Deep sky blue for ferry routes
                    }
                }
            };
        }

        // Building symbol methods
        private static CIMSymbol CreateResidentialBuildingSymbol() => CreateDefaultPolygonSymbol();
        private static CIMSymbol CreateCommercialBuildingSymbol() => CreateDefaultPolygonSymbol();
        private static CIMSymbol CreateIndustrialBuildingSymbol() => CreateDefaultPolygonSymbol();
        
        // Place symbol methods
        private static CIMSymbol CreateRetailSymbol() => CreateDefaultPointSymbol();
        private static CIMSymbol CreateFoodSymbol() => CreateDefaultPointSymbol();
        private static CIMSymbol CreateAccommodationSymbol() => CreateDefaultPointSymbol();
        
        // Division boundary symbol methods
        private static CIMSymbol CreateCountryBoundarySymbol() => CreateDefaultLineSymbol();
        private static CIMSymbol CreateRegionBoundarySymbol() => CreateDefaultLineSymbol();
        private static CIMSymbol CreateLocalityBoundarySymbol() => CreateDefaultLineSymbol();

        // Renderer methods
        private static CIMRenderer CreateWaterRenderer() => CreateSimpleRenderer(CreateWaterPolygonSymbol());
        private static CIMRenderer CreateLandRenderer() => CreateSimpleRenderer(CreateLandPolygonSymbol());
        private static CIMRenderer CreateInfrastructureRenderer() => CreateSimpleRenderer(CreateDefaultPolygonSymbol());

        #endregion

        #region Utility Methods

        private static Geometry CreateCircleGeometry(double radius)
        {
            // Create a simple circle geometry for point symbols using GeometryEngine
            var centerPoint = MapPointBuilderEx.CreateMapPoint(0, 0);
            var circle = GeometryEngine.Instance.Buffer(centerPoint, radius) as Polygon;
            return circle;
        }

        private static CIMSymbol CreateSimpleCircleSymbol()
        {
            return new CIMPolygonSymbol
            {
                SymbolLayers = new CIMSymbolLayer[]
                {
                    new CIMSolidFill { Color = CIMColor.CreateRGBColor(55, 126, 184) }
                }
            };
        }

        #endregion
    }
} 