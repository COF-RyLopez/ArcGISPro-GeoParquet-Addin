using System.Collections.Generic;
using System.Linq;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Static catalog of all available map styles.
    /// Color values are extracted from Esri Open Basemap vector tile style JSON
    /// (Mapbox GL Style Spec v8) for the corresponding basemap themes.
    /// </summary>
    public static class MapStyleCatalog
    {
        public static List<MapStylePack> StylePacks { get; } = new()
        {
            CreateOpenBasemapPack(),
            CreateExperimentalPack(),
        };

        public static List<MapStyleDefinition> AllStyles { get; } = StylePacks
            .SelectMany(pack => pack.Styles)
            .ToList();

        private static MapStylePack CreateOpenBasemapPack()
        {
            var pack = new MapStylePack
            {
                Id = "esri_open_basemap",
                DisplayName = "Esri Open Basemap",
                Description = "Core basemap-aligned cartography derived from Esri Open Basemap themes.",
                Category = "Official",
                IsExperimental = false,
                Styles = new List<MapStyleDefinition>
                {
                    CreateStreetsStyle(),
                    CreateStreetsNightStyle(),
                    CreateLightGrayCanvasStyle(),
                }
            };

            ApplyPackMetadata(pack);
            return pack;
        }

        private static MapStylePack CreateExperimentalPack()
        {
            var pack = new MapStylePack
            {
                Id = "community_experimental",
                DisplayName = "Community Experimental",
                Description = "Artist-inspired and in-progress style experiments.",
                Category = "Experimental",
                IsExperimental = true,
                Styles = new List<MapStyleDefinition>
                {
                    CreateNewAmsterdamStylxStyle(),
                }
            };

            ApplyPackMetadata(pack);
            return pack;
        }

        private static MapStyleDefinition CreateStreetsStyle()
        {
            var roadClassColors = new Dictionary<string, string>
            {
                { "motorway", "#FFC64E" },
                { "trunk", "#FFC64E" },
                { "primary", "#FFEAB2" },
                { "secondary", "#FFFFFF" },
                { "tertiary", "#FFFFFF" },
                { "residential", "#FFFFFF" },
                { "service", "#F0F0F0" },
                { "living_street", "#FFFFFF" },
                { "pedestrian", "#F0EDE2" },
                { "track", "#E8E0CC" },
                { "unclassified", "#FFFFFF" },
            };
            var roadClassWidths = new Dictionary<string, double>
            {
                { "motorway", 2.5 },
                { "trunk", 2.2 },
                { "primary", 2.0 },
                { "secondary", 1.6 },
                { "tertiary", 1.2 },
                { "residential", 0.8 },
                { "service", 0.6 },
                { "living_street", 0.8 },
                { "pedestrian", 0.6 },
                { "track", 0.5 },
                { "unclassified", 0.8 },
            };

            return new MapStyleDefinition
            {
                Id = "streets",
                DisplayName = "Streets",
                Description = "Classic light street map emphasizing the road network and urban landscape.",
                ThumbnailResourceKey = "StyleThumb_Streets",
                ThumbnailUri = "pack://application:,,,/DuckDBGeoparquet;component/Images/Styles/streets_thumb.png",

                LandFillColor = "#f7f6d5",
                LandOutlineColor = "#E8E0CC",

                ParkFillColor = "#d2e4bf",
                ResidentialFillColor = "#F0EDE2",
                CommercialFillColor = "#EDE3D4",
                IndustrialFillColor = "#E4DDD0",
                EducationFillColor = "#E8DED0",
                HospitalFillColor = "#F2E1DB",
                CemeteryFillColor = "#D6DFC1",
                MilitaryFillColor = "#E4DFCB",

                ForestFillColor = "#C8E0AB",
                GrassFillColor = "#d7e6c1",
                CroplandFillColor = "#E3E2C2",

                WaterFillColor = "#B0D3E8",
                WaterOutlineColor = "#7EB8D4",
                WaterLineColor = "#7EB8D4",

                BuildingFillColor = "#FAFAE8",
                BuildingOutlineColor = "#D9D1B8",

                MotorwayColor = "#FFC64E",
                MotorwayWidth = 2.5,
                PrimaryRoadColor = "#FFEAB2",
                PrimaryRoadWidth = 2.0,
                SecondaryRoadColor = "#FFFFFF",
                SecondaryRoadWidth = 1.6,
                TertiaryRoadColor = "#FFFFFF",
                TertiaryRoadWidth = 1.2,
                ResidentialRoadColor = "#FFFFFF",
                ResidentialRoadWidth = 0.8,
                RoadCasingColor = "#CCCCCC",
                RailColor = "#A0A0A0",

                ConnectorColor = "#888888",
                ConnectorSize = 3.0,

                PlaceDefaultColor = "#6B4226",
                PlaceDefaultSize = 5.0,

                BoundaryLineColor = "#9E9E9E",
                BoundaryLineWidth = 1.0,
                DivisionFillColor = "#F5F5F0",

                InfrastructureFillColor = "#E0DDD4",
                InfrastructureLineColor = "#C0BEB5",

                BathymetryFillColor = "#8CC1D9",

                AddressPointColor = "#7A7A7A",
                AddressPointSize = 3.0,

                RoadClassColors = roadClassColors,
                RoadClassWidths = roadClassWidths,
            };
        }

        private static MapStyleDefinition CreateStreetsNightStyle()
        {
            var roadClassColors = new Dictionary<string, string>
            {
                { "motorway", "#D4A843" },
                { "trunk", "#D4A843" },
                { "primary", "#6B6B50" },
                { "secondary", "#4A4A56" },
                { "tertiary", "#3E3E4A" },
                { "residential", "#363642" },
                { "service", "#30303C" },
                { "living_street", "#363642" },
                { "pedestrian", "#3A3A46" },
                { "track", "#333340" },
                { "unclassified", "#363642" },
            };
            var roadClassWidths = new Dictionary<string, double>
            {
                { "motorway", 2.5 },
                { "trunk", 2.2 },
                { "primary", 2.0 },
                { "secondary", 1.6 },
                { "tertiary", 1.2 },
                { "residential", 0.8 },
                { "service", 0.6 },
                { "living_street", 0.8 },
                { "pedestrian", 0.6 },
                { "track", 0.5 },
                { "unclassified", 0.8 },
            };

            return new MapStyleDefinition
            {
                Id = "streets_night",
                DisplayName = "Streets Night",
                Description = "Dark theme street map for night mode or dark backgrounds.",
                ThumbnailResourceKey = "StyleThumb_StreetsNight",
                ThumbnailUri = "pack://application:,,,/DuckDBGeoparquet;component/Images/Styles/streets_night_thumb.png",

                LandFillColor = "#2a2a28",
                LandOutlineColor = "#3A3A38",

                ParkFillColor = "#2A3325",
                ResidentialFillColor = "#2E2E2C",
                CommercialFillColor = "#302E2A",
                IndustrialFillColor = "#2E2C28",
                EducationFillColor = "#302E28",
                HospitalFillColor = "#352A28",
                CemeteryFillColor = "#2A302A",
                MilitaryFillColor = "#2E2E28",

                ForestFillColor = "#243020",
                GrassFillColor = "#2A3325",
                CroplandFillColor = "#2E2E22",

                WaterFillColor = "#000F1A",
                WaterOutlineColor = "#0A2030",
                WaterLineColor = "#0A2030",

                BuildingFillColor = "#373845",
                BuildingOutlineColor = "#515461",

                MotorwayColor = "#D4A843",
                MotorwayWidth = 2.5,
                PrimaryRoadColor = "#6B6B50",
                PrimaryRoadWidth = 2.0,
                SecondaryRoadColor = "#4A4A56",
                SecondaryRoadWidth = 1.6,
                TertiaryRoadColor = "#3E3E4A",
                TertiaryRoadWidth = 1.2,
                ResidentialRoadColor = "#363642",
                ResidentialRoadWidth = 0.8,
                RoadCasingColor = "#1E1E2A",
                RailColor = "#505060",

                ConnectorColor = "#555566",
                ConnectorSize = 3.0,

                PlaceDefaultColor = "#C8B88A",
                PlaceDefaultSize = 5.0,

                BoundaryLineColor = "#555555",
                BoundaryLineWidth = 1.0,
                DivisionFillColor = "#2C2C2A",

                InfrastructureFillColor = "#2E2E30",
                InfrastructureLineColor = "#404045",

                BathymetryFillColor = "#001525",

                AddressPointColor = "#888899",
                AddressPointSize = 3.0,

                RoadClassColors = roadClassColors,
                RoadClassWidths = roadClassWidths,
            };
        }

        private static MapStyleDefinition CreateLightGrayCanvasStyle()
        {
            var roadClassColors = new Dictionary<string, string>
            {
                { "motorway", "#F0F0F0" },
                { "trunk", "#F0F0F0" },
                { "primary", "#F5F5F5" },
                { "secondary", "#F8F8F8" },
                { "tertiary", "#FAFAFA" },
                { "residential", "#FCFCFC" },
                { "service", "#FEFEFE" },
                { "living_street", "#FCFCFC" },
                { "pedestrian", "#FAFAFA" },
                { "track", "#F5F5F5" },
                { "unclassified", "#FCFCFC" },
            };
            var roadClassWidths = new Dictionary<string, double>
            {
                { "motorway", 2.0 },
                { "trunk", 1.8 },
                { "primary", 1.5 },
                { "secondary", 1.2 },
                { "tertiary", 0.8 },
                { "residential", 0.5 },
                { "service", 0.4 },
                { "living_street", 0.5 },
                { "pedestrian", 0.4 },
                { "track", 0.3 },
                { "unclassified", 0.5 },
            };

            return new MapStyleDefinition
            {
                Id = "light_gray_canvas",
                DisplayName = "Light Gray Canvas",
                Description = "Minimal light gray canvas for data overlay with subtle basemap context.",
                ThumbnailResourceKey = "StyleThumb_LightGrayCanvas",
                ThumbnailUri = "pack://application:,,,/DuckDBGeoparquet;component/Images/Styles/light_gray_canvas_thumb.png",

                LandFillColor = "#ececec",
                LandOutlineColor = "#DFDFDF",

                ParkFillColor = "#E2E8E0",
                ResidentialFillColor = "#EAEAEA",
                CommercialFillColor = "#E6E6E6",
                IndustrialFillColor = "#E4E4E4",
                EducationFillColor = "#E8E8E8",
                HospitalFillColor = "#EBEBEB",
                CemeteryFillColor = "#E5E8E4",
                MilitaryFillColor = "#E6E6E4",

                ForestFillColor = "#DDE5DA",
                GrassFillColor = "#E2E8E0",
                CroplandFillColor = "#E8E8E2",

                WaterFillColor = "#cacecf",
                WaterOutlineColor = "#B8C0C2",
                WaterLineColor = "#B8C0C2",

                BuildingFillColor = "#dbddde",
                BuildingOutlineColor = "#efefef",

                MotorwayColor = "#F0F0F0",
                MotorwayWidth = 2.0,
                PrimaryRoadColor = "#F5F5F5",
                PrimaryRoadWidth = 1.5,
                SecondaryRoadColor = "#F8F8F8",
                SecondaryRoadWidth = 1.2,
                TertiaryRoadColor = "#FAFAFA",
                TertiaryRoadWidth = 0.8,
                ResidentialRoadColor = "#FCFCFC",
                ResidentialRoadWidth = 0.5,
                RoadCasingColor = "#D5D5D5",
                RailColor = "#C0C0C0",

                ConnectorColor = "#AAAAAA",
                ConnectorSize = 2.5,

                PlaceDefaultColor = "#808080",
                PlaceDefaultSize = 4.0,

                BoundaryLineColor = "#B0B0B0",
                BoundaryLineWidth = 0.8,
                DivisionFillColor = "#E8E8E8",

                InfrastructureFillColor = "#E0E0E0",
                InfrastructureLineColor = "#D0D0D0",

                BathymetryFillColor = "#B8C4C8",

                AddressPointColor = "#A0A0A0",
                AddressPointSize = 2.5,

                RoadClassColors = roadClassColors,
                RoadClassWidths = roadClassWidths,
            };
        }

        private static MapStyleDefinition CreateNewAmsterdamStylxStyle()
        {
            // Keep the existing Streets palette as a fallback when a style item cannot be resolved
            // from the .stylx source.
            var style = CreateStreetsStyle();
            style.Id = "new_amsterdam_stylx";
            style.DisplayName = "New Amsterdam";
            style.Description = "Stylx-based cartography inspired by John Nelson's New Amsterdam style.";
            style.ThumbnailResourceKey = "StyleThumb_NewAmsterdam";
            style.ThumbnailUri = "pack://application:,,,/DuckDBGeoparquet;component/Images/Styles/streets_thumb.png";

            style.UseStylxSymbols = true;
            style.StylxPath = "NewAmsterdam.stylx";

            style.StylxTypeGeometrySymbolKeys = new Dictionary<string, string>
            {
                { "place:point", "Place" },
                // Force fallback to palette markers for ultra-dense point themes.
                { "address:point", "" },
                { "connector:point", "" },
                { "infrastructure:point", "" },
                { "land:point", "" },
                { "division:point", "" },
                { "division_boundary:line", "Boundary" },
                // Performance safe mode: use palette fallback for dense polygon themes by default.
                { "water:polygon", "" },
                { "building:polygon", "" },
                { "building_part:polygon", "" },
                { "division_area:polygon", "" },
                { "land_use:polygon", "" },
                { "land_cover:polygon", "" },
                { "bathymetry:polygon", "" },
                { "land:polygon", "" },
                { "infrastructure:polygon", "" },
            };

            style.StylxSegmentClassSymbolKeys = new Dictionary<string, string>();

            // No default stylx symbol fallback; use stylx only for explicit type/geometry mappings above.
            style.StylxDefaultPointSymbolKey = null;
            style.StylxDefaultLineSymbolKey = null;
            style.StylxDefaultPolygonSymbolKey = null;

            // Tone down dense point layers in this theme.
            style.AddressPointColor = "#6E6E6E";
            style.AddressPointSize = 1.6;
            style.ConnectorColor = "#6B6B6B";
            style.ConnectorSize = 1.8;
            style.PlaceDefaultColor = "#6E5842";
            style.PlaceDefaultSize = 3.0;

            return style;
        }

        private static void ApplyPackMetadata(MapStylePack pack)
        {
            if (pack?.Styles == null)
                return;

            foreach (var style in pack.Styles)
            {
                if (style == null)
                    continue;

                style.StylePackId = pack.Id;
                style.StylePackName = pack.DisplayName;
                style.StyleCategory = pack.Category;
                style.IsExperimental = pack.IsExperimental;
                style.DrawOrderRanks ??= CreateDefaultDrawOrderRanks();
            }
        }

        private static Dictionary<string, int> CreateDefaultDrawOrderRanks()
        {
            // Higher rank draws above lower rank.
            // This default favors operational readability:
            // points and transportation above context polygons, with buildings above base fills.
            return new Dictionary<string, int>(System.StringComparer.OrdinalIgnoreCase)
            {
                // Point overlays
                { "place:point", 130 },
                { "division:point", 126 },
                { "address:point", 118 },
                { "connector:point", 114 },
                { "infrastructure:point", 112 },
                { "land:point", 108 },
                { "water:point", 106 },

                // Linear networks and boundaries
                { "segment:line", 104 },
                { "division_boundary:line", 100 },
                { "infrastructure:line", 96 },
                { "water:line", 94 },
                { "land_use:line", 92 },
                { "land:line", 90 },

                // Polygonal overlays
                { "building:polygon", 86 },
                { "building_part:polygon", 84 },
                { "infrastructure:polygon", 78 },
                { "division_area:polygon", 76 },
                { "division:polygon", 74 },
                { "land_use:polygon", 72 },
                { "land_cover:polygon", 68 },
                { "land:polygon", 64 },
                { "bathymetry:polygon", 60 },
                { "water:polygon", 56 },

                // Type-only fallbacks
                { "building", 86 },
                { "building_part", 84 },
                { "segment", 104 },
                { "address", 118 },
                { "place", 130 },
                { "bathymetry", 60 },
                { "water", 56 },
            };
        }
    }
}
