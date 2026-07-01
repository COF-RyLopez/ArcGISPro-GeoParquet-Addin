using System;
using System.Collections.Generic;
using System.Linq;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Built-in user-facing map purpose profiles.
    /// </summary>
    public static class CartographicProfileCatalog
    {
        private const string ThumbnailBaseUri = "pack://application:,,,/DuckDBGeoparquet;component/Images/Styles/";

        public static List<CartographicProfile> AllProfiles { get; } = new()
        {
            CreateAnalystCanvasProfile(),
            CreateMobilityFocusProfile(),
            CreateUrbanFormProfile(),
            CreateEnvironmentProfile(),
        };

        public static CartographicProfile GetById(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return null;

            return AllProfiles.FirstOrDefault(profile =>
                string.Equals(profile.Id, id, StringComparison.OrdinalIgnoreCase));
        }

        private static CartographicProfile CreateAnalystCanvasProfile()
        {
            var style = CreateProfileStyle(
                baseStyleId: "light_gray_canvas",
                id: "profile_analyst_canvas",
                displayName: "Analyst Canvas",
                description: "Quiet reference cartography for analysis, overlays, and exploration.",
                thumbnailUri: $"{ThumbnailBaseUri}analyst_canvas_thumb.png");

            style.LandFillColor = "#F4F2ED";
            style.LandOutlineColor = "#E4E1DA";
            style.ParkFillColor = "#E4ECDD";
            style.ForestFillColor = "#DCE8D4";
            style.GrassFillColor = "#E6EEDF";
            style.WaterFillColor = "#C7DCE8";
            style.WaterOutlineColor = "#B8CCD6";
            style.WaterLineColor = "#B8CCD6";
            style.BuildingFillColor = "#DAD7CF";
            style.BuildingOutlineColor = "#F2F0EA";
            style.PlaceDefaultColor = "#777777";
            style.PlaceDefaultSize = 3.0;
            style.AddressPointColor = "#9A9A9A";
            style.AddressPointSize = 1.8;
            style.LabelColor = "#4B4B4B";
            style.DivisionLabelColor = "#6B6B6B";
            style.LabelSize = 8.5;
            style.RoadCasingColor = "#D6D6D6";
            style.RailColor = "#B8B8B8";
            SetRoads(
                style,
                motorway: ("#D7D7D7", 1.8),
                trunk: ("#DADADA", 1.6),
                primary: ("#E2E2E2", 1.3),
                secondary: ("#EFEFEF", 1.0),
                tertiary: ("#F4F4F4", 0.7),
                residential: ("#FFFFFF", 0.45),
                service: ("#FFFFFF", 0.35),
                pedestrian: ("#F7F7F7", 0.3),
                track: ("#EFEFEF", 0.3),
                unclassified: ("#FFFFFF", 0.45));

            SetRanks(style, new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                { "place:point", 124 },
                { "address:point", 110 },
                { "connector:point", 108 },
                { "segment:line", 100 },
                { "building:polygon", 74 },
                { "building_part:polygon", 72 },
                { "land_use:polygon", 66 },
                { "land_cover:polygon", 64 },
                { "water:polygon", 58 },
            });

            return new CartographicProfile
            {
                Id = "analyst_canvas",
                DisplayName = "Analyst Canvas",
                Description = "Subdued cartography designed to keep user overlays and analysis layers in front.",
                Category = "Analysis",
                ThumbnailUri = style.ThumbnailUri,
                BaseStyleId = "light_gray_canvas",
                Style = style,
                EnabledLabelTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "segment", "division" },
                ShowLocalRoadLabels = false,
                PointMinScales = CreatePointScaleMap(address: 4000, connector: 9000, place: 12000, division: 60000, infrastructure: 18000, land: 25000),
            };
        }

        private static CartographicProfile CreateMobilityFocusProfile()
        {
            var style = CreateProfileStyle(
                baseStyleId: "streets",
                id: "profile_mobility_focus",
                displayName: "Mobility Focus",
                description: "Transportation-first cartography with stronger road hierarchy and labels.",
                thumbnailUri: $"{ThumbnailBaseUri}mobility_focus_thumb.png");

            style.LandFillColor = "#F5F1E7";
            style.LandOutlineColor = "#E2DCCE";
            style.BuildingFillColor = "#E2DFD6";
            style.BuildingOutlineColor = "#CDC8BC";
            style.WaterFillColor = "#B9D7EA";
            style.WaterOutlineColor = "#8FBBD4";
            style.PlaceDefaultColor = "#5E5E5E";
            style.PlaceDefaultSize = 3.6;
            style.ConnectorColor = "#2F6F9F";
            style.ConnectorSize = 3.4;
            style.LabelColor = "#2F2F2F";
            style.DivisionLabelColor = "#5E5E5E";
            style.LabelSize = 9.2;
            style.RoadCasingColor = "#A9A39A";
            style.RailColor = "#595959";
            SetRoads(
                style,
                motorway: ("#F49B22", 3.4),
                trunk: ("#F2AE3F", 3.0),
                primary: ("#FFD166", 2.5),
                secondary: ("#FFF2B8", 1.9),
                tertiary: ("#FFFFFF", 1.45),
                residential: ("#FFFFFF", 0.9),
                service: ("#F4F1EA", 0.55),
                pedestrian: ("#E9E1D3", 0.45),
                track: ("#D8CBB6", 0.4),
                unclassified: ("#FFFFFF", 0.8));

            SetRanks(style, new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                { "segment:line", 132 },
                { "connector:point", 128 },
                { "place:point", 122 },
                { "division_boundary:line", 118 },
                { "address:point", 112 },
                { "building:polygon", 76 },
                { "building_part:polygon", 74 },
            });

            return new CartographicProfile
            {
                Id = "mobility_focus",
                DisplayName = "Mobility Focus",
                Description = "Emphasizes roads, connectors, rail, and transportation readability.",
                Category = "Mobility",
                ThumbnailUri = style.ThumbnailUri,
                BaseStyleId = "streets",
                Style = style,
                EnabledLabelTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "segment", "division" },
                ShowLocalRoadLabels = true,
                PointMinScales = CreatePointScaleMap(address: 5000, connector: 20000, place: 12000, division: 70000, infrastructure: 26000, land: 22000),
            };
        }

        private static CartographicProfile CreateUrbanFormProfile()
        {
            var style = CreateProfileStyle(
                baseStyleId: "streets",
                id: "profile_urban_form",
                displayName: "Urban Form",
                description: "Built-environment cartography for buildings, blocks, and local structure.",
                thumbnailUri: $"{ThumbnailBaseUri}urban_form_thumb.png");

            style.LandFillColor = "#ECE8DE";
            style.LandOutlineColor = "#D8D2C6";
            style.ParkFillColor = "#D7E5C8";
            style.ForestFillColor = "#C8DDB5";
            style.GrassFillColor = "#DCE9CE";
            style.WaterFillColor = "#C5DAE5";
            style.WaterOutlineColor = "#9FBFCE";
            style.BuildingFillColor = "#6F6C64";
            style.BuildingOutlineColor = "#3E3C38";
            style.PlaceDefaultColor = "#514D45";
            style.PlaceDefaultSize = 2.8;
            style.AddressPointColor = "#5D5951";
            style.AddressPointSize = 1.8;
            style.LabelColor = "#3C3933";
            style.DivisionLabelColor = "#5F5A50";
            style.LabelSize = 8.4;
            style.RoadCasingColor = "#B8B1A6";
            style.RailColor = "#4D4A45";
            SetRoads(
                style,
                motorway: ("#F4C46B", 2.4),
                trunk: ("#F4C46B", 2.2),
                primary: ("#FFFFFF", 1.8),
                secondary: ("#FFFFFF", 1.4),
                tertiary: ("#FFFFFF", 1.15),
                residential: ("#FFFFFF", 0.9),
                service: ("#EFECE5", 0.55),
                pedestrian: ("#E8E2D5", 0.45),
                track: ("#D2C8B6", 0.4),
                unclassified: ("#FFFFFF", 0.75));

            SetRanks(style, new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                { "segment:line", 118 },
                { "building:polygon", 112 },
                { "building_part:polygon", 110 },
                { "address:point", 108 },
                { "place:point", 104 },
                { "connector:point", 102 },
                { "division_boundary:line", 96 },
                { "land_use:polygon", 70 },
                { "land_cover:polygon", 66 },
            });

            return new CartographicProfile
            {
                Id = "urban_form",
                DisplayName = "Urban Form",
                Description = "Highlights building footprints and block texture with restrained contextual labels.",
                Category = "Urban",
                ThumbnailUri = style.ThumbnailUri,
                BaseStyleId = "streets",
                Style = style,
                EnabledLabelTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "segment" },
                ShowLocalRoadLabels = false,
                PointMinScales = CreatePointScaleMap(address: 3000, connector: 10000, place: 9000, division: 30000, infrastructure: 18000, land: 18000),
            };
        }

        private static CartographicProfile CreateEnvironmentProfile()
        {
            var style = CreateProfileStyle(
                baseStyleId: "streets",
                id: "profile_environment",
                displayName: "Environment",
                description: "Natural-feature cartography for land, water, parks, and land cover.",
                thumbnailUri: $"{ThumbnailBaseUri}environment_thumb.png");

            style.LandFillColor = "#F4F1E6";
            style.LandOutlineColor = "#DDD5C4";
            style.ParkFillColor = "#A8D08D";
            style.ResidentialFillColor = "#EEE8DB";
            style.CommercialFillColor = "#ECE3D4";
            style.IndustrialFillColor = "#E1D8CA";
            style.EducationFillColor = "#E7DEC7";
            style.HospitalFillColor = "#EEDBD2";
            style.CemeteryFillColor = "#AFC99E";
            style.MilitaryFillColor = "#DED8C6";
            style.ForestFillColor = "#73A96F";
            style.GrassFillColor = "#B9D99D";
            style.CroplandFillColor = "#D6CF83";
            style.WaterFillColor = "#6CAED6";
            style.WaterOutlineColor = "#3B87B5";
            style.WaterLineColor = "#3B87B5";
            style.BuildingFillColor = "#D8D3C8";
            style.BuildingOutlineColor = "#C3BBAE";
            style.InfrastructureFillColor = "#D2CCBF";
            style.InfrastructureLineColor = "#A9A194";
            style.BathymetryFillColor = "#579BC8";
            style.PlaceDefaultColor = "#526043";
            style.PlaceDefaultSize = 3.0;
            style.AddressPointColor = "#8D8D83";
            style.AddressPointSize = 1.8;
            style.LabelColor = "#375033";
            style.DivisionLabelColor = "#5C604A";
            style.LabelSize = 8.8;
            style.RoadCasingColor = "#C8C0B2";
            style.RailColor = "#8C8578";
            SetRoads(
                style,
                motorway: ("#D7C487", 1.9),
                trunk: ("#D7C487", 1.7),
                primary: ("#EEE5C6", 1.3),
                secondary: ("#F4EEE1", 1.0),
                tertiary: ("#F7F2E8", 0.75),
                residential: ("#FFFFFF", 0.5),
                service: ("#F3EFE7", 0.35),
                pedestrian: ("#E7DDCB", 0.35),
                track: ("#CDBD9B", 0.35),
                unclassified: ("#FFFFFF", 0.45));

            SetRanks(style, new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                { "place:point", 120 },
                { "division:point", 118 },
                { "infrastructure:point", 116 },
                { "water:line", 112 },
                { "land:line", 110 },
                { "segment:line", 98 },
                { "land_use:polygon", 92 },
                { "land_cover:polygon", 90 },
                { "water:polygon", 86 },
                { "bathymetry:polygon", 84 },
                { "building:polygon", 70 },
                { "building_part:polygon", 68 },
            });

            return new CartographicProfile
            {
                Id = "environment",
                DisplayName = "Environment",
                Description = "Promotes natural context while keeping roads and buildings secondary.",
                Category = "Environment",
                ThumbnailUri = style.ThumbnailUri,
                BaseStyleId = "streets",
                Style = style,
                EnabledLabelTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "division", "infrastructure", "land" },
                ShowLocalRoadLabels = false,
                PointMinScales = CreatePointScaleMap(address: 3000, connector: 8000, place: 9000, division: 60000, infrastructure: 26000, land: 60000),
            };
        }

        private static MapStyleDefinition CreateProfileStyle(
            string baseStyleId,
            string id,
            string displayName,
            string description,
            string thumbnailUri)
        {
            var baseStyle = MapStyleCatalog.AllStyles.FirstOrDefault(style =>
                    string.Equals(style.Id, baseStyleId, StringComparison.OrdinalIgnoreCase))
                ?? MapStyleCatalog.AllStyles.FirstOrDefault();

            var style = CloneStyle(baseStyle);
            style.Id = id;
            style.DisplayName = displayName;
            style.Description = description;
            style.StylePackId = "cartographic_profiles";
            style.StylePackName = "Cartographic Profiles";
            style.StyleCategory = "Purpose";
            style.IsExperimental = false;
            style.ThumbnailResourceKey = null;
            style.ThumbnailUri = thumbnailUri;
            return style;
        }

        private static MapStyleDefinition CloneStyle(MapStyleDefinition source)
        {
            var clone = new MapStyleDefinition();
            if (source == null)
                return clone;

            foreach (var property in typeof(MapStyleDefinition).GetProperties())
            {
                if (!property.CanRead || !property.CanWrite)
                    continue;

                property.SetValue(clone, CloneValue(property.GetValue(source)));
            }

            return clone;
        }

        private static object CloneValue(object value)
        {
            return value switch
            {
                Dictionary<string, string> stringMap => new Dictionary<string, string>(stringMap, StringComparer.OrdinalIgnoreCase),
                Dictionary<string, double> doubleMap => new Dictionary<string, double>(doubleMap, StringComparer.OrdinalIgnoreCase),
                Dictionary<string, int> intMap => new Dictionary<string, int>(intMap, StringComparer.OrdinalIgnoreCase),
                _ => value,
            };
        }

        private static void SetRoads(
            MapStyleDefinition style,
            (string Color, double Width) motorway,
            (string Color, double Width) trunk,
            (string Color, double Width) primary,
            (string Color, double Width) secondary,
            (string Color, double Width) tertiary,
            (string Color, double Width) residential,
            (string Color, double Width) service,
            (string Color, double Width) pedestrian,
            (string Color, double Width) track,
            (string Color, double Width) unclassified)
        {
            style.MotorwayColor = motorway.Color;
            style.MotorwayWidth = motorway.Width;
            style.PrimaryRoadColor = primary.Color;
            style.PrimaryRoadWidth = primary.Width;
            style.SecondaryRoadColor = secondary.Color;
            style.SecondaryRoadWidth = secondary.Width;
            style.TertiaryRoadColor = tertiary.Color;
            style.TertiaryRoadWidth = tertiary.Width;
            style.ResidentialRoadColor = residential.Color;
            style.ResidentialRoadWidth = residential.Width;

            style.RoadClassColors = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                { "motorway", motorway.Color },
                { "trunk", trunk.Color },
                { "primary", primary.Color },
                { "secondary", secondary.Color },
                { "tertiary", tertiary.Color },
                { "residential", residential.Color },
                { "service", service.Color },
                { "living_street", residential.Color },
                { "pedestrian", pedestrian.Color },
                { "track", track.Color },
                { "unclassified", unclassified.Color },
            };

            style.RoadClassWidths = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase)
            {
                { "motorway", motorway.Width },
                { "trunk", trunk.Width },
                { "primary", primary.Width },
                { "secondary", secondary.Width },
                { "tertiary", tertiary.Width },
                { "residential", residential.Width },
                { "service", service.Width },
                { "living_street", residential.Width },
                { "pedestrian", pedestrian.Width },
                { "track", track.Width },
                { "unclassified", unclassified.Width },
            };
        }

        private static void SetRanks(MapStyleDefinition style, Dictionary<string, int> ranks)
        {
            style.DrawOrderRanks ??= new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var rank in ranks)
            {
                style.DrawOrderRanks[rank.Key] = rank.Value;
            }
        }

        private static Dictionary<string, double> CreatePointScaleMap(
            double address,
            double connector,
            double place,
            double division,
            double infrastructure,
            double land)
        {
            return new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase)
            {
                { "address", address },
                { "connector", connector },
                { "place", place },
                { "division", division },
                { "infrastructure", infrastructure },
                { "land", land },
            };
        }
    }
}
