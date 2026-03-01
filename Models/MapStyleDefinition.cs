using System.Collections.Generic;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Defines the color palette and metadata for a single map style theme.
    /// Colors are stored as hex RGB strings (e.g., "#FAFAE8") matching the
    /// Esri Open Basemap vector tile style JSON values.
    /// </summary>
    public class MapStyleDefinition
    {
        public string Id { get; set; }
        public string DisplayName { get; set; }
        public string Description { get; set; }
        public string ThumbnailResourceKey { get; set; }

        /// <summary>
        /// Relative pack URI to the thumbnail image for XAML binding.
        /// </summary>
        public string ThumbnailUri { get; set; }

        // Land / background
        public string LandFillColor { get; set; }
        public string LandOutlineColor { get; set; }

        // Land use categories
        public string ParkFillColor { get; set; }
        public string ResidentialFillColor { get; set; }
        public string CommercialFillColor { get; set; }
        public string IndustrialFillColor { get; set; }
        public string EducationFillColor { get; set; }
        public string HospitalFillColor { get; set; }
        public string CemeteryFillColor { get; set; }
        public string MilitaryFillColor { get; set; }

        // Land cover
        public string ForestFillColor { get; set; }
        public string GrassFillColor { get; set; }
        public string CroplandFillColor { get; set; }

        // Water
        public string WaterFillColor { get; set; }
        public string WaterOutlineColor { get; set; }
        public string WaterLineColor { get; set; }

        // Buildings
        public string BuildingFillColor { get; set; }
        public string BuildingOutlineColor { get; set; }

        // Roads / transportation segments
        public string MotorwayColor { get; set; }
        public double MotorwayWidth { get; set; }
        public string PrimaryRoadColor { get; set; }
        public double PrimaryRoadWidth { get; set; }
        public string SecondaryRoadColor { get; set; }
        public double SecondaryRoadWidth { get; set; }
        public string TertiaryRoadColor { get; set; }
        public double TertiaryRoadWidth { get; set; }
        public string ResidentialRoadColor { get; set; }
        public double ResidentialRoadWidth { get; set; }
        public string RoadCasingColor { get; set; }
        public string RailColor { get; set; }

        // Connectors
        public string ConnectorColor { get; set; }
        public double ConnectorSize { get; set; }

        // Places (POIs)
        public string PlaceDefaultColor { get; set; }
        public double PlaceDefaultSize { get; set; }

        // Divisions / boundaries
        public string BoundaryLineColor { get; set; }
        public double BoundaryLineWidth { get; set; }
        public string DivisionFillColor { get; set; }

        // Infrastructure
        public string InfrastructureFillColor { get; set; }
        public string InfrastructureLineColor { get; set; }

        // Bathymetry
        public string BathymetryFillColor { get; set; }

        // Addresses
        public string AddressPointColor { get; set; }
        public double AddressPointSize { get; set; }

        /// <summary>
        /// Road class-to-color mapping for CIMUniqueValueRenderer on the "class" field.
        /// Keys are Overture segment class values; values are hex color strings.
        /// </summary>
        public Dictionary<string, string> RoadClassColors { get; set; }

        /// <summary>
        /// Road class-to-width mapping for line widths per road class.
        /// </summary>
        public Dictionary<string, double> RoadClassWidths { get; set; }
    }
}
