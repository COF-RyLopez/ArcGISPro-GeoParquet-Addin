namespace DuckDBGeoparquet.Services
{
    // Structure to hold layer information for bulk creation
    public class LayerCreationInfo
    {
        public string FilePath { get; set; }
        public string LayerName { get; set; }
        public string GeometryType { get; set; }
        public int StackingPriority { get; set; }
        public string ParentTheme { get; set; }
        public string ActualType { get; set; }
        public double Area { get; set; }
    }
}
