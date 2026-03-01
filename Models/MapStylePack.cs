using System.Collections.Generic;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Groups related map styles into a pack so style catalogs can be extended
    /// without changing UI binding contracts.
    /// </summary>
    public class MapStylePack
    {
        public string Id { get; set; }
        public string DisplayName { get; set; }
        public string Description { get; set; }
        public string Category { get; set; }
        public bool IsExperimental { get; set; }
        public List<MapStyleDefinition> Styles { get; set; } = new();
    }
}
