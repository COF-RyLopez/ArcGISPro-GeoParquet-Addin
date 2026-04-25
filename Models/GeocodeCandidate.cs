namespace DuckDBGeoparquet.Models
{
    public class GeocodeCandidate
    {
        public string DisplayLabel { get; set; }
        public double Longitude { get; set; }
        public double Latitude { get; set; }
        public string SourceType { get; set; }
        public string MatchTier { get; set; }
        public string ConfidenceTier { get; set; }
        public int Score { get; set; }
    }
}
