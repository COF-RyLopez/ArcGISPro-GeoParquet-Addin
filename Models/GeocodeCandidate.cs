using DuckDBGeoparquet.Services;

namespace DuckDBGeoparquet.Models
{
    public class GeocodeCandidate
    {
        private string _rawMatchTier;
        private string _rawConfidenceTier;

        public string DisplayLabel { get; set; }
        public double Longitude { get; set; }
        public double Latitude { get; set; }
        public string SourceType { get; set; }
        public string MatchTier
        {
            get => GeocodeResultLabels.GetMatchSummary(_rawMatchTier, SourceType);
            set => _rawMatchTier = value;
        }

        public string ConfidenceTier
        {
            get => GeocodeResultLabels.GetConfidenceSummary(_rawConfidenceTier);
            set => _rawConfidenceTier = value;
        }

        public string RawMatchTier => _rawMatchTier;
        public string RawConfidenceTier => _rawConfidenceTier;
        public int Score { get; set; }
        public string MatchSummary => MatchTier;
        public string ConfidenceSummary => ConfidenceTier;
    }
}
