namespace DuckDBGeoparquet.Models
{
    public class GersMatchCandidate
    {
        public string RecordId { get; set; }
        public string UserName { get; set; }
        public string UserAddress { get; set; }
        public double Longitude { get; set; }
        public double Latitude { get; set; }
        public string GersId { get; set; }
        public string OvertureId { get; set; }
        public string OvertureName { get; set; }
        public double DistanceMeters { get; set; }
        public double NameSimilarity { get; set; }
        public double? AddressSimilarity { get; set; }
        public double MatchScore { get; set; }
        public int CandidateRank { get; set; }
        public bool Accepted { get; set; }
    }
}
