namespace DuckDBGeoparquet.Models
{
    public class GersSourceTraceRecord
    {
        public string GersId { get; init; }
        public string Dataset { get; init; }
        public string RecordId { get; init; }
        public string UpdateTime { get; init; }
        public string Theme { get; init; }
        public string Type { get; init; }
        public string SourceUrl { get; init; }
        public string EditUrl { get; init; }
        public string EditPlatform { get; init; }
        public string ContributionUrl { get; init; }
        public string EditInstructions { get; init; }

        public string DisplayLabel =>
            string.IsNullOrWhiteSpace(EditPlatform)
                ? Dataset
                : $"{EditPlatform} • {RecordId}";
    }
}
