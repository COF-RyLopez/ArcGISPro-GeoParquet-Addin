using System.Collections.Generic;

namespace DuckDBGeoparquet.Models
{
    public class GersifyResult
    {
        public string TargetLabel { get; set; }
        public string TargetTheme { get; set; }
        public string TargetDatasetType { get; set; }
        public int InputCount { get; set; }
        public int InputNameCount { get; set; }
        public int InputAddressCount { get; set; }
        public int CandidateCount { get; set; }
        public int CandidateOvertureAddressCount { get; set; }
        public int CandidateAddressSimilarityCount { get; set; }
        public int AcceptedCount { get; set; }
        public string OutputCsvPath { get; set; }
        public string CandidateCsvPath { get; set; }
        public string BridgeCsvPath { get; set; }
        public string OutputFeatureClassPath { get; set; }
        public IReadOnlyDictionary<string, int> AcceptedStrategyCounts { get; set; } = new Dictionary<string, int>();
        public IReadOnlyList<GersMatchCandidate> AcceptedMatches { get; set; } = [];
    }

    public class TraceSourcesResult
    {
        public int InputCount { get; set; }
        public int OutputCount { get; set; }
        public string OutputCsvPath { get; set; }
        public IReadOnlyDictionary<string, int> DatasetCounts { get; set; } = new Dictionary<string, int>();
        public IReadOnlyList<GersSourceTraceRecord> SampleRecords { get; set; } = [];
        public string DatasetSummaryText { get; set; } = string.Empty;
    }
}
