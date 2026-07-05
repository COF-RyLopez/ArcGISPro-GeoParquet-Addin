using System.Collections.Generic;

namespace DuckDBGeoparquet.Models
{
    public class GersifyResult
    {
        public int InputCount { get; set; }
        public int CandidateCount { get; set; }
        public int AcceptedCount { get; set; }
        public string OutputCsvPath { get; set; }
        public string CandidateCsvPath { get; set; }
        public string BridgeCsvPath { get; set; }
        public string OutputFeatureClassPath { get; set; }
        public IReadOnlyList<GersMatchCandidate> AcceptedMatches { get; set; } = [];
    }

    public class TraceSourcesResult
    {
        public int InputCount { get; set; }
        public int OutputCount { get; set; }
        public string OutputCsvPath { get; set; }
    }
}
