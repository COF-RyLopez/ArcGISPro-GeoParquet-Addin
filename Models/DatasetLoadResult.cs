using System.Collections.Generic;

namespace DuckDBGeoparquet.Models
{
    public enum DatasetLoadStatus { Completed, Empty, Failed, Cancelled }

    public sealed record DatasetLoadResult
    {
        public string Theme { get; init; }
        public string Type { get; init; }
        public DatasetLoadStatus Status { get; init; }
        public long RowCount { get; init; }
        public string SourceUri { get; init; }
        public List<string> SourceAssets { get; init; } = new();
        public string SourceFile { get; init; }
        public List<string> DisplayFiles { get; init; } = new();
        public string Error { get; init; }
    }
}
