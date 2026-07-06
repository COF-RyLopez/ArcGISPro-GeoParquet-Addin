using System.Globalization;

namespace DuckDBGeoparquet.Models
{
    public class GersifyLinkageSummary
    {
        public int InputCount { get; init; }
        public int LinkedCount { get; init; }
        public int UnmatchedCount { get; init; }
        public int WeakLinkCount { get; init; }
        public double LinkedPercent { get; init; }
        public string TargetLabel { get; init; }
        public string SourceLayerName { get; init; }
        public string RelateSummary { get; init; }
        public string StrategySummary { get; init; }

        public string LinkedPercentText =>
            InputCount == 0
                ? "0%"
                : $"{LinkedPercent.ToString("0.#", CultureInfo.InvariantCulture)}%";

        public string Headline =>
            LinkedCount == 0
                ? "No stable GERS links were accepted yet."
                : $"{LinkedCount:N0} features now carry stable Overture GERS IDs";

        public string Subheadline =>
            InputCount == 0
                ? string.Empty
                : $"{LinkedPercentText} of {InputCount:N0} {TargetLabel?.ToLowerInvariant() ?? "features"} linked — source geometry unchanged.";
    }
}
