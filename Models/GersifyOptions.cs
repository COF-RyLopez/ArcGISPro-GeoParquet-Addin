using System;

namespace DuckDBGeoparquet.Models
{
    public class GersifyOptions
    {
        public string InputCsvPath { get; set; }
        public string InputLayerName { get; set; }
        public string UniqueIdField { get; set; }
        public string NameField { get; set; }
        public string AddressField { get; set; }
        public string CityField { get; set; }
        public string StateField { get; set; }
        public string PostcodeField { get; set; }
        public string OvertureReleaseFolder { get; set; }
        public string OutputFolder { get; set; }
        public string DatasetName { get; set; } = "user_data";
        public ExtentBounds InputExtent { get; set; }
        public double MaxDistanceMeters { get; set; } = 75;
        public double NameSimilarityThreshold { get; set; } = 0.86;
        public double AddressSimilarityThreshold { get; set; } = 0.72;
        public double AcceptScoreThreshold { get; set; } = 72;
        public int ReviewCandidatesPerRecord { get; set; } = 5;

        public static string DefaultReleaseVersion => "2026-06-17.0";

        public static string BuildTimestampSuffix() =>
            DateTime.UtcNow.ToString("yyyyMMdd_HHmmss", System.Globalization.CultureInfo.InvariantCulture);
    }

    public class TraceSourcesOptions
    {
        public string InputCsvPath { get; set; }
        public string OutputFolder { get; set; }
        public string BridgeRoot { get; set; } = "https://overturemapswestus2.blob.core.windows.net/bridgefiles";
        public string Release { get; set; } = GersifyOptions.DefaultReleaseVersion;
        public string Theme { get; set; } = "places";
        public string Type { get; set; } = "place";
        public int MaxRows { get; set; } = 250000;
    }
}
