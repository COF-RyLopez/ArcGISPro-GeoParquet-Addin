using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GeocodeResultLabelsTests
    {
        [Theory]
        [InlineData("exact", "Address", "Exact address match")]
        [InlineData("prefix", "Address", "Strong address match")]
        [InlineData("contains", "Place", "Partial place match")]
        [InlineData("token", "Address", "Approximate text match")]
        [InlineData("locator", "Locator", "Matched by ArcGIS locator")]
        public void GetMatchSummary_MapsInternalTiersToFriendlyLabels(string matchTier, string sourceType, string expected)
        {
            Assert.Equal(expected, GeocodeResultLabels.GetMatchSummary(matchTier, sourceType));
        }

        [Theory]
        [InlineData("High", "High confidence")]
        [InlineData("Medium", "Good candidate")]
        [InlineData("Low", "Needs review")]
        public void GetConfidenceSummary_MapsInternalTiersToFriendlyLabels(string confidenceTier, string expected)
        {
            Assert.Equal(expected, GeocodeResultLabels.GetConfidenceSummary(confidenceTier));
        }

        [Fact]
        public void GeocodeCandidate_ExposesFriendlyMatchAndConfidenceLabels()
        {
            var candidate = new GeocodeCandidate
            {
                SourceType = "Address",
                MatchTier = "prefix",
                ConfidenceTier = "Medium"
            };

            Assert.Equal("Strong address match", candidate.MatchTier);
            Assert.Equal("Strong address match", candidate.MatchSummary);
            Assert.Equal("Good candidate", candidate.ConfidenceTier);
            Assert.Equal("Good candidate", candidate.ConfidenceSummary);
            Assert.Equal("prefix", candidate.RawMatchTier);
            Assert.Equal("Medium", candidate.RawConfidenceTier);
        }
    }
}
