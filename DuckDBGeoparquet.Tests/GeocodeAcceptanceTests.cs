using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GeocodeAcceptanceTests
    {
        private static GeocodeCandidate Candidate(string label) => new() { DisplayLabel = label, Longitude = -119, Latitude = 36 };
        [Theory]
        [InlineData("23 Main St", "123 Main St", "review")]
        [InlineData("12 1/2 Main St", "12 Main St", "review")]
        [InlineData("12 Main St Apt 2", "12 Main St Apt 3", "review")]
        [InlineData("12 Main St, Fresno", "12 Main St, Madera", "review")]
        [InlineData("12 Main St, Fresno", "12 Main St, Fresno, CA", "matched")]
        public void AutomaticAcceptance_PreservesAddressIdentity(string query, string label, string expected)
            => Assert.Equal(expected, GeocodeAcceptance.Evaluate(query, new[] { Candidate(label) }).Status);

        [Fact]
        public void TwoCompatibleAddressesRequireReview()
            => Assert.Equal("review", GeocodeAcceptance.Evaluate("12 Main St",
                new[] { Candidate("12 Main St, Fresno"), Candidate("12 Main St, Madera") }).Status);
    }
}
