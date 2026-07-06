using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GersSourceEditResolverTests
    {
        [Theory]
        [InlineData("OpenStreetMap", "n12345@2", "https://www.openstreetmap.org/node/12345")]
        [InlineData("OpenStreetMap", "w1182486582@1", "https://www.openstreetmap.org/way/1182486582")]
        [InlineData("OpenStreetMap", "r42", "https://www.openstreetmap.org/relation/42")]
        public void BuildOpenStreetMapUrl_ParsesOsmRecordIds(string dataset, string recordId, string expected)
        {
            var resolved = GersSourceEditResolver.Resolve(dataset, recordId);
            Assert.Equal(expected, resolved.EditUrl);
            Assert.Equal("OpenStreetMap", resolved.Platform);
        }

        [Fact]
        public void Resolve_EsriDataset_ReturnsCommunityMapsContributionPath()
        {
            var resolved = GersSourceEditResolver.Resolve("Esri Community Maps", "esri_ChulaVistaCA13510");
            Assert.Equal("Esri Community Maps", resolved.Platform);
            Assert.Contains("livingatlas.arcgis.com", resolved.ContributionUrl);
            Assert.Null(resolved.EditUrl);
        }
    }
}
