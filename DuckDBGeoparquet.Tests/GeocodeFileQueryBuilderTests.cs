using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GeocodeFileQueryBuilderTests
    {
        [Fact]
        public void BuildQueryVariants_IncludesProgressiveFallbacks()
        {
            var variants = GeocodeFileQueryBuilder.BuildQueryVariants(
                "1939 E OLIVE AVE", "FRESNO", "CA", "93701");

            Assert.Equal("1939 E OLIVE AVE, FRESNO, CA 93701", variants[0]);
            Assert.Contains("1939 E OLIVE AVE, 93701", variants);
            Assert.Contains("1939 E OLIVE AVE", variants);
        }

        [Fact]
        public void BuildQueryVariants_DeduplicatesEquivalentVariants()
        {
            var variants = GeocodeFileQueryBuilder.BuildQueryVariants(
                "1939 E OLIVE AVE", "", "", "93701");

            Assert.Equal(2, variants.Count);
            Assert.Equal("1939 E OLIVE AVE, 93701", variants[0]);
            Assert.Equal("1939 E OLIVE AVE", variants[1]);
        }
    }
}
