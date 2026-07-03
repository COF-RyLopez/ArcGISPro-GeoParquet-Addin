using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GeocodeTextNormalizerTests
    {
        [Theory]
        [InlineData(null, "")]
        [InlineData("", "")]
        [InlineData("   ", "")]
        [InlineData("1939 OLIVE", "1939 olive")]
        [InlineData("147 E ESCALON AVE, FRESNO, CA 93710-5109", "147 e escalon ave fresno ca 93710 5109")]
        [InlineData("  2438   Tulare St. Apt #200  ", "2438 tulare st apt 200")]
        public void NormalizeForSearch_StripsPunctuationAndCollapsesWhitespace(string input, string expected)
        {
            Assert.Equal(expected, GeocodeTextNormalizer.NormalizeForSearch(input));
        }
    }
}
