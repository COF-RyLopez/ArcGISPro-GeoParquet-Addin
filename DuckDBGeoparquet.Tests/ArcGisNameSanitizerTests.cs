using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class ArcGisNameSanitizerTests
    {
        [Fact]
        public void ToFileGeodatabaseFeatureClassName_ReplacesQualifiedDatabaseNamePunctuation()
        {
            string result = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName("GERSified_geoprod.CY_FRESNO.ADDRESS_VW");

            Assert.Equal("GERSified_geoprod_CY_FRESNO_ADDRESS_VW", result);
        }

        [Fact]
        public void ToFileGeodatabaseFeatureClassName_PrefixesNamesThatDoNotStartWithLetter()
        {
            string result = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName("123 bad/name");

            Assert.Equal("fc_123_bad_name", result);
        }

        [Fact]
        public void ToFileGeodatabaseFeatureClassName_FallsBackWhenInputIsEmpty()
        {
            string result = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName("...", "FallbackName");

            Assert.Equal("FallbackName", result);
        }
    }
}
