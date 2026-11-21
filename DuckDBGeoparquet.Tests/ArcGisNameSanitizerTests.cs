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

        [Theory]
        [InlineData("buildings building polygons", "buildings_building_polygons")]
        [InlineData("base/land-use", "base_land_use")]
        [InlineData("division_area (polygons)", "division_area_polygons")]
        public void ToFileGeodatabaseFeatureClassName_SanitizesThemeAndTypeNames(string input, string expected)
        {
            Assert.Equal(expected, ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(input));
        }

        [Fact]
        public void ToFileGeodatabaseFeatureClassName_TruncatesToMaxLength()
        {
            string result = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(
                "transportation_division_boundary_multilines_extra_detail", maxLength: 50);

            Assert.True(result.Length <= 50);
            Assert.False(result.EndsWith('_'));
        }
    }
}
