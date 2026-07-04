using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class ExtentResolutionTests
    {
        [Theory]
        // Map wins whenever "use current map extent" is on and a map is active,
        // even if a custom extent is also set.
        [InlineData(true, true, false, false, ExtentSource.Map)]
        [InlineData(true, true, true, true, ExtentSource.Map)]
        // No active map (or toggle off) falls back to a set custom extent.
        [InlineData(true, false, true, true, ExtentSource.Custom)]
        [InlineData(false, true, true, true, ExtentSource.Custom)]
        [InlineData(false, false, true, true, ExtentSource.Custom)]
        // Neither source available → None.
        [InlineData(true, false, true, false, ExtentSource.None)]  // custom flag on but no extent
        [InlineData(true, false, false, true, ExtentSource.None)]  // custom extent present but flag off
        [InlineData(false, false, false, false, ExtentSource.None)]
        public void ChooseSource_FollowsMapThenCustomPrecedence(
            bool useMap, bool hasMap, bool useCustom, bool hasCustom, ExtentSource expected)
        {
            Assert.Equal(expected, ExtentResolution.ChooseSource(useMap, hasMap, useCustom, hasCustom));
        }

        [Theory]
        [InlineData(4326, false)]     // already WGS84
        [InlineData(3857, true)]      // Web Mercator
        [InlineData(102100, true)]    // Esri Web Mercator
        [InlineData(null, true)]      // unknown SR → project defensively
        public void NeedsProjectionToWgs84_TrueUnlessAlready4326(int? wkid, bool expected)
        {
            Assert.Equal(expected, ExtentResolution.NeedsProjectionToWgs84(wkid));
        }
    }
}
