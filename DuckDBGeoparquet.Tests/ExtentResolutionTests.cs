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
        // Each new AOI source is chosen when its flag is on and its
        // prerequisite exists; a flag without its prerequisite is None.
        [InlineData(true, true, ExtentSource.Typed)]
        [InlineData(true, false, ExtentSource.None)]
        [InlineData(false, true, ExtentSource.None)]
        public void ChooseSource_TypedExtent(bool useTyped, bool hasTyped, ExtentSource expected)
        {
            Assert.Equal(expected, ExtentResolution.ChooseSource(
                false, false, false, false, useTyped, hasTyped));
        }

        [Theory]
        [InlineData(true, true, ExtentSource.Layer)]
        [InlineData(true, false, ExtentSource.None)]
        [InlineData(false, true, ExtentSource.None)]
        public void ChooseSource_LayerExtent(bool useLayer, bool hasLayer, ExtentSource expected)
        {
            Assert.Equal(expected, ExtentResolution.ChooseSource(
                false, false, false, false, false, false, useLayer, hasLayer));
        }

        [Theory]
        [InlineData(true, true, ExtentSource.Polygon)]
        [InlineData(true, false, ExtentSource.None)]
        [InlineData(false, true, ExtentSource.None)]
        public void ChooseSource_PolygonExtent(bool usePolygon, bool hasPolygon, ExtentSource expected)
        {
            Assert.Equal(expected, ExtentResolution.ChooseSource(
                false, false, false, false, false, false, false, false, usePolygon, hasPolygon));
        }

        [Fact]
        public void ChooseSource_MapStillWinsOverNewSources()
        {
            Assert.Equal(ExtentSource.Map, ExtentResolution.ChooseSource(
                true, true, true, true, true, true, true, true, true, true, true, true));
        }

        [Theory]
        [InlineData(true, true, ExtentSource.SelectedFeature)]
        [InlineData(true, false, ExtentSource.None)]
        [InlineData(false, true, ExtentSource.None)]
        public void ChooseSource_SelectedFeatureExtent(bool use, bool has, ExtentSource expected)
        {
            Assert.Equal(expected, ExtentResolution.ChooseSource(
                false, false, false, false, false, false, false, false, false, false, use, has));
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
