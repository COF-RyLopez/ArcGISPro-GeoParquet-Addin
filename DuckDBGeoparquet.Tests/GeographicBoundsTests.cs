using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GeographicBoundsTests
    {
        [Theory]
        [InlineData("-119.9", -119.9)]
        [InlineData(" 36.76 ", 36.76)]
        [InlineData("0", 0)]
        [InlineData("-180", -180)]
        [InlineData("1e1", 10)]        // scientific notation is fine
        [InlineData("36,76", 36.76)]   // lone comma = decimal comma (pasted from comma-decimal locales)
        public void TryParseCoordinate_AcceptsPlainDecimalDegrees(string text, double expected)
        {
            Assert.True(GeographicBounds.TryParseCoordinate(text, out double value));
            Assert.Equal(expected, value, 10);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        [InlineData("abc")]
        [InlineData("12.3.4")]
        [InlineData("1,234.5")]  // thousands separators rejected
        [InlineData("1,2,3")]
        [InlineData("NaN")]
        [InlineData("Infinity")]
        public void TryParseCoordinate_RejectsNonCoordinates(string text)
        {
            Assert.False(GeographicBounds.TryParseCoordinate(text, out _));
        }

        [Fact]
        public void TryParseBbox_ValidInput_ReturnsBounds()
        {
            string error = GeographicBounds.TryParseBbox("-119.9", "36.6", "-119.6", "36.9", out ExtentBounds bounds);
            Assert.Null(error);
            Assert.Equal(-119.9, bounds.XMin);
            Assert.Equal(36.6, bounds.YMin);
            Assert.Equal(-119.6, bounds.XMax);
            Assert.Equal(36.9, bounds.YMax);
            // The parsed bbox always passes the load pipeline's validator.
            GeographicBounds.Validate(bounds);
        }

        [Theory]
        [InlineData("x", "0", "1", "1", "West")]
        [InlineData("0", "x", "1", "1", "South")]
        [InlineData("0", "0", "x", "1", "East")]
        [InlineData("0", "0", "1", "x", "North")]
        public void TryParseBbox_NamesTheBadField(string w, string s, string e, string n, string expectedField)
        {
            string error = GeographicBounds.TryParseBbox(w, s, e, n, out var bounds);
            Assert.Null(bounds);
            Assert.StartsWith(expectedField, error);
        }

        [Theory]
        [InlineData("-181", "0", "1", "1")]   // west out of range
        [InlineData("0", "-91", "1", "1")]    // south out of range
        [InlineData("0", "0", "181", "1")]    // east out of range
        [InlineData("0", "0", "1", "91")]     // north out of range
        [InlineData("2", "0", "1", "1")]      // west >= east
        [InlineData("0", "2", "1", "1")]      // south >= north
        [InlineData("1", "0", "1", "1")]      // zero-width
        public void TryParseBbox_RejectsOutOfRangeOrInverted(string w, string s, string e, string n)
        {
            Assert.NotNull(GeographicBounds.TryParseBbox(w, s, e, n, out var bounds));
            Assert.Null(bounds);
        }
    }
}
