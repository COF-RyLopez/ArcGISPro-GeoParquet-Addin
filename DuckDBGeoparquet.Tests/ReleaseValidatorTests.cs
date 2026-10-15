using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class ReleaseValidatorTests
    {
        [Theory]
        [InlineData("No files found that match the glob pattern")]
        [InlineData("glob pattern matched 0 files")]
        [InlineData("The file does not exist")]
        [InlineData("HTTP Error 404: Not Found")]
        public void IsMissingFileError_DetectsMissingFileErrors(string errorMessage)
        {
            Assert.True(ReleaseValidator.IsMissingFileError(errorMessage));
        }

        [Theory]
        [InlineData("Connection timeout")]
        [InlineData("Invalid SQL syntax")]
        [InlineData("")]
        [InlineData(null)]
        public void IsMissingFileError_DoesNotDetectUnrelatedErrors(string errorMessage)
        {
            Assert.False(ReleaseValidator.IsMissingFileError(errorMessage));
        }
    }
}
