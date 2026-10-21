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

        [Fact]
        public void RemovedReleaseMessage_IncludesNewestAvailableRelease()
        {
            string message = ReleaseValidator.RemovedReleaseMessage("2025-08-20.0", "2026-09-23.1");
            Assert.Contains("2025-08-20.0", message);
            Assert.Contains("2026-09-23.1", message);
            Assert.Contains("no longer available", message);
        }
    }
}
