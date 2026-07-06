using DuckDBGeoparquet.Services;
using System;
using System.IO;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GersifyOutputPathsTests
    {
        [Fact]
        public void FeatureClassExists_ReturnsTrueWhenParentGdbExists()
        {
            string gdbPath = Path.Combine(Path.GetTempPath(), $"gersify_paths_{Guid.NewGuid():N}.gdb");
            Directory.CreateDirectory(gdbPath);
            try
            {
                string featureClassPath = Path.Combine(gdbPath, "GERSified_Test");
                Assert.True(GersifyOutputPaths.FeatureClassExists(featureClassPath));
            }
            finally
            {
                if (Directory.Exists(gdbPath))
                {
                    Directory.Delete(gdbPath, recursive: true);
                }
            }
        }
    }
}
