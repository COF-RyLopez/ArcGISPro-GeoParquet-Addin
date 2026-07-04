using DuckDBGeoparquet.Services;
using System;
using System.IO;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class OvertureDataPathsTests
    {
        [Fact]
        public void ResolveNewestReleaseFolder_ReturnsNull_ForMissingDirectory()
        {
            string missing = Path.Combine(Path.GetTempPath(), $"odp_missing_{Guid.NewGuid():N}");
            Assert.Null(OvertureDataPaths.ResolveNewestReleaseFolder(missing));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void ResolveNewestReleaseFolder_ReturnsNull_ForBlankInput(string input)
        {
            Assert.Null(OvertureDataPaths.ResolveNewestReleaseFolder(input));
        }

        [Fact]
        public void ResolveNewestReleaseFolder_ReturnsNull_WhenNoSubfolders()
        {
            string dir = CreateTempDir();
            try
            {
                // A file (not a folder) present — still no release subfolders.
                File.WriteAllText(Path.Combine(dir, "note.txt"), "x");
                Assert.Null(OvertureDataPaths.ResolveNewestReleaseFolder(dir));
            }
            finally { Directory.Delete(dir, recursive: true); }
        }

        [Fact]
        public void ResolveNewestReleaseFolder_PicksMostRecentlyModifiedSubfolder()
        {
            string dataDir = CreateTempDir();
            try
            {
                string older = Directory.CreateDirectory(Path.Combine(dataDir, "2026-05-01.0")).FullName;
                string newer = Directory.CreateDirectory(Path.Combine(dataDir, "2026-06-17.0")).FullName;
                string oldest = Directory.CreateDirectory(Path.Combine(dataDir, "2026-04-01.0")).FullName;

                // Set explicit write times so the assertion doesn't depend on creation order.
                var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                Directory.SetLastWriteTimeUtc(oldest, baseTime);
                Directory.SetLastWriteTimeUtc(older, baseTime.AddDays(10));
                Directory.SetLastWriteTimeUtc(newer, baseTime.AddDays(20));

                Assert.Equal(newer, OvertureDataPaths.ResolveNewestReleaseFolder(dataDir));
            }
            finally { Directory.Delete(dataDir, recursive: true); }
        }

        private static string CreateTempDir()
        {
            string dir = Path.Combine(Path.GetTempPath(), $"odp_{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            return dir;
        }
    }
}
