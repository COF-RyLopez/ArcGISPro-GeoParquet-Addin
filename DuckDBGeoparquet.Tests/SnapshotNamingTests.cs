using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using DuckDBGeoparquet.Views;
using System;
using System.IO;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class SnapshotNamingTests : IDisposable
    {
        private readonly string _root = Path.Combine(Path.GetTempPath(), "SnapshotNamingTests_" + Guid.NewGuid().ToString("N"));

        public void Dispose()
        {
            try { if (Directory.Exists(_root)) Directory.Delete(_root, true); } catch { }
        }

        private static SelectableThemeItem Leaf(string name, string parentKey, UserDataSource source) =>
            new(name, name, parentKey, true) { UserSource = source };

        private static UserDataSource Preset() => new() { Name = "p", Url = "https://x", IsBuiltIn = true };

        [Fact]
        public void ProviderTag_Overture_Osm_SourceCoop_Custom_Mixed()
        {
            var overture = Leaf("building", "buildings", null);
            var osm = Leaf("OSM Buildings", PresetCatalog.OsmThemeKey, Preset());
            var coop = Leaf("US Structures", PresetCatalog.SourceCoopThemeKey, Preset());
            var custom = Leaf("Mine", "custom", new UserDataSource { Name = "Mine", Url = "https://y" });

            Assert.Equal("Overture", SelectableThemeItem.ComputeProviderTag(new[] { overture }));
            Assert.Equal("OSM", SelectableThemeItem.ComputeProviderTag(new[] { osm }));
            Assert.Equal("SourceCoop", SelectableThemeItem.ComputeProviderTag(new[] { coop }));
            Assert.Equal("Custom", SelectableThemeItem.ComputeProviderTag(new[] { custom }));
            Assert.Equal("Mixed", SelectableThemeItem.ComputeProviderTag(new[] { overture, osm }));
            Assert.Equal("Mixed", SelectableThemeItem.ComputeProviderTag(new[] { osm, coop }));
            Assert.Equal("Overture", SelectableThemeItem.ComputeProviderTag(Array.Empty<SelectableThemeItem>()));
            Assert.Equal("Overture", SelectableThemeItem.ComputeProviderTag(null));
        }

        [Fact]
        public void SnapshotId_CarriesTag_AndReleaseOnlyForOverture()
        {
            using var overture = new DownloadSnapshot(_root, "2026-11-18.0", new ExtentBounds(0, 0, 1, 1), "ZSTD", "Overture");
            Assert.Matches(@"^Overture_2026-11-18\.0_\d{14}_[0-9a-f]{8}$", Path.GetFileName(overture.FinalPath));

            using var osm = new DownloadSnapshot(_root, null, new ExtentBounds(0, 0, 1, 1), "ZSTD", "OSM");
            Assert.Matches(@"^OSM_\d{14}_[0-9a-f]{8}$", Path.GetFileName(osm.FinalPath));
            Assert.Null(osm.Manifest.Release);

            using var mixed = new DownloadSnapshot(_root, "2026-11-18.0", new ExtentBounds(0, 0, 1, 1), "ZSTD", "Mixed");
            Assert.Matches(@"^Mixed_2026-11-18\.0_\d{14}_[0-9a-f]{8}$", Path.GetFileName(mixed.FinalPath));
        }

        [Fact]
        public void SnapshotId_OvertureAndMixed_StillRequireDatedRelease()
        {
            Assert.Throws<ArgumentException>(() =>
                new DownloadSnapshot(_root, "", new ExtentBounds(0, 0, 1, 1), "ZSTD", "Mixed"));
            Assert.Throws<ArgumentException>(() =>
                new DownloadSnapshot(_root, "latest", new ExtentBounds(0, 0, 1, 1), "ZSTD"));
        }

        [Fact]
        public void SnapshotId_SanitizesHostileTag()
        {
            using var s = new DownloadSnapshot(_root, null, new ExtentBounds(0, 0, 1, 1), "ZSTD", "../evil tag");
            Assert.Matches(@"^eviltag_\d{14}_[0-9a-f]{8}$", Path.GetFileName(s.FinalPath));
        }

        [Theory]
        [InlineData("OSM_20261122043436_e962ad7b", "OSM — 2026-11-22 04:34")]
        [InlineData("SourceCoop_20261122050012_fd59a5a8", "Source Coop — 2026-11-22 05:00")]
        [InlineData("Custom_20261122050012_fd59a5a8", "Custom — 2026-11-22 05:00")]
        [InlineData("Overture_2026-11-18.0_20261122043436_ed02171e", "Overture 2026-11-18.0 — 2026-11-22 04:34")]
        [InlineData("Mixed_2026-11-18.0_20261122043436_ed02171e", "Mixed 2026-11-18.0 — 2026-11-22 04:34")]
        [InlineData("2026-11-18.0_20261122043436_ed02171e", "Overture 2026-11-18.0 — 2026-11-22 04:34")] // pre-tag folders
        [InlineData("SomeRandomFolder", "SomeRandomFolder")]
        [InlineData("", "")]
        public void DescribeFolder_NamesProviders(string folder, string expected)
        {
            Assert.Equal(expected, DownloadSnapshot.DescribeFolder(folder));
        }
    }
}
