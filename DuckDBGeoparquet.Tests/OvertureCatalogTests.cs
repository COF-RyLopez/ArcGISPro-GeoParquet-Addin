using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public sealed class OvertureCatalogTests : IDisposable
    {
        private readonly string _cache = Path.Combine(Path.GetTempPath(), "catalog-" + Guid.NewGuid().ToString("N"));
        private const string Collection = "{\"links\":[{\"rel\":\"item\",\"href\":\"near.json\"},{\"rel\":\"item\",\"href\":\"far.json\"}]}";
        private static string Item(string name, string bounds) => "{\"bbox\":" + bounds + ",\"assets\":{\"aws\":{\"href\":\"https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/" + name + ".parquet\"}}}";
        [Fact]
        public async Task CompleteManifest_IsPrunedAndReusedForAnotherArea()
        {
            int requests = 0;
            var catalog = new OvertureCatalog((uri, _) =>
            {
                Interlocked.Increment(ref requests);
                return Task.FromResult(uri.AbsolutePath.EndsWith("collection.json") ? Collection :
                    uri.AbsolutePath.EndsWith("near.json") ? Item("near", "[0,0,1,1]") : Item("far", "[40,40,41,41]"));
            });
            var selected = await catalog.FindFilesAsync("2026-08-19.0", "buildings", "building", new ExtentBounds(1, 1, 2, 2), _cache);
            Assert.EndsWith("near.parquet", Assert.Single(selected));
            Assert.Equal(3, requests);
            var offline = new OvertureCatalog((_, _) => throw new Exception("Cache should avoid network access."));
            selected = await offline.FindFilesAsync("2026-08-19.0", "buildings", "building", new ExtentBounds(40, 40, 41, 41), _cache);
            Assert.EndsWith("far.parquet", Assert.Single(selected));
            Assert.Empty(await offline.FindFilesAsync("2026-08-19.0", "buildings", "building", new ExtentBounds(80, 40, 81, 41), _cache));
        }
        [Fact]
        public async Task MissingItem_CannotPublishPartialInventory()
        {
            var catalog = new OvertureCatalog((uri, _) => Task.FromResult(uri.AbsolutePath.EndsWith("collection.json") ? Collection :
                uri.AbsolutePath.EndsWith("near.json") ? Item("near", "[0,0,1,1]") : throw new IOException("Item unavailable")));
            await Assert.ThrowsAsync<IOException>(() => catalog.FindFilesAsync("2026-08-19.0", "buildings", "building", new ExtentBounds(0, 0, 1, 1), _cache));
            Assert.False(Directory.Exists(_cache));
        }
        [Fact]
        public void LiveLandCoverBounds_DoNotRejectValidUserArea()
        {
            // Captured from 2026-08-19.0/base/land_cover/00000/00000.json.
            using var item = JsonDocument.Parse(Item("land-cover", "[-180.00022888183594,-80.46366882324219,-56.77799606323242,20.327512741088867]"));
            var asset = OvertureCatalog.ParseAsset(item.RootElement);
            Assert.Equal(-180, asset.Bounds.XMin);
            Assert.Equal(180, asset.Bounds.XMax);
            Assert.Equal(-80.46366882324219, asset.Bounds.YMin);
            Assert.Equal(20.327512741088867, asset.Bounds.YMax);
        }

        [Fact]
        public async Task UncertainLongitude_KeepsCandidateFileButStillPrunesByLatitude()
        {
            var catalog = new OvertureCatalog((uri, _) => Task.FromResult(uri.AbsolutePath.EndsWith("collection.json") ? Collection :
                uri.AbsolutePath.EndsWith("near.json") ? Item("near", "[-180.00022888183594,36,-56,37]") : Item("far", "[-180.00022888183594,-80,-56,20]")));
            // An uncertain axis must not exclude either side of the dateline.
            var files = await catalog.FindFilesAsync("2026-08-19.0", "base", "land_cover", new ExtentBounds(170, 36, 175, 37), _cache);
            Assert.EndsWith("near.parquet", Assert.Single(files));
            var offline = new OvertureCatalog((_, _) => throw new Exception("Inventory should be cached"));
            Assert.Single(await offline.FindFilesAsync("2026-08-19.0", "base", "land_cover", new ExtentBounds(-119.85, 36.70, -119.71, 36.79), _cache));
        }

        [Theory]
        [InlineData(178, 179, true)]
        [InlineData(-180, -179, true)]
        [InlineData(-1, 1, false)]
        public async Task WrappedCatalogBounds_SelectBothDatelineSidesOnly(double west, double east, bool expected)
        {
            var catalog = new OvertureCatalog((uri, _) => Task.FromResult(uri.AbsolutePath.EndsWith("collection.json") ? Collection : Item("wrapped", "[177,-20,-178,-16]")));
            var files = await catalog.FindFilesAsync("2026-08-19.0", "base", "land_cover", new ExtentBounds(west, -19, east, -17), _cache);
            Assert.Equal(expected ? 1 : 0, files.Count);
        }

        [Theory]
        [InlineData("null")]
        [InlineData("[]")]
        [InlineData("[1,2,3]")]
        [InlineData("[\"unknown\",2,3,4]")]
        public void UnsupportedBounds_RetainFileWithWorldBounds(string bbox)
        {
            using var item = JsonDocument.Parse(Item("unknown", bbox));
            Assert.Equal(new ExtentBounds(-180, -90, 180, 90), OvertureCatalog.ParseAsset(item.RootElement).Bounds);
        }

        [Fact]
        public void ThreeDimensionalBounds_UseHorizontalAxes()
        {
            using var item = JsonDocument.Parse(Item("3d", "[-120,35,-50,-119,36,100]"));
            Assert.Equal(new ExtentBounds(-120, 35, -119, 36), OvertureCatalog.ParseAsset(item.RootElement).Bounds);
        }

        [Fact]
        public async Task LegacyCachedBounds_AreNormalizedWithoutNetworkAccess()
        {
            Directory.CreateDirectory(_cache);
            var asset = new CatalogAsset("https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/cached.parquet", new ExtentBounds(-181, 35, -119, 37));
            await File.WriteAllTextAsync(Path.Combine(_cache, "2026-08-19.0_base_land_cover.json"), JsonSerializer.Serialize(new[] { asset }));
            var catalog = new OvertureCatalog((_, _) => throw new Exception("Unexpected network request"));
            Assert.Single(await catalog.FindFilesAsync("2026-08-19.0", "base", "land_cover", new ExtentBounds(170, 36, 175, 37), _cache));
        }

        [Fact]
        public async Task InvalidUserArea_StillFailsBeforeReadingCatalog()
        {
            var catalog = new OvertureCatalog((_, _) => throw new Exception("Unexpected network request"));
            await Assert.ThrowsAsync<ArgumentException>(() => catalog.FindFilesAsync("2026-08-19.0", "base", "land_cover", new ExtentBounds(-181, 36, -119, 37), _cache));
        }

        public void Dispose() { if (Directory.Exists(_cache)) Directory.Delete(_cache, true); }
    }
}
