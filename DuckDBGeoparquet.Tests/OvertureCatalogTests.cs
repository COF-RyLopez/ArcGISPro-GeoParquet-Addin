using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
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
        public void Dispose() { if (Directory.Exists(_cache)) Directory.Delete(_cache, true); }
    }
}
