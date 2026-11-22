using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class PresetCatalogTests
    {
        private static UserDataSource[] All =>
            PresetCatalog.OpenStreetMap.Concat(PresetCatalog.SourceCooperative).ToArray();

        [Fact]
        public void Catalog_HasExpectedGroups()
        {
            Assert.Equal(9, PresetCatalog.OpenStreetMap.Count);
            Assert.Equal(7, PresetCatalog.SourceCooperative.Count);
        }

        [Fact]
        public void EveryPreset_IsBuiltInWithSupportedUrlAndGeometry()
        {
            foreach (var preset in All)
            {
                Assert.True(preset.IsBuiltIn, preset.Name);
                Assert.True(UserSourceStore.IsSupportedUrl(preset.Url), preset.Url);
                Assert.False(string.IsNullOrWhiteSpace(preset.GeometryColumn), preset.Name);
            }
        }

        [Fact]
        public void PresetNamesAndTypeKeys_AreUnique()
        {
            Assert.Equal(All.Length, All.Select(p => p.Name).Distinct().Count());
            Assert.Equal(All.Length, All.Select(p => UserSourceStore.SanitizeTypeName(p.Name)).Distinct().Count());
        }

        [Fact]
        public void IsBuiltIn_IsNeverSerialized()
        {
            string json = JsonSerializer.Serialize(new UserDataSource { Name = "n", Url = "https://x/p.parquet", IsBuiltIn = true });
            Assert.DoesNotContain("IsBuiltIn", json);
        }

        [Fact]
        public void SourceCoopSecret_IsScopedToTheDottedBucket()
        {
            Assert.Contains("SCOPE 's3://us-west-2.opendata.source.coop/'", PresetCatalog.SourceCoopSecretSql);
            Assert.Contains("URL_STYLE 'path'", PresetCatalog.SourceCoopSecretSql);
            // The session settings apply it, so every DuckDB session can read the bucket.
            Assert.Contains(PresetCatalog.SourceCoopSecretSql,
                HttpProxySettingsBuilder.BuildSessionSettingsSql(new AddinSettings(), null, 4, "/tmp"));
        }
    }
}
