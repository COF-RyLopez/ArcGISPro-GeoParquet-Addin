using System;
using System.Collections.Generic;
using System.IO;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class UserSourceStoreTests
    {
        [Fact]
        public void Load_MissingFile_ReturnsEmptyList()
        {
            string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"), "user_sources.json");
            Assert.Empty(UserSourceStore.Load(path));
        }

        [Fact]
        public void SaveAndLoad_RoundTripsSources()
        {
            string directory = Path.Combine(Path.GetTempPath(), "UserSourceStoreTests_" + Guid.NewGuid().ToString("N"));
            string path = Path.Combine(directory, "config", "user_sources.json");
            try
            {
                var sources = new List<UserDataSource>
                {
                    new() { Name = "City Parcels", Url = "https://example.com/parcels/*.parquet", GeometryColumn = "geom", GeometryIsWkb = true },
                    new() { Name = "Trails", Url = "s3://bucket/trails.parquet" }
                };
                UserSourceStore.Save(path, sources);
                var loaded = UserSourceStore.Load(path);

                Assert.Equal(2, loaded.Count);
                Assert.Equal("City Parcels", loaded[0].Name);
                Assert.Equal("geom", loaded[0].GeometryColumn);
                Assert.True(loaded[0].GeometryIsWkb);
                Assert.Equal("geometry", loaded[1].GeometryColumn);
                Assert.False(loaded[1].GeometryIsWkb);
            }
            finally
            {
                if (Directory.Exists(directory)) Directory.Delete(directory, true);
            }
        }

        [Fact]
        public void Load_CorruptFile_ReturnsEmptyList()
        {
            string path = Path.GetTempFileName();
            try
            {
                File.WriteAllText(path, "{not json");
                Assert.Empty(UserSourceStore.Load(path));
            }
            finally { File.Delete(path); }
        }

        [Fact]
        public void Load_DropsEntriesMissingNameOrUrl()
        {
            string path = Path.GetTempFileName();
            try
            {
                File.WriteAllText(path, """[{"Name":"ok","Url":"https://x/y.parquet"},{"Name":"","Url":"https://x"},{"Url":"https://x"}]""");
                var loaded = UserSourceStore.Load(path);
                Assert.Single(loaded);
                Assert.Equal("ok", loaded[0].Name);
            }
            finally { File.Delete(path); }
        }

        [Fact]
        public void PickGeometryColumn_PrefersGeometryTypedColumn()
        {
            var (column, isWkb) = UserSourceStore.PickGeometryColumn(new List<(string, string)>
            {
                ("id", "VARCHAR"), ("shape", "BLOB"), ("the_geom", "GEOMETRY")
            });
            Assert.Equal("the_geom", column);
            Assert.False(isWkb);
        }

        [Fact]
        public void PickGeometryColumn_FallsBackToWkbByConventionalName()
        {
            var (column, isWkb) = UserSourceStore.PickGeometryColumn(new List<(string, string)>
            {
                ("id", "VARCHAR"), ("wkb_geometry", "BLOB")
            });
            Assert.Equal("wkb_geometry", column);
            Assert.True(isWkb);
        }

        [Fact]
        public void PickGeometryColumn_NoGeometry_ReturnsNull()
        {
            var (column, _) = UserSourceStore.PickGeometryColumn(new List<(string, string)>
            {
                ("id", "VARCHAR"), ("value", "DOUBLE")
            });
            Assert.Null(column);
        }

        [Theory]
        [InlineData("My Parcels (2026)", "my_parcels_2026")]
        [InlineData("  Trails  ", "trails")]
        [InlineData("___", "custom_source")]
        [InlineData("Ñandú", "and")]
        public void SanitizeTypeName_ProducesStableSafeKeys(string name, string expected)
        {
            Assert.Equal(expected, UserSourceStore.SanitizeTypeName(name));
        }

        [Theory]
        [InlineData("https://example.com/a.parquet", true)]
        [InlineData("s3://bucket/a/*.parquet", true)]
        [InlineData("http://example.com/a.parquet", true)]
        [InlineData("C:\\data\\a.parquet", false)]
        [InlineData("ftp://example.com/a.parquet", false)]
        [InlineData("", false)]
        [InlineData(null, false)]
        public void IsSupportedUrl_AcceptsHttpAndS3Only(string url, bool expected)
        {
            Assert.Equal(expected, UserSourceStore.IsSupportedUrl(url));
        }

        [Fact]
        public void ValidateNewSource_AcceptsNewSource_RejectsBadInput()
        {
            var existing = new List<UserDataSource>
            {
                new() { Name = "City Parcels", Url = "https://x/p.parquet" }
            };

            Assert.Null(UserSourceStore.ValidateNewSource("Trails", "https://x/t.parquet", existing));
            Assert.Null(UserSourceStore.ValidateNewSource("Trails", "s3://bucket/*.parquet", null));

            Assert.Equal("Enter a name and a URL.", UserSourceStore.ValidateNewSource("  ", "https://x/t.parquet", existing));
            Assert.Equal("Enter a name and a URL.", UserSourceStore.ValidateNewSource("Trails", null, existing));
            Assert.Contains("http(s)://", UserSourceStore.ValidateNewSource("Trails", "ftp://x/t.parquet", existing));

            // Duplicate by case-insensitive name and by colliding sanitized type key.
            Assert.Contains("already exists", UserSourceStore.ValidateNewSource("city parcels", "https://x/t.parquet", existing));
            Assert.Contains("already exists", UserSourceStore.ValidateNewSource("City  Parcels!", "https://x/t.parquet", existing));
        }

        [Fact]
        public void BuildProbeQuery_EscapesQuotes()
        {
            string query = UserSourceStore.BuildProbeQuery("https://x/it's.parquet");
            Assert.Contains("it''s.parquet", query);
            Assert.StartsWith("DESCRIBE SELECT * FROM read_parquet(", query);
        }

        [Fact]
        public void WrapGeometryColumn_RenamesAndConvertsWkb()
        {
            string read = "read_parquet('x')";
            Assert.Equal(read, S3Ingester.WrapGeometryColumn(read, null, false));
            Assert.Equal(read, S3Ingester.WrapGeometryColumn(read, "geometry", false));

            string renamed = S3Ingester.WrapGeometryColumn(read, "geom", false);
            Assert.Equal("(SELECT * EXCLUDE(\"geom\"), \"geom\" AS geometry FROM read_parquet('x'))", renamed);

            string wkb = S3Ingester.WrapGeometryColumn(read, "wkb_geometry", true);
            Assert.Equal("(SELECT * EXCLUDE(\"wkb_geometry\"), ST_GeomFromWKB(\"wkb_geometry\") AS geometry FROM read_parquet('x'))", wkb);

            string wkbSameName = S3Ingester.WrapGeometryColumn(read, "geometry", true);
            Assert.Equal("(SELECT * EXCLUDE(geometry), ST_GeomFromWKB(geometry) AS geometry FROM read_parquet('x'))", wkbSameName);
        }

        [Fact]
        public void BuildLoadQuery_WithGeometryColumn_WrapsReadExpression()
        {
            string query = S3Ingester.BuildLoadQuery(
                "https://example.com/data/*.parquet", "my_source",
                new List<string> { "id", "geometry" },
                new ExtentBounds(-120, 36, -119, 37),
                preserveSource: true, geometryColumn: "geom");

            Assert.Contains("\"geom\" AS geometry", query);
            Assert.Contains("ST_Intersects(geometry", query);
        }
    }
}
