using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class OvertureReleaseIdTests
    {
        [Theory]
        [InlineData("2026-09-23.1", "2026-09-23.0")]
        [InlineData("2026-10-15.0", "2026-09-23.10")]
        [InlineData("2026-09-23.10", "2026-09-23.2")]
        [InlineData("2025-01-01.0", "2024-12-31.99")]
        public void Compare_OrdersByDateThenNumericRevision(string newer, string older)
        {
            Assert.True(OvertureReleaseId.Compare(newer, older) > 0);
            Assert.True(OvertureReleaseId.Compare(older, newer) < 0);
            Assert.Equal(0, OvertureReleaseId.Compare(newer, newer));
        }

        [Fact]
        public void SelectNewest_PicksLaterDateOverHigherRevisionOnEarlierDate()
        {
            Assert.Equal(
                "2026-10-15.0",
                OvertureReleaseId.SelectNewest(new[] { "2026-09-23.10", "2026-10-15.0", "2026-09-23.1" }));
        }

        [Fact]
        public void SelectNewest_PicksHigherRevisionOnSameDate()
        {
            Assert.Equal(
                "2026-09-23.1",
                OvertureReleaseId.SelectNewest(new[] { "2026-09-23.0", "2026-09-23.1" }));
        }

        [Fact]
        public void SelectNewest_IgnoresNonReleasePrefixes()
        {
            var mixed = new[]
            {
                "latest",
                "release/latest/",
                "theme=buildings",
                "release/theme=buildings/",
                "2026-09-23",
                "not-a-release",
                "",
                null,
                "release/2026-09-23.0/",
                "release/2026-09-23.1/",
                "2026-13-01.0"
            };

            Assert.Equal("2026-09-23.1", OvertureReleaseId.SelectNewest(mixed));
        }

        [Theory]
        [InlineData("2026-09-23.1", "2026-09-23.1")]
        [InlineData("release/2026-09-23.1/", "2026-09-23.1")]
        [InlineData("/release/2026-09-23.0/", "2026-09-23.0")]
        [InlineData("https://stac.overturemaps.org/2026-09-23.1/catalog.json", "2026-09-23.1")]
        [InlineData("2026-09-23.1 Overture Release", "2026-09-23.1")]
        public void TryNormalize_ExtractsCanonicalReleaseId(string input, string expected)
        {
            Assert.True(OvertureReleaseId.TryNormalize(input, out var id));
            Assert.Equal(expected, id);
            Assert.True(OvertureReleaseId.IsUsable(input));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("latest")]
        [InlineData("LATEST")]
        [InlineData("release/latest/")]
        [InlineData("theme=buildings")]
        [InlineData("2026-09-23")]
        [InlineData("2026-13-40.0")]
        public void TryNormalize_RejectsPlaceholdersAndNonReleasePrefixes(string input)
        {
            Assert.False(OvertureReleaseId.TryNormalize(input, out _));
            Assert.False(OvertureReleaseId.IsUsable(input));
        }

        [Fact]
        public void IsPlaceholder_DetectsLatestOnly()
        {
            Assert.True(OvertureReleaseId.IsPlaceholder("latest"));
            Assert.True(OvertureReleaseId.IsPlaceholder("LATEST"));
            Assert.False(OvertureReleaseId.IsPlaceholder("2026-09-23.1"));
            Assert.False(OvertureReleaseId.IsPlaceholder(null));
        }

        [Fact]
        public void ParseStacCatalog_ReadsChildLinksAndLatestField()
        {
            const string json = """
                {
                  "type": "Catalog",
                  "latest": "2026-09-23.1",
                  "links": [
                    { "rel": "root", "href": "https://stac.overturemaps.org/catalog.json" },
                    { "rel": "child", "href": "https://stac.overturemaps.org/2026-09-23.0/catalog.json", "title": "2026-09-23.0 Overture Release" },
                    { "rel": "child", "href": "https://stac.overturemaps.org/2026-09-23.1/catalog.json", "title": "2026-09-23.1 Overture Release", "latest": true }
                  ]
                }
                """;

            var releases = OvertureReleaseId.ParseStacCatalog(json);
            Assert.Equal(new[] { "2026-09-23.1", "2026-09-23.0" }, releases);
            Assert.Equal("2026-09-23.1", OvertureReleaseId.SelectNewest(releases));
        }

        [Fact]
        public void ParseS3ListXml_ReadsCommonPrefixesAndIgnoresNonReleases()
        {
            const string xml = """
                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <Name>overturemaps-us-west-2</Name>
                  <Prefix>release/</Prefix>
                  <Delimiter>/</Delimiter>
                  <CommonPrefixes>
                    <Prefix>release/2026-09-23.0/</Prefix>
                  </CommonPrefixes>
                  <CommonPrefixes>
                    <Prefix>release/2026-09-23.1/</Prefix>
                  </CommonPrefixes>
                  <CommonPrefixes>
                    <Prefix>release/latest/</Prefix>
                  </CommonPrefixes>
                  <CommonPrefixes>
                    <Prefix>release/theme=buildings/</Prefix>
                  </CommonPrefixes>
                </ListBucketResult>
                """;

            var releases = OvertureReleaseId.ParseS3ListXml(xml);
            Assert.Equal(new[] { "2026-09-23.1", "2026-09-23.0" }, releases);
        }

        [Fact]
        public void ToReleaseInfo_SetsNewestAsLatest()
        {
            var info = ReleaseValidator.ToReleaseInfo(new[]
            {
                "2026-09-23.10",
                "latest",
                "2026-10-15.0",
                "theme=buildings"
            });

            Assert.Equal("2026-10-15.0", info.Latest);
            Assert.Equal(new List<string> { "2026-10-15.0", "2026-09-23.10" }, info.Releases);
        }

        [Fact]
        public void RemovedReleaseMessage_NamesNewestAvailable()
        {
            Assert.Equal(
                "Release 2025-01-01.0 is no longer available from Overture; newest available is 2026-09-23.1",
                ReleaseValidator.RemovedReleaseMessage("2025-01-01.0", "2026-09-23.1"));
        }
    }
}
