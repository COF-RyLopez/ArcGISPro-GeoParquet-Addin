using DuckDBGeoparquet.Services;
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class OvertureReleaseDiscoveryTests
    {
        private const string Inventory = "<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>release/2026-07-22.0/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>release/2026-08-19.0/</Prefix></CommonPrefixes></ListBucketResult>";
        [Fact]
        public async Task ForbiddenCatalog_UsesAvailableS3ReleaseInsteadOfStaleCachedDate()
        {
            var discovery = new OvertureReleaseDiscovery((uri, _) => uri.Host == "stac.overturemaps.org"
                ? Task.FromException<string>(new HttpRequestException("Forbidden", null, HttpStatusCode.Forbidden))
                : Task.FromResult(Inventory));
            var selection = await discovery.GetLatestAsync();
            Assert.Equal("2026-08-19.0", selection.Release);
            Assert.Equal("public S3 release inventory", selection.Source);
        }
        [Fact]
        public async Task HealthyCatalog_DoesNotRequestFallback()
        {
            int calls = 0;
            var discovery = new OvertureReleaseDiscovery((uri, _) => { calls++; Assert.Equal("stac.overturemaps.org", uri.Host); return Task.FromResult("{\"latest\":\"2026-08-19.0\"}"); });
            Assert.Equal("2026-08-19.0", (await discovery.GetLatestAsync()).Release);
            Assert.Equal(1, calls);
        }
        [Theory]
        [InlineData("{}")]
        [InlineData("{\"latest\":\"latest\"}")]
        [InlineData("{\"latest\":\"2026-99-99.0\"}")]
        public async Task InvalidCatalog_FallsBack(string json)
        {
            var discovery = new OvertureReleaseDiscovery((uri, _) => Task.FromResult(uri.Host == "stac.overturemaps.org" ? json : Inventory));
            Assert.Equal("2026-08-19.0", (await discovery.GetLatestAsync()).Release);
        }
        [Fact]
        public async Task PaginatedInventory_UsesNewestDateAndNumericRevision()
        {
            int pages = 0;
            var discovery = new OvertureReleaseDiscovery((uri, _) =>
            {
                if (uri.Host == "stac.overturemaps.org") throw new HttpRequestException("Forbidden");
                pages++;
                if (pages == 1) return Task.FromResult("<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>a+b/=</NextContinuationToken><CommonPrefixes><Prefix>release/2026-08-19.2/</Prefix></CommonPrefixes></ListBucketResult>");
                Assert.Contains("continuation-token=a%2Bb%2F%3D", uri.Query);
                return Task.FromResult("<ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>release/2026-08-19.10/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>release/latest/</Prefix></CommonPrefixes></ListBucketResult>");
            });
            Assert.Equal("2026-08-19.10", (await discovery.GetLatestAsync()).Release);
            Assert.Equal(2, pages);
        }
        [Fact]
        public async Task Cancellation_DoesNotStartFallback()
        {
            using var cancellation = new System.Threading.CancellationTokenSource();
            int calls = 0;
            var discovery = new OvertureReleaseDiscovery((_, token) => { calls++; cancellation.Cancel(); return Task.FromCanceled<string>(token); });
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => discovery.GetLatestAsync(token: cancellation.Token));
            Assert.Equal(1, calls);
        }
        [Fact]
        public async Task BothEndpointsFail_ReportFailureInsteadOfReturningCachedDate()
        {
            var discovery = new OvertureReleaseDiscovery((_, _) => Task.FromException<string>(new HttpRequestException("Forbidden")));
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => discovery.GetLatestAsync());
            Assert.Contains("Catalog:", error.Message); Assert.Contains("S3 inventory:", error.Message);
        }
        [Fact]
        public async Task RetiredRelease_IsRejectedBeforeDatasetQueries()
        {
            int calls = 0;
            var discovery = new OvertureReleaseDiscovery((uri, _) => { calls++; Assert.Contains("2026-06-17.0", uri.Query); return Task.FromResult("<ListBucketResult><KeyCount>0</KeyCount></ListBucketResult>"); });
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => discovery.EnsureAvailableAsync("2026-06-17.0"));
            Assert.Contains("may have been retired", error.Message);
            Assert.Equal(1, calls);
        }
        [Fact]
        public async Task PublishedRelease_PassesPreflight()
        {
            var discovery = new OvertureReleaseDiscovery((_, _) => Task.FromResult("<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'><Contents><Key>release/2026-08-19.0/theme=addresses/type=address/a.parquet</Key></Contents></ListBucketResult>"));
            await discovery.EnsureAvailableAsync("2026-08-19.0");
        }
        [Theory]
        [InlineData("latest")]
        [InlineData("2026-02-30.0")]
        [InlineData("../2026-08-19.0")]
        public async Task InvalidDate_DoesNotRequestNetwork(string release)
        {
            var discovery = new OvertureReleaseDiscovery((_, _) => throw new Exception("Unexpected network request"));
            await Assert.ThrowsAsync<ArgumentException>(() => discovery.EnsureAvailableAsync(release));
        }
    }
}
