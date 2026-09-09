using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>Share proxy behavior and an identifiable client across Overture metadata requests.</summary>
    internal static class OvertureHttp
    {
        private static readonly HttpClient Client = CreateClient();
        private static HttpClient CreateClient()
        {
            var client = new HttpClient(new HttpClientHandler
            {
                UseProxy = true,
                DefaultProxyCredentials = CredentialCache.DefaultCredentials
            }) { Timeout = TimeSpan.FromSeconds(15) };
            client.DefaultRequestHeaders.UserAgent.ParseAdd("ArcGISPro-GeoParquet-Addin/0.8");
            return client;
        }
        public static Task<string> GetStringAsync(Uri uri, CancellationToken token) => Client.GetStringAsync(uri, token);
    }
}
