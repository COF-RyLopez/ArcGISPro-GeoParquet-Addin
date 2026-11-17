using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
using System.Net;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class HttpProxySettingsTests
    {
        [Fact]
        public void Resolve_UsesManualProxy_WhenUrlIsSet()
        {
            var system = HttpProxySettingsBuilder.Parse("http://system.example:3128", source: HttpProxySettings.SourceSystem);
            var resolved = HttpProxySettingsBuilder.Resolve(
                "proxy.corp.example:8080",
                "alice",
                "s3cret!",
                system);

            Assert.True(resolved.IsConfigured);
            Assert.Equal(HttpProxySettings.SourceManual, resolved.Source);
            Assert.Equal("http://proxy.corp.example:8080", resolved.ProxyUrl);
            Assert.Equal("alice", resolved.Username);
            Assert.Equal("s3cret!", resolved.Password);
        }

        [Fact]
        public void Resolve_FallsBackToSystemProxy_WhenManualIsBlank()
        {
            var system = HttpProxySettingsBuilder.Parse("http://system.example:3128", "bob", "from-system", HttpProxySettings.SourceSystem);
            var resolved = HttpProxySettingsBuilder.Resolve("  ", null, null, system);

            Assert.Equal(HttpProxySettings.SourceSystem, resolved.Source);
            Assert.Equal("http://system.example:3128", resolved.ProxyUrl);
            Assert.Equal("bob", resolved.Username);
            Assert.Equal("from-system", resolved.Password);
        }

        [Fact]
        public void Resolve_ReturnsUnconfigured_WhenNeitherManualNorSystemIsSet()
        {
            var resolved = HttpProxySettingsBuilder.Resolve(null, null, null, detectedSystemProxy: null);

            Assert.False(resolved.IsConfigured);
            Assert.Equal(HttpProxySettings.SourceNone, resolved.Source);
            Assert.Equal(string.Empty, HttpProxySettingsBuilder.BuildDuckDbSetSql(resolved));
        }

        [Theory]
        [InlineData("proxy.example:8080", "http://proxy.example:8080")]
        [InlineData("http://proxy.example:8080", "http://proxy.example:8080")]
        [InlineData("https://secure-proxy.example:8443", "https://secure-proxy.example:8443")]
        public void Parse_NormalizesHostPortAndUrls(string input, string expected)
        {
            var parsed = HttpProxySettingsBuilder.Parse(input);
            Assert.Equal(expected, parsed.ProxyUrl);
        }

        [Fact]
        public void Parse_ReadsUserInfoFromUrl_UnlessManualCredentialsOverride()
        {
            var fromUrl = HttpProxySettingsBuilder.Parse("http://urluser:urlpass@proxy.example:8080");
            Assert.Equal("http://proxy.example:8080", fromUrl.ProxyUrl);
            Assert.Equal("urluser", fromUrl.Username);
            Assert.Equal("urlpass", fromUrl.Password);

            var overridden = HttpProxySettingsBuilder.Parse(
                "http://urluser:urlpass@proxy.example:8080",
                "manual-user",
                "manual-pass");
            Assert.Equal("manual-user", overridden.Username);
            Assert.Equal("manual-pass", overridden.Password);
        }

        [Fact]
        public void Parse_DecodesUrlEncodedUserInfo()
        {
            var parsed = HttpProxySettingsBuilder.Parse("http://al%40ice:p%40ss%3Aword@proxy.example:8080");
            Assert.Equal("al@ice", parsed.Username);
            Assert.Equal("p@ss:word", parsed.Password);
        }

        [Fact]
        public void SelectProxyFromWinHttpList_PrefersHttpsForS3Target()
        {
            string selected = HttpProxySettingsBuilder.SelectProxyFromWinHttpList(
                "http=http-proxy.example:80;https=https-proxy.example:443",
                HttpProxySettings.DefaultS3TargetUrl);

            Assert.Equal("http://https-proxy.example:443", selected);
        }

        [Fact]
        public void ParseWinHttpProxyList_UsesSingleHostPortEntry()
        {
            var parsed = HttpProxySettingsBuilder.ParseWinHttpProxyList(
                "corp-proxy.example:8080",
                HttpProxySettings.DefaultS3TargetUrl);

            Assert.True(parsed.IsConfigured);
            Assert.Equal(HttpProxySettings.SourceSystem, parsed.Source);
            Assert.Equal("http://corp-proxy.example:8080", parsed.ProxyUrl);
        }

        [Fact]
        public void FromWebProxy_ReturnsSystemProxy_ForDestinationThatIsNotDirect()
        {
            var webProxy = new FakeWebProxy
            {
                ProxyUri = new Uri("http://detected.example:8888"),
                Bypass = false
            };
            var settings = HttpProxySettingsBuilder.FromWebProxy(
                webProxy,
                new Uri(HttpProxySettings.DefaultS3TargetUrl));

            Assert.True(settings.IsConfigured);
            Assert.Equal(HttpProxySettings.SourceSystem, settings.Source);
            Assert.Equal("http://detected.example:8888", settings.ProxyUrl);
        }

        [Fact]
        public void FromWebProxy_ReturnsUnconfigured_WhenDestinationIsBypassed()
        {
            var webProxy = new FakeWebProxy
            {
                ProxyUri = new Uri("http://detected.example:8888"),
                Bypass = true
            };
            var settings = HttpProxySettingsBuilder.FromWebProxy(
                webProxy,
                new Uri(HttpProxySettings.DefaultS3TargetUrl));

            Assert.False(settings.IsConfigured);
        }

        private sealed class FakeWebProxy : IWebProxy
        {
            public Uri ProxyUri { get; set; }
            public bool Bypass { get; set; }
            public ICredentials Credentials { get; set; }
            public Uri GetProxy(Uri destination) => Bypass ? destination : ProxyUri;
            public bool IsBypassed(Uri host) => Bypass;
        }

        [Fact]
        public void BuildDuckDbSetSql_EmitsHttpfsProxySettings_AndEscapesQuotes()
        {
            var settings = new HttpProxySettings
            {
                ProxyUrl = "http://proxy.example:8080",
                Username = "O'Brien",
                Password = "p@ss'word",
                Source = HttpProxySettings.SourceManual
            };

            string sql = HttpProxySettingsBuilder.BuildDuckDbSetSql(settings);

            Assert.Contains("SET http_proxy='http://proxy.example:8080';", sql);
            Assert.Contains("SET http_proxy_username='O''Brien';", sql);
            Assert.Contains("SET http_proxy_password='p@ss''word';", sql);
        }

        [Fact]
        public void BuildDuckDbSetSql_OmitsCredentials_WhenOnlyUrlIsSet()
        {
            var settings = HttpProxySettingsBuilder.Parse("http://proxy.example:8080");
            string sql = HttpProxySettingsBuilder.BuildDuckDbSetSql(settings);

            Assert.Equal("SET http_proxy='http://proxy.example:8080';", sql);
            Assert.DoesNotContain("http_proxy_username", sql);
            Assert.DoesNotContain("http_proxy_password", sql);
        }

        [Fact]
        public void ToLogSafeDescription_NeverIncludesPassword()
        {
            const string password = "super-secret-proxy-password";
            var settings = HttpProxySettingsBuilder.Parse(
                "http://alice:super-secret-proxy-password@proxy.example:8080");

            string description = settings.ToLogSafeDescription();

            Assert.DoesNotContain(password, description);
            Assert.Contains("proxy.example", description);
            Assert.Contains("alice", description);
            Assert.Contains(HttpProxySettings.SourceManual, description);
            Assert.Equal(
                "SET http_proxy_password='***';",
                settings.RedactSecrets("SET http_proxy_password='super-secret-proxy-password';"));
        }

        [Fact]
        public void BuildSessionSettingsSql_KeepsMemoryDefaults_AndDoesNotLogPassword()
        {
            var addin = new AddinSettings();
            var proxy = HttpProxySettingsBuilder.Parse("http://user:hidden-pass@proxy.example:8080");

            string sql = HttpProxySettingsBuilder.BuildSessionSettingsSql(
                addin, proxy, threads: 8, tempDirectory: @"C:\Temp");

            Assert.Contains("SET http_proxy='http://proxy.example:8080';", sql);
            Assert.Contains("SET http_proxy_username='user';", sql);
            Assert.Contains("SET http_proxy_password='hidden-pass';", sql);
            Assert.Contains("SET memory_limit='2GB';", sql);
            Assert.Contains("SET max_memory='4GB';", sql);
            Assert.Contains("SET threads=8;", sql);
            Assert.Contains("SET temp_directory='C:/Temp';", sql);

            Assert.DoesNotContain("hidden-pass", proxy.ToLogSafeDescription());
        }

        [Theory]
        [InlineData("4GB", "4GB")]
        [InlineData("512mb", "512MB")]
        [InlineData("1.5 gb", "1.5GB")]
        [InlineData("", "2GB")]
        [InlineData("not-a-size", "2GB")]
        public void NormalizeMemorySize_AcceptsDuckDbSizesOrFallsBack(string input, string expected)
        {
            Assert.Equal(expected, AddinSettings.NormalizeMemorySize(input, AddinSettings.DefaultMemoryLimit));
        }

        [Fact]
        public void BuildSessionSettingsSql_UsesConfiguredMemoryLimits()
        {
            var addin = new AddinSettings { MemoryLimit = "8GB", MaxMemory = "16GB" };
            string sql = HttpProxySettingsBuilder.BuildSessionSettingsSql(
                addin, proxy: null, threads: 1, tempDirectory: "/tmp");

            Assert.Contains("SET memory_limit='8GB';", sql);
            Assert.Contains("SET max_memory='16GB';", sql);
            Assert.DoesNotContain("http_proxy", sql);
        }
    }

    public class DuckDbExtensionPolicyTests
    {
        [Fact]
        public void ShouldAttemptNetworkInstall_IsFalse_WhenBothBundledExtensionsExist()
        {
            string dir = Path.Combine(Path.GetTempPath(), "DuckDbExt-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            try
            {
                File.WriteAllText(DuckDbExtensionPolicy.SpatialPath(dir), "spatial");
                File.WriteAllText(DuckDbExtensionPolicy.HttpfsPath(dir), "httpfs");

                Assert.True(DuckDbExtensionPolicy.BundledExtensionsPresent(dir));
                Assert.False(DuckDbExtensionPolicy.ShouldAttemptNetworkInstall(dir));
                string sql = DuckDbExtensionPolicy.BuildBundledLoadSql(dir);
                Assert.Contains("LOAD '", sql);
                Assert.Contains("spatial.duckdb_extension", sql);
                Assert.Contains("httpfs.duckdb_extension", sql);
                Assert.DoesNotContain("INSTALL", sql);
            }
            finally
            {
                Directory.Delete(dir, recursive: true);
            }
        }

        [Fact]
        public void ShouldAttemptNetworkInstall_IsTrue_WhenBundledExtensionsAreMissing()
        {
            string dir = Path.Combine(Path.GetTempPath(), "DuckDbExt-missing-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            try
            {
                Assert.False(DuckDbExtensionPolicy.BundledExtensionsPresent(dir));
                Assert.True(DuckDbExtensionPolicy.ShouldAttemptNetworkInstall(dir));
            }
            finally
            {
                Directory.Delete(dir, recursive: true);
            }
        }
    }

    public class DuckDbNetworkErrorsTests
    {
        [Theory]
        [InlineData("HTTP Error: Failed to connect to S3")]
        [InlineData("IO Error: Connection error for HTTP HEAD to https://overturemaps-us-west-2.s3.amazonaws.com")]
        [InlineData("Unable to connect to remote host")]
        [InlineData("Connection timed out")]
        [InlineData("Proxy CONNECT failed")]
        public void IsNetworkOrProxyFailure_DetectsConnectivityErrors(string message)
        {
            Assert.True(DuckDbNetworkErrors.IsNetworkOrProxyFailure(message));
            Assert.Equal(
                DuckDbNetworkErrors.OvertureS3UnreachableMessage,
                DuckDbNetworkErrors.ToUserFacingMessage(message));
        }

        [Theory]
        [InlineData("No files found that match the glob pattern")]
        [InlineData("HTTP Error 404: Not Found")]
        [InlineData("Invalid SQL syntax")]
        [InlineData(null)]
        public void IsNetworkOrProxyFailure_DoesNotTreatMissingFilesOrUnrelatedErrorsAsProxy(string message)
        {
            Assert.False(DuckDbNetworkErrors.IsNetworkOrProxyFailure(message));
            Assert.Equal(message, DuckDbNetworkErrors.ToUserFacingMessage(message));
        }
    }
}
