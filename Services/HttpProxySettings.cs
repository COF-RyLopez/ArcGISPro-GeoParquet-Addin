using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Net;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Resolved HTTP proxy that DuckDB httpfs should use
    /// (<c>http_proxy</c>, <c>http_proxy_username</c>, <c>http_proxy_password</c>).
    /// </summary>
    public sealed class HttpProxySettings
    {
        /// <summary>
        /// Public Overture S3 endpoint used when auto-detecting a per-URL system proxy.
        /// </summary>
        public const string DefaultS3TargetUrl = "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/";

        public const string SourceManual = "manual";
        public const string SourceSystem = "system";
        public const string SourceNone = "none";

        /// <summary>DuckDB <c>http_proxy</c> value, e.g. <c>http://proxy.corp:8080</c>.</summary>
        public string ProxyUrl { get; init; }

        public string Username { get; init; }

        public string Password { get; init; }

        /// <summary><see cref="SourceManual"/>, <see cref="SourceSystem"/>, or <see cref="SourceNone"/>.</summary>
        public string Source { get; init; } = SourceNone;

        public bool IsConfigured => !string.IsNullOrWhiteSpace(ProxyUrl);

        /// <summary>
        /// Human-readable description that never includes the proxy password.
        /// </summary>
        public string ToLogSafeDescription()
        {
            if (!IsConfigured)
                return "no proxy";

            string auth = string.IsNullOrEmpty(Username)
                ? "no credentials"
                : $"user={Username}";
            return $"{Source} proxy {ProxyUrl} ({auth})";
        }

        /// <summary>
        /// Strips the proxy password from exception text or SQL so it is never logged.
        /// </summary>
        public string RedactSecrets(string text)
        {
            if (string.IsNullOrEmpty(text) || string.IsNullOrEmpty(Password))
                return text;
            return text.Replace(Password, "***", StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Builds <see cref="HttpProxySettings"/> from a manual override or a detected
    /// system proxy, and emits the DuckDB httpfs SET statements.
    /// </summary>
    public static class HttpProxySettingsBuilder
    {
        /// <summary>
        /// Manual proxy wins whenever a URL is supplied; otherwise the detected
        /// Windows system proxy is used. Empty on both sides means no SET statements.
        /// </summary>
        public static HttpProxySettings Resolve(
            string manualProxy,
            string manualUsername,
            string manualPassword,
            HttpProxySettings detectedSystemProxy)
        {
            var manual = Parse(manualProxy, manualUsername, manualPassword, HttpProxySettings.SourceManual);
            if (manual.IsConfigured)
                return manual;

            if (detectedSystemProxy != null && detectedSystemProxy.IsConfigured)
                return detectedSystemProxy;

            return new HttpProxySettings { Source = HttpProxySettings.SourceNone };
        }

        public static HttpProxySettings Resolve(AddinSettings settings, HttpProxySettings detectedSystemProxy) =>
            Resolve(settings?.HttpProxy, settings?.HttpProxyUsername, settings?.HttpProxyPassword, detectedSystemProxy);

        /// <summary>
        /// Parses a proxy URL or host:port, optionally with userinfo credentials.
        /// Manual username/password arguments override credentials embedded in the URL.
        /// </summary>
        public static HttpProxySettings Parse(
            string proxy,
            string username = null,
            string password = null,
            string source = HttpProxySettings.SourceManual)
        {
            if (string.IsNullOrWhiteSpace(proxy))
            {
                return new HttpProxySettings { Source = source ?? HttpProxySettings.SourceNone };
            }

            string parsedUser = username;
            string parsedPassword = password;
            string normalized = proxy.Trim();

            if (TryCreateProxyUri(normalized, out var uri))
            {
                if (!string.IsNullOrEmpty(uri.UserInfo))
                {
                    SplitUserInfo(uri.UserInfo, out string urlUser, out string urlPassword);
                    if (string.IsNullOrEmpty(parsedUser))
                        parsedUser = urlUser;
                    if (string.IsNullOrEmpty(parsedPassword))
                        parsedPassword = urlPassword;
                }

                int port = uri.IsDefaultPort ? DefaultPort(uri.Scheme) : uri.Port;
                normalized = $"{uri.Scheme}://{uri.Host}:{port}";
            }
            else
            {
                normalized = NormalizeProxyUrl(normalized);
            }

            return new HttpProxySettings
            {
                ProxyUrl = normalized,
                Username = string.IsNullOrWhiteSpace(parsedUser) ? null : parsedUser.Trim(),
                Password = string.IsNullOrEmpty(parsedPassword) ? null : parsedPassword,
                Source = source ?? HttpProxySettings.SourceManual
            };
        }

        /// <summary>
        /// Picks the matching protocol entry from a WinHTTP proxy list such as
        /// <c>http=proxy:80;https=proxy:443</c>, then parses it.
        /// </summary>
        public static HttpProxySettings ParseWinHttpProxyList(
            string proxyList,
            string targetUrl,
            string username = null,
            string password = null)
        {
            string selected = SelectProxyFromWinHttpList(proxyList, targetUrl);
            return Parse(selected, username, password, HttpProxySettings.SourceSystem);
        }

        /// <summary>
        /// Resolves <paramref name="proxy"/> for <paramref name="target"/> the same way
        /// <see cref="WebRequest.GetSystemWebProxy"/> is intended to be used.
        /// </summary>
        public static HttpProxySettings FromWebProxy(IWebProxy proxy, Uri target)
        {
            if (proxy == null || target == null)
                return new HttpProxySettings { Source = HttpProxySettings.SourceSystem };

            try
            {
                if (proxy.IsBypassed(target))
                    return new HttpProxySettings { Source = HttpProxySettings.SourceSystem };

                Uri proxyUri = proxy.GetProxy(target);
                if (proxyUri == null || IsDirect(proxyUri, target))
                    return new HttpProxySettings { Source = HttpProxySettings.SourceSystem };

                string user = null;
                string password = null;
                if (!string.IsNullOrEmpty(proxyUri.UserInfo))
                    SplitUserInfo(proxyUri.UserInfo, out user, out password);

                TryCopyCredentials(proxy.Credentials, proxyUri, target, ref user, ref password);

                int port = proxyUri.IsDefaultPort ? DefaultPort(proxyUri.Scheme) : proxyUri.Port;
                return new HttpProxySettings
                {
                    ProxyUrl = $"{proxyUri.Scheme}://{proxyUri.Host}:{port}",
                    Username = string.IsNullOrWhiteSpace(user) ? null : user,
                    Password = string.IsNullOrEmpty(password) ? null : password,
                    Source = HttpProxySettings.SourceSystem
                };
            }
            catch
            {
                return new HttpProxySettings { Source = HttpProxySettings.SourceSystem };
            }
        }

        /// <summary>
        /// DuckDB httpfs configuration options. Empty when no proxy is configured.
        /// The returned SQL embeds the password and must never be written to logs.
        /// </summary>
        public static string BuildDuckDbSetSql(HttpProxySettings settings)
        {
            if (settings == null || !settings.IsConfigured)
                return string.Empty;

            string proxy = GeoParquetSql.EscapeSqlLiteral(settings.ProxyUrl);
            var sql = $"SET http_proxy='{proxy}';";

            if (!string.IsNullOrEmpty(settings.Username))
                sql += $"\nSET http_proxy_username='{GeoParquetSql.EscapeSqlLiteral(settings.Username)}';";

            if (!string.IsNullOrEmpty(settings.Password))
                sql += $"\nSET http_proxy_password='{GeoParquetSql.EscapeSqlLiteral(settings.Password)}';";

            return sql;
        }

        public static string BuildSessionSettingsSql(
            AddinSettings addinSettings,
            HttpProxySettings proxy,
            int threads,
            string tempDirectory)
        {
            string memoryLimit = AddinSettings.NormalizeMemorySize(
                addinSettings?.MemoryLimit, AddinSettings.DefaultMemoryLimit);
            string maxMemory = AddinSettings.NormalizeMemorySize(
                addinSettings?.MaxMemory, AddinSettings.DefaultMaxMemory);
            string tempDir = GeoParquetSql.EscapeSqlLiteral(
                (tempDirectory ?? string.Empty).Replace('\\', '/'));

            string proxySql = BuildDuckDbSetSql(proxy);
            string prefix = string.IsNullOrEmpty(proxySql) ? string.Empty : proxySql + "\n";

            return prefix +
                   $@"SET s3_region='us-west-2';
SET enable_http_metadata_cache=true;
SET enable_object_cache=true;
SET enable_progress_bar=true;
SET memory_limit='{GeoParquetSql.EscapeSqlLiteral(memoryLimit)}';
SET max_memory='{GeoParquetSql.EscapeSqlLiteral(maxMemory)}';
SET temp_directory='{tempDir}';
SET threads={Math.Max(1, threads)};";
        }

        /// <summary>
        /// Selects the https= or http= entry from a WinHTTP proxy list, or the whole
        /// string when it is a single host:port.
        /// </summary>
        public static string SelectProxyFromWinHttpList(string proxyList, string targetUrl)
        {
            if (string.IsNullOrWhiteSpace(proxyList))
                return null;

            string trimmed = proxyList.Trim();
            if (trimmed.IndexOf('=') < 0)
                return NormalizeProxyUrl(trimmed);

            string scheme = "https";
            if (Uri.TryCreate(targetUrl, UriKind.Absolute, out var target) &&
                !string.IsNullOrEmpty(target.Scheme))
            {
                scheme = target.Scheme;
            }

            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in trimmed.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                string entry = part.Trim();
                int eq = entry.IndexOf('=');
                if (eq <= 0)
                    continue;
                map[entry.Substring(0, eq).Trim()] = entry.Substring(eq + 1).Trim();
            }

            if (map.TryGetValue(scheme, out var selected) && !string.IsNullOrWhiteSpace(selected))
                return NormalizeProxyUrl(selected);
            if (map.TryGetValue("https", out selected) && !string.IsNullOrWhiteSpace(selected))
                return NormalizeProxyUrl(selected);
            if (map.TryGetValue("http", out selected) && !string.IsNullOrWhiteSpace(selected))
                return NormalizeProxyUrl(selected);

            return null;
        }

        public static string NormalizeProxyUrl(string proxy)
        {
            if (string.IsNullOrWhiteSpace(proxy))
                return null;

            string trimmed = proxy.Trim();
            if (trimmed.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith("socks://", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith("socks5://", StringComparison.OrdinalIgnoreCase))
            {
                return trimmed;
            }

            return "http://" + trimmed;
        }

        public static IWebProxy ToWebProxy(HttpProxySettings settings)
        {
            if (settings == null || !settings.IsConfigured)
                return null;

            var webProxy = new WebProxy(settings.ProxyUrl);
            if (!string.IsNullOrEmpty(settings.Username))
            {
                webProxy.Credentials = new NetworkCredential(settings.Username, settings.Password ?? string.Empty);
            }
            else
            {
                webProxy.Credentials = CredentialCache.DefaultCredentials;
            }
            return webProxy;
        }

        private static bool TryCreateProxyUri(string proxy, out Uri uri)
        {
            uri = null;
            string candidate = NormalizeProxyUrl(proxy);
            return candidate != null &&
                   Uri.TryCreate(candidate, UriKind.Absolute, out uri) &&
                   !string.IsNullOrEmpty(uri.Host);
        }

        private static void SplitUserInfo(string userInfo, out string user, out string password)
        {
            user = null;
            password = null;
            if (string.IsNullOrEmpty(userInfo))
                return;

            int colon = userInfo.IndexOf(':');
            if (colon < 0)
            {
                user = Uri.UnescapeDataString(userInfo);
                return;
            }

            user = Uri.UnescapeDataString(userInfo.Substring(0, colon));
            password = Uri.UnescapeDataString(userInfo.Substring(colon + 1));
        }

        private static void TryCopyCredentials(
            ICredentials credentials,
            Uri proxyUri,
            Uri target,
            ref string user,
            ref string password)
        {
            if (credentials == null || !string.IsNullOrEmpty(user))
                return;

            NetworkCredential network = null;
            try
            {
                network = credentials.GetCredential(proxyUri, "Basic")
                          ?? credentials.GetCredential(target, "Basic")
                          ?? credentials.GetCredential(proxyUri, string.Empty);
            }
            catch
            {
                // DefaultNetworkCredentials can throw for some auth types.
            }

            if (network == null)
                return;

            string name = network.UserName;
            if (string.IsNullOrEmpty(name))
                return;

            user = string.IsNullOrEmpty(network.Domain) ? name : $"{network.Domain}\\{name}";
            if (string.IsNullOrEmpty(password))
                password = network.Password;
        }

        private static bool IsDirect(Uri proxyUri, Uri target)
        {
            return Uri.Compare(
                       proxyUri,
                       target,
                       UriComponents.SchemeAndServer | UriComponents.Path,
                       UriFormat.Unescaped,
                       StringComparison.OrdinalIgnoreCase) == 0;
        }

        private static int DefaultPort(string scheme)
        {
            if (string.Equals(scheme, "https", StringComparison.OrdinalIgnoreCase))
                return 443;
            if (string.Equals(scheme, "socks", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(scheme, "socks5", StringComparison.OrdinalIgnoreCase))
                return 1080;
            return 80;
        }
    }
}
