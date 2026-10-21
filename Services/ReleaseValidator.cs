using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public class ReleaseValidator
    {
        /// <summary>
        /// Canonical discovery source documented by Overture
        /// (https://github.com/OvertureMaps/data#release-discovery).
        /// </summary>
        public const string DefaultStacCatalogUrl = "https://stac.overturemaps.org/catalog.json";

        /// <summary>
        /// Anonymous ListObjectsV2 listing of <c>release/</c> prefixes.
        /// Used when the STAC catalog is unreachable.
        /// </summary>
        public const string DefaultS3ListUrl =
            "https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&prefix=release/&delimiter=/";

        public const string DiscoveryUnreachableMessage =
            "Could not reach Overture release discovery. Check your network or pick a release.";

        private const string S3HttpsBase = "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/";

        private static readonly HttpClient _httpClient = new(new HttpClientHandler
        {
            UseProxy = true,
            Proxy = WebRequest.DefaultWebProxy,
            DefaultProxyCredentials = CredentialCache.DefaultCredentials
        })
        {
            Timeout = TimeSpan.FromSeconds(15)
        };

        public class ReleaseInfo
        {
            public string Latest { get; set; }
            public List<string> Releases { get; set; }
        }

        public class ValidationResult
        {
            public bool IsValid { get; set; }
            public string LatestAvailable { get; set; }
            public List<string> AvailableReleases { get; set; }
            public string Message { get; set; }
        }

        public static string RemovedReleaseMessage(string release, string newest)
        {
            if (!string.IsNullOrWhiteSpace(newest))
                return $"Release {release} is no longer available from Overture; newest available is {newest}";
            return $"Release {release} is no longer available from Overture.";
        }

        /// <summary>
        /// Validates that a release exists by consulting the STAC catalog
        /// (with S3 listing fallback). Never treats the literal "latest"
        /// as a real folder.
        /// </summary>
        public static async Task<ValidationResult> ValidateReleaseAsync(string release)
        {
            ReleaseInfo discovered = null;
            try
            {
                discovered = await DiscoverReleasesAsync();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to discover releases: {ex.Message}");
            }

            string newest = discovered?.Latest;
            var available = discovered?.Releases;

            if (!OvertureReleaseId.IsUsable(release))
            {
                if (available != null && available.Count > 0)
                {
                    string label = string.IsNullOrWhiteSpace(release)
                        ? "Release version is empty"
                        : $"Release '{release.Trim()}' is not a real Overture release folder";
                    return new ValidationResult
                    {
                        IsValid = false,
                        LatestAvailable = newest,
                        AvailableReleases = available,
                        Message = $"{label}; newest available is {newest}"
                    };
                }

                return new ValidationResult
                {
                    IsValid = false,
                    LatestAvailable = newest,
                    AvailableReleases = available,
                    Message = DiscoveryUnreachableMessage
                };
            }

            var normalized = release.Trim();
            OvertureReleaseId.TryNormalize(normalized, out var canonical);
            canonical ??= normalized;

            if (available != null && available.Count > 0)
            {
                bool exists = available.Contains(canonical, StringComparer.OrdinalIgnoreCase);
                return new ValidationResult
                {
                    IsValid = exists,
                    LatestAvailable = newest,
                    AvailableReleases = available,
                    Message = exists
                        ? $"Release {canonical} is valid"
                        : RemovedReleaseMessage(canonical, newest)
                };
            }

            try
            {
                bool existsInS3 = await CheckReleaseExistsInS3Async(canonical);
                if (existsInS3)
                {
                    return new ValidationResult
                    {
                        IsValid = true,
                        LatestAvailable = newest,
                        AvailableReleases = available,
                        Message = $"Release {canonical} exists (verified via S3)"
                    };
                }

                return new ValidationResult
                {
                    IsValid = false,
                    LatestAvailable = newest,
                    AvailableReleases = available,
                    Message = string.IsNullOrWhiteSpace(newest)
                        ? $"{RemovedReleaseMessage(canonical, newest)} {DiscoveryUnreachableMessage}"
                        : RemovedReleaseMessage(canonical, newest)
                };
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to check S3: {ex.Message}");
                return new ValidationResult
                {
                    IsValid = false,
                    LatestAvailable = newest,
                    AvailableReleases = available,
                    Message = DiscoveryUnreachableMessage
                };
            }
        }

        /// <summary>
        /// Resolves the published Overture releases. Primary source is the
        /// official STAC catalog; fallback is an anonymous S3 ListObjectsV2
        /// listing of <c>release/</c> prefixes.
        /// </summary>
        public static async Task<ReleaseInfo> DiscoverReleasesAsync()
        {
            var errors = new List<string>();

            var stacUrl = Environment.GetEnvironmentVariable("OVERTURE_STAC_CATALOG_URL")
                          ?? DefaultStacCatalogUrl;
            var fromStac = await TryReadReleasesAsync(
                stacUrl,
                OvertureReleaseId.ParseStacCatalog,
                "STAC catalog",
                errors);
            if (fromStac != null)
                return fromStac;

            var s3ListUrl = Environment.GetEnvironmentVariable("OVERTURE_S3_LIST_URL")
                            ?? DefaultS3ListUrl;
            var fromS3 = await TryReadReleasesAsync(
                s3ListUrl,
                OvertureReleaseId.ParseS3ListXml,
                "S3 listing",
                errors);
            if (fromS3 != null)
                return fromS3;

            throw new InvalidOperationException(
                errors.Count > 0
                    ? $"{DiscoveryUnreachableMessage} ({string.Join("; ", errors)})"
                    : DiscoveryUnreachableMessage);
        }

        /// <summary>
        /// Back-compat wrapper around <see cref="DiscoverReleasesAsync"/>.
        /// </summary>
        public static Task<ReleaseInfo> FetchReleaseInfoAsync() => DiscoverReleasesAsync();

        private static async Task<ReleaseInfo> TryReadReleasesAsync(
            string url,
            Func<string, IReadOnlyList<string>> parser,
            string sourceName,
            List<string> errors)
        {
            try
            {
                var body = await _httpClient.GetStringAsync(url);
                var releases = parser(body);
                if (releases == null || releases.Count == 0)
                {
                    errors.Add($"{sourceName} returned no release folders");
                    return null;
                }

                return ToReleaseInfo(releases);
            }
            catch (Exception ex)
            {
                errors.Add($"{sourceName}: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Failed to read {sourceName}: {ex.Message}");
                return null;
            }
        }

        internal static ReleaseInfo ToReleaseInfo(IReadOnlyList<string> releases)
        {
            var ordered = OvertureReleaseId.NormalizeMany(releases);
            return new ReleaseInfo
            {
                Latest = ordered.Count == 0 ? null : ordered[0],
                Releases = ordered.ToList()
            };
        }

        /// <summary>
        /// Checks if a release folder exists in S3 via anonymous HTTPS HEAD request.
        /// </summary>
        private static async Task<bool> CheckReleaseExistsInS3Async(string release)
        {
            if (!OvertureReleaseId.IsUsable(release))
                return false;

            var testUrl = $"{S3HttpsBase}{release.Trim()}/";

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Head, testUrl);
                using var response = await _httpClient.SendAsync(request);
                return response.IsSuccessStatusCode;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Detects if a DuckDB error indicates missing/pruned S3 files.
        /// </summary>
        public static bool IsMissingFileError(string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(errorMessage))
                return false;

            return errorMessage.Contains("No files found", StringComparison.OrdinalIgnoreCase) ||
                   errorMessage.Contains("glob pattern matched 0 files", StringComparison.OrdinalIgnoreCase) ||
                   errorMessage.Contains("does not exist", StringComparison.OrdinalIgnoreCase) ||
                   errorMessage.Contains("404", StringComparison.OrdinalIgnoreCase);
        }
    }
}
