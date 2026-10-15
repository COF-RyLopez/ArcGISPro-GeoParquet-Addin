using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public class ReleaseValidator
    {
        private const string RELEASE_URL = "https://labs.overturemaps.org/data/releases.json";
        private const string S3_HTTPS_BASE = "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/";

        private static readonly HttpClient _httpClient = new(new HttpClientHandler
        {
            UseProxy = true,
            Proxy = WebRequest.DefaultWebProxy,
            DefaultProxyCredentials = CredentialCache.DefaultCredentials
        })
        {
            Timeout = TimeSpan.FromSeconds(5)
        };

        private static readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNameCaseInsensitive = true
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

        /// <summary>
        /// Validates that a release exists by checking releases.json and/or S3 directly.
        /// </summary>
        public static async Task<ValidationResult> ValidateReleaseAsync(string release)
        {
            if (string.IsNullOrWhiteSpace(release))
            {
                return new ValidationResult
                {
                    IsValid = false,
                    Message = "Release version is empty"
                };
            }

            try
            {
                var releaseInfo = await FetchReleaseInfoAsync();
                
                if (releaseInfo?.Releases != null && releaseInfo.Releases.Count > 0)
                {
                    bool exists = releaseInfo.Releases.Contains(release, StringComparer.OrdinalIgnoreCase);
                    return new ValidationResult
                    {
                        IsValid = exists,
                        LatestAvailable = releaseInfo.Latest,
                        AvailableReleases = releaseInfo.Releases,
                        Message = exists 
                            ? $"Release {release} is valid"
                            : $"Release {release} is no longer available from Overture; latest is {releaseInfo.Latest}"
                    };
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to fetch releases.json: {ex.Message}");
            }

            try
            {
                bool existsInS3 = await CheckReleaseExistsInS3Async(release);
                if (existsInS3)
                {
                    return new ValidationResult
                    {
                        IsValid = true,
                        Message = $"Release {release} exists (verified via S3)"
                    };
                }
                else
                {
                    return new ValidationResult
                    {
                        IsValid = false,
                        Message = $"Release {release} not found in S3"
                    };
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to check S3: {ex.Message}");
                return new ValidationResult
                {
                    IsValid = false,
                    Message = $"Unable to validate release {release}: {ex.Message}"
                };
            }
        }

        /// <summary>
        /// Fetches the releases.json from Overture.
        /// </summary>
        public static async Task<ReleaseInfo> FetchReleaseInfoAsync()
        {
            var url = Environment.GetEnvironmentVariable("OVERTURE_RELEASE_URL") ?? RELEASE_URL;
            var response = await _httpClient.GetStringAsync(url);
            return JsonSerializer.Deserialize<ReleaseInfo>(response, _jsonOptions);
        }

        /// <summary>
        /// Checks if a release folder exists in S3 via anonymous HTTPS HEAD request.
        /// </summary>
        private static async Task<bool> CheckReleaseExistsInS3Async(string release)
        {
            var testUrl = $"{S3_HTTPS_BASE}{release}/";
            
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
