using System.Text.RegularExpressions;

namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Persisted add-in settings for DuckDB httpfs proxy and memory limits.
    /// Defaults match the historical hard-coded DuckDB session values.
    /// </summary>
    public sealed class AddinSettings
    {
        public const string DefaultMemoryLimit = "2GB";
        public const string DefaultMaxMemory = "4GB";

        /// <summary>
        /// Optional manual HTTP proxy URL (host:port or http://host:port).
        /// When empty, the add-in auto-detects the Windows system proxy.
        /// </summary>
        public string HttpProxy { get; set; }

        public string HttpProxyUsername { get; set; }

        /// <summary>
        /// Stored locally for corporate authenticated proxies. Never log this value.
        /// </summary>
        public string HttpProxyPassword { get; set; }

        public string MemoryLimit { get; set; } = DefaultMemoryLimit;

        public string MaxMemory { get; set; } = DefaultMaxMemory;

        /// <summary>
        /// Accepts DuckDB-style sizes such as 2GB, 512MB, 1.5GB. Falls back when empty or invalid.
        /// </summary>
        public static string NormalizeMemorySize(string value, string fallback)
        {
            if (string.IsNullOrWhiteSpace(fallback))
                fallback = DefaultMemoryLimit;

            if (string.IsNullOrWhiteSpace(value))
                return fallback;

            string trimmed = Regex.Replace(value.Trim(), @"\s+", "");
            if (!Regex.IsMatch(trimmed, @"^\d+(\.\d+)?(K|M|G|T)B$", RegexOptions.IgnoreCase))
                return fallback;

            // DuckDB accepts 2GB / 512MB; keep the unit letters upper-case.
            return Regex.Replace(trimmed, @"[A-Za-z]+", m => m.Value.ToUpperInvariant());
        }

        public string EffectiveMemoryLimit => NormalizeMemorySize(MemoryLimit, DefaultMemoryLimit);

        public string EffectiveMaxMemory => NormalizeMemorySize(MaxMemory, DefaultMaxMemory);
    }
}
