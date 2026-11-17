using System;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Maps DuckDB/httpfs connectivity failures to an actionable user message
    /// so locked-down networks never surface a raw DuckDB stack in the UI.
    /// </summary>
    public static class DuckDbNetworkErrors
    {
        public const string OvertureS3UnreachableMessage =
            "Can't reach Overture S3. Check the proxy or network.";

        private static readonly string[] NetworkTokens =
        {
            "unable to connect",
            "could not connect",
            "couldn't connect",
            "connection timed out",
            "connection refused",
            "connection reset",
            "connection error for http",
            "network is unreachable",
            "failed to establish",
            "could not establish",
            "could not resolve",
            "name or service not known",
            "failed to download",
            "operation timed out",
            "no route to host",
            "temporary failure in name resolution",
            "proxy",
            "ssl error",
            "certificate",
            "http error",
            "httpexception",
            "httpclientexception",
            "error during http",
            "failed to request",
            "network"
        };

        public static bool IsNetworkOrProxyFailure(string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(errorMessage))
                return false;

            // A 404 / missing glob means S3 was reachable; keep the release-missing path.
            if (ReleaseValidator.IsMissingFileError(errorMessage))
                return false;

            foreach (var token in NetworkTokens)
            {
                if (errorMessage.Contains(token, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Returns the actionable S3/proxy message for connectivity failures;
        /// otherwise returns the original message.
        /// </summary>
        public static string ToUserFacingMessage(string errorMessage)
        {
            return IsNetworkOrProxyFailure(errorMessage)
                ? OvertureS3UnreachableMessage
                : errorMessage;
        }
    }
}
