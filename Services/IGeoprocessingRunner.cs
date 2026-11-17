using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Result of a geoprocessing tool invocation. Kept free of ArcGIS types so
    /// export plumbing can be unit-tested with a fake runner.
    /// </summary>
    public sealed class GeoprocessingToolResult
    {
        public GeoprocessingToolResult(bool succeeded, string messages = null, Exception exception = null)
        {
            Succeeded = succeeded;
            Messages = messages ?? string.Empty;
            Exception = exception;
        }

        public bool Succeeded { get; }
        public string Messages { get; }
        public Exception Exception { get; }

        public static GeoprocessingToolResult Success(string messages = null)
            => new(true, messages);

        public static GeoprocessingToolResult Failure(string messages, Exception exception = null)
            => new(false, messages, exception);
    }

    /// <summary>
    /// Thin wrapper around <c>Geoprocessing.ExecuteToolAsync</c> so file
    /// geodatabase export logic can run in unit tests without ArcGIS Pro.
    /// </summary>
    public interface IGeoprocessingRunner
    {
        Task<GeoprocessingToolResult> ExecuteAsync(
            string toolName,
            IReadOnlyList<object> values,
            bool overwriteOutput,
            CancellationToken cancellationToken = default);
    }
}
