using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Runs geoprocessing tools on the ArcGIS Pro MCT via
    /// <c>Geoprocessing.ExecuteToolAsync</c>. Outputs are not added to the map;
    /// callers add layers themselves after a successful export.
    /// </summary>
    public sealed class ArcGisGeoprocessingRunner : IGeoprocessingRunner
    {
        public async Task<GeoprocessingToolResult> ExecuteAsync(
            string toolName,
            IReadOnlyList<object> values,
            bool overwriteOutput,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                IGPResult result = await QueuedTask.Run(async () =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var parameters = Geoprocessing.MakeValueArray(values?.ToArray() ?? Array.Empty<object>());
                    var environment = Geoprocessing.MakeEnvironmentArray(overwriteoutput: overwriteOutput);
                    return await Geoprocessing.ExecuteToolAsync(
                        toolName,
                        parameters,
                        environment,
                        flags: GPExecuteToolFlags.None);
                });

                string messages = result?.Messages == null
                    ? string.Empty
                    : string.Join(" | ", result.Messages
                        .Select(message => message?.Text)
                        .Where(text => !string.IsNullOrWhiteSpace(text)));

                if (result == null)
                    return GeoprocessingToolResult.Failure($"Geoprocessing returned no result for {toolName}.");

                if (result.IsFailed)
                    return GeoprocessingToolResult.Failure(
                        string.IsNullOrWhiteSpace(messages)
                            ? $"{toolName} failed with no messages."
                            : messages);

                return GeoprocessingToolResult.Success(messages);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return GeoprocessingToolResult.Failure($"Geoprocessing threw while running {toolName}.", ex);
            }
        }
    }
}
