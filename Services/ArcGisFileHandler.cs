using System;
using System.IO;
using System.Threading.Tasks;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public class ArcGisFileHandler : IFileHandler
    {
        public async Task DeleteFileAsync(string path)
        {
            if (!File.Exists(path)) return;

            // Try Geoprocessing delete first (handles locks better in ArcGIS)
            await QueuedTask.Run(async () =>
            {
                try
                {
                    var env = Geoprocessing.MakeEnvironmentArray(overwriteoutput: true);
                    var parameters = Geoprocessing.MakeValueArray(path);
                    await Geoprocessing.ExecuteToolAsync("management.Delete", parameters, env);
                }
                catch
                {
                    // Ignore GP errors and fall through to direct delete
                }
            });

            // If still exists, try direct delete with retry
            if (File.Exists(path))
            {
                try
                {
                    File.Delete(path);
                }
                catch
                {
                    // Best effort
                }
            }
        }
    }
}
