using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Persists geocode results as a point feature class in the project's
    /// default geodatabase and adds it to the active map — mirroring the
    /// Locate pane's "Add To Feature Class".
    /// </summary>
    public static class GeocodeResultWriter
    {
        /// <param name="candidates">Results to persist (WGS84 lon/lat).</param>
        /// <param name="featureClassName">Output feature class / layer name.</param>
        /// <param name="sourceQueries">Optional per-candidate input query
        /// (parallel to <paramref name="candidates"/>), written to a 'query'
        /// field for file-geocoding output.</param>
        /// <returns>The output feature class path.</returns>
        public static async Task<string> WriteToFeatureClassAsync(
            IReadOnlyList<GeocodeCandidate> candidates,
            string featureClassName,
            IReadOnlyList<string> sourceQueries = null)
        {
            if (candidates == null || candidates.Count == 0)
            {
                throw new ArgumentException("No candidates to write.", nameof(candidates));
            }

            string csvPath = Path.Combine(Path.GetTempPath(), $"{featureClassName}.csv");
            var sb = new StringBuilder();
            sb.AppendLine("query,label,longitude,latitude,source,match,confidence,score");
            for (int i = 0; i < candidates.Count; i++)
            {
                var c = candidates[i];
                string query = sourceQueries != null && i < sourceQueries.Count ? sourceQueries[i] : string.Empty;
                sb.AppendLine(string.Join(",",
                    Csv(query),
                    Csv(c.DisplayLabel),
                    c.Longitude.ToString("G", CultureInfo.InvariantCulture),
                    c.Latitude.ToString("G", CultureInfo.InvariantCulture),
                    Csv(c.SourceType),
                    Csv(c.MatchTier),
                    Csv(c.ConfidenceTier),
                    c.Score.ToString(CultureInfo.InvariantCulture)));
            }
            File.WriteAllText(csvPath, sb.ToString(), Encoding.UTF8);

            try
            {
                string outputPath = Path.Combine(Project.Current.DefaultGeodatabasePath, featureClassName);
                var parameters = Geoprocessing.MakeValueArray(
                    csvPath, outputPath, "longitude", "latitude", "", SpatialReferences.WGS84);
                var result = await Geoprocessing.ExecuteToolAsync(
                    "management.XYTableToPoint", parameters, null, flags: GPExecuteToolFlags.None);
                if (result.IsFailed)
                {
                    throw new Exception("XY Table To Point failed: " +
                        string.Join(" | ", result.Messages.Select(m => m.Text)));
                }

                int outputCount = await CountFeaturesAsync(outputPath);
                if (outputCount != candidates.Count)
                {
                    throw new Exception(
                        $"Output feature class contains {outputCount} row(s), but {candidates.Count} geocode result(s) were expected.");
                }

                await QueuedTask.Run(() =>
                {
                    var map = MapView.Active?.Map;
                    if (map != null)
                    {
                        LayerFactory.Instance.CreateLayer(new Uri(outputPath), map, layerName: featureClassName);
                    }
                });

                return outputPath;
            }
            finally
            {
                try { File.Delete(csvPath); } catch { /* temp file; best effort */ }
            }
        }

        private static async Task<int> CountFeaturesAsync(string featureClassPath)
        {
            return await QueuedTask.Run(() =>
            {
                string gdbPath = Path.GetDirectoryName(featureClassPath);
                string featureClassName = Path.GetFileName(featureClassPath);
                var gdbConnectionPath = new FileGeodatabaseConnectionPath(new Uri(gdbPath));
                using var gdb = new Geodatabase(gdbConnectionPath);
                using var featureClass = gdb.OpenDataset<FeatureClass>(featureClassName);
                using var cursor = featureClass.Search(new QueryFilter(), false);

                int count = 0;
                while (cursor.MoveNext())
                {
                    count++;
                }

                return count;
            });
        }

        private static string Csv(string value)
        {
            value ??= string.Empty;
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }
    }
}
