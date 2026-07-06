using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public enum GersifyMapReviewMode
    {
        Unmatched,
        WeakLinks,
        Clear
    }

    public sealed class GersifyMapReviewResult
    {
        public bool Success { get; init; }
        public string Message { get; init; }
        public int FeatureCount { get; init; }
        public bool SourceLayerSelected { get; init; }
        internal Envelope ZoomExtent { get; init; }
    }

    public static class GersifyMapReviewService
    {
        private const int MaxSourceSelectionKeys = 3000;
        private const int SourceSelectionChunkSize = 400;

        public static async Task<GersifyMapReviewResult> ApplyReviewAsync(
            GersifyMapReviewMode mode,
            string outputFeatureClassPath,
            double acceptScoreThreshold,
            FeatureLayer sourceLayer = null,
            string sourceIdField = null,
            string outputSourceKeyField = "source_record_key")
        {
            if (string.IsNullOrWhiteSpace(outputFeatureClassPath) || !Directory.Exists(Path.GetDirectoryName(outputFeatureClassPath)))
            {
                return Failure("GERSified output layer was not found. Run GERSify again or add the output feature class to the map.");
            }

            var mapView = MapView.Active;
            if (mapView == null)
            {
                return Failure("Open a map view before reviewing linkage on the map.");
            }

            GersifyMapReviewResult reviewResult = await QueuedTask.Run(() =>
            {
                FeatureLayer outputLayer = FindFeatureLayerByCatalogPath(mapView.Map, outputFeatureClassPath);
                if (outputLayer == null)
                {
                    return Failure("Add the GERSified output layer to the active map, then try again.");
                }

                if (mode == GersifyMapReviewMode.Clear)
                {
                    return ClearReview(outputLayer, sourceLayer);
                }

                using FeatureClass featureClass = outputLayer.GetFeatureClass();
                FeatureClassDefinition definition = featureClass.GetDefinition();
                string gersIdField = ResolveField(definition, "gers_id", "GERS_ID");
                string scoreField = ResolveField(definition, "gers_match_score", "GERS_MATCH_SCORE");
                string sourceKeyField = ResolveField(definition, outputSourceKeyField, "source_record_key", "record_id");

                if (string.IsNullOrWhiteSpace(gersIdField))
                {
                    return Failure("The GERSified output layer is missing a gers_id field.");
                }

                if (mode == GersifyMapReviewMode.WeakLinks && string.IsNullOrWhiteSpace(scoreField))
                {
                    return Failure("The GERSified output layer is missing gers_match_score.");
                }

                string whereClause = mode switch
                {
                    GersifyMapReviewMode.Unmatched =>
                        GersifyMapReviewSql.BuildUnmatchedWhereClause(gersIdField),
                    GersifyMapReviewMode.WeakLinks =>
                        GersifyMapReviewSql.BuildWeakLinksWhereClause(gersIdField, scoreField, acceptScoreThreshold),
                    _ => string.Empty
                };

                outputLayer.SetDefinitionQuery(whereClause);
                outputLayer.ClearSelection();
                var query = new QueryFilter { WhereClause = whereClause };
                int featureCount = CountFeatures(featureClass, query);

                Envelope zoomExtent = null;
                using (Selection selection = outputLayer.Select(query, SelectionCombinationMethod.New, null, null, null))
                {
                    if (selection.GetCount() > 0)
                    {
                        Envelope layerExtent = outputLayer.QueryExtent();
                        if (layerExtent != null && !layerExtent.IsEmpty)
                        {
                            zoomExtent = layerExtent.Expand(1.15, 1.15, true);
                        }
                    }
                }

                bool sourceLayerSelected = false;
                if (mode == GersifyMapReviewMode.Unmatched &&
                    sourceLayer != null &&
                    !string.IsNullOrWhiteSpace(sourceIdField) &&
                    !string.IsNullOrWhiteSpace(sourceKeyField) &&
                    featureCount > 0 &&
                    featureCount <= MaxSourceSelectionKeys)
                {
                    sourceLayerSelected = TrySelectSourceFeatures(
                        sourceLayer,
                        sourceIdField,
                        featureClass,
                        sourceKeyField,
                        whereClause);
                }

                string reviewLabel = mode == GersifyMapReviewMode.Unmatched ? "unmatched" : "weak";
                string sourceNote = sourceLayerSelected
                    ? " Matching features are also selected on your authoritative source layer."
                    : mode == GersifyMapReviewMode.Unmatched && featureCount > MaxSourceSelectionKeys
                        ? " Use the filtered GERSified layer for large unmatched sets; source selection is skipped above 3,000 features."
                        : string.Empty;

                return new GersifyMapReviewResult
                {
                    Success = true,
                    FeatureCount = featureCount,
                    SourceLayerSelected = sourceLayerSelected,
                    ZoomExtent = zoomExtent,
                    Message = featureCount == 0
                        ? $"No {reviewLabel} features were found in the GERSified output layer."
                        : $"Showing {featureCount:N0} {reviewLabel} feature(s) on the GERSified output layer.{sourceNote}"
                };
            });

            if (reviewResult.ZoomExtent != null)
            {
                await mapView.ZoomToAsync(reviewResult.ZoomExtent);
            }

            return reviewResult;
        }

        private static GersifyMapReviewResult ClearReview(FeatureLayer outputLayer, FeatureLayer sourceLayer)
        {
            outputLayer.SetDefinitionQuery(string.Empty);
            outputLayer.ClearSelection();
            sourceLayer?.ClearSelection();

            return new GersifyMapReviewResult
            {
                Success = true,
                Message = "Cleared GERSify map review filters and selections."
            };
        }

        private static bool TrySelectSourceFeatures(
            FeatureLayer sourceLayer,
            string sourceIdField,
            FeatureClass outputFeatureClass,
            string outputSourceKeyField,
            string unmatchedWhereClause)
        {
            try
            {
                using FeatureClass sourceFeatureClass = sourceLayer.GetFeatureClass();
                FeatureClassDefinition sourceDefinition = sourceFeatureClass.GetDefinition();
                string resolvedSourceIdField = ResolveField(sourceDefinition, sourceIdField);
                if (string.IsNullOrWhiteSpace(resolvedSourceIdField))
                    return false;

                var unmatchedKeys = ReadDistinctFieldValues(outputFeatureClass, outputSourceKeyField, unmatchedWhereClause);
                if (unmatchedKeys.Count == 0)
                    return false;

                sourceLayer.ClearSelection();
                bool anySelected = false;
                foreach (string[] chunk in Chunk(unmatchedKeys, SourceSelectionChunkSize))
                {
                    string whereClause = GersifyMapReviewSql.BuildSourceKeysWhereClause(resolvedSourceIdField, chunk);
                    using Selection selection = sourceLayer.Select(
                        new QueryFilter { WhereClause = whereClause },
                        anySelected ? SelectionCombinationMethod.Add : SelectionCombinationMethod.New,
                        null,
                        null,
                        null);
                    anySelected = anySelected || selection.GetCount() > 0;
                }

                return anySelected;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Source-layer unmatched selection skipped: {ex.Message}");
                return false;
            }
        }

        private static List<string> ReadDistinctFieldValues(
            FeatureClass featureClass,
            string fieldName,
            string whereClause)
        {
            var values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var query = new QueryFilter
            {
                WhereClause = whereClause,
                SubFields = fieldName
            };

            using RowCursor cursor = featureClass.Search(query, false);
            while (cursor.MoveNext())
            {
                using Row row = cursor.Current;
                string value = Convert.ToString(row[fieldName], CultureInfo.InvariantCulture);
                if (!string.IsNullOrWhiteSpace(value))
                {
                    values.Add(value.Trim());
                }
            }

            return values.ToList();
        }

        private static int CountFeatures(FeatureClass featureClass, QueryFilter query) =>
            (int)featureClass.GetCount(query);

        private static FeatureLayer FindFeatureLayerByCatalogPath(Map map, string catalogPath)
        {
            if (map == null || string.IsNullOrWhiteSpace(catalogPath))
                return null;

            string normalizedTarget = NormalizeCatalogPath(catalogPath);
            string targetFeatureClassName = Path.GetFileName(normalizedTarget);

            return map.GetLayersAsFlattenedList()
                .OfType<FeatureLayer>()
                .FirstOrDefault(layer =>
                {
                    try
                    {
                        using FeatureClass featureClass = layer.GetFeatureClass();
                        string layerPath = featureClass.GetPath()?.LocalPath;
                        if (string.IsNullOrWhiteSpace(layerPath))
                            return false;

                        string normalizedLayerPath = NormalizeCatalogPath(layerPath);
                        if (string.Equals(normalizedLayerPath, normalizedTarget, StringComparison.OrdinalIgnoreCase))
                            return true;

                        return string.Equals(Path.GetFileName(normalizedLayerPath), targetFeatureClassName, StringComparison.OrdinalIgnoreCase) &&
                               string.Equals(
                                   Path.GetDirectoryName(normalizedLayerPath),
                                   Path.GetDirectoryName(normalizedTarget),
                                   StringComparison.OrdinalIgnoreCase);
                    }
                    catch
                    {
                        return false;
                    }
                });
        }

        private static string ResolveField(FeatureClassDefinition definition, params string[] candidates)
        {
            if (definition == null || candidates == null || candidates.Length == 0)
                return null;

            var available = definition.GetFields()
                .Select(field => field.Name)
                .Where(name => !string.IsNullOrWhiteSpace(name))
                .ToList();

            foreach (string candidate in candidates)
            {
                string match = available.FirstOrDefault(name =>
                    string.Equals(name, candidate, StringComparison.OrdinalIgnoreCase));
                if (!string.IsNullOrWhiteSpace(match))
                    return match;
            }

            return null;
        }

        private static string NormalizeCatalogPath(string path) =>
            Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        private static IEnumerable<string[]> Chunk(IReadOnlyList<string> values, int chunkSize)
        {
            for (int index = 0; index < values.Count; index += chunkSize)
            {
                int length = Math.Min(chunkSize, values.Count - index);
                var chunk = new string[length];
                for (int offset = 0; offset < length; offset++)
                {
                    chunk[offset] = values[index + offset];
                }

                yield return chunk;
            }
        }

        private static GersifyMapReviewResult Failure(string message) =>
            new() { Success = false, Message = message };
    }
}
