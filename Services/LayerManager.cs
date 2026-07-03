using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Models;

namespace DuckDBGeoparquet.Services
{
    public class LayerManager
    {
        private readonly List<LayerCreationInfo> _pendingLayers = new();
        private static readonly bool VerbosePerLayerDebugLogging = false;

        // Default draw order fallback used when a style does not provide explicit draw ranks.
        // Higher rank draws above lower rank.
        private static readonly Dictionary<string, int> DefaultDrawOrderRanks = new(StringComparer.OrdinalIgnoreCase)
        {
            // Point overlays
            { "place:point", 330 },
            { "division:point", 326 },
            { "address:point", 318 },
            { "connector:point", 314 },
            { "infrastructure:point", 312 },
            { "land:point", 308 },
            { "water:point", 306 },

            // Linear networks and boundaries
            { "segment:line", 304 },
            { "division_boundary:line", 300 },
            { "infrastructure:line", 296 },
            { "water:line", 294 },
            { "land_use:line", 292 },
            { "land:line", 290 },

            // Polygonal layers (buildings above polygons, water at the bottom)
            { "building:polygon", 286 },
            { "building_part:polygon", 284 },
            { "infrastructure:polygon", 278 },
            { "division_area:polygon", 276 },
            { "division:polygon", 274 },
            { "land_use:polygon", 272 },
            { "land_cover:polygon", 268 },
            { "land:polygon", 264 },
            { "bathymetry:polygon", 260 },
            { "water:polygon", 256 },

            // Type-only fallbacks
            { "building", 286 },
            { "building_part", 284 },
            { "segment", 304 },
            { "address", 318 },
            { "place", 330 },
            { "water", 256 },
            { "bathymetry", 260 },
        };

        public int LastAddedLayerCount { get; private set; }
        public string LastAppliedStyleName { get; private set; } = "default";

        public void AddPendingLayer(LayerCreationInfo layerInfo)
        {
            _pendingLayers.Add(layerInfo);
        }

        public void ClearPendingLayers()
        {
            _pendingLayers.Clear();
        }

        public List<LayerCreationInfo> GetPendingLayers() => _pendingLayers.ToList();

        /// <summary>
        /// Adds all pending layers to the map in optimal stacking order (polygons → lines → points)
        /// </summary>
        public async Task AddAllLayersToMapAsync(MapStyleDefinition selectedMapStyle, CartographicProfile selectedProfile, IProgress<string> progress = null)
        {
            if (!_pendingLayers.Any())
            {
                LastAddedLayerCount = 0;
                progress?.Report("No layers to add to map.");
                return;
            }

            var effectiveStyle = GetEffectiveSelectedStyle(selectedProfile, selectedMapStyle);
            var sortedLayers = SortLayersByGeometryPriority(_pendingLayers, effectiveStyle);

            progress?.Report($"Preparing to add {sortedLayers.Count} layers with optimal stacking order...");
            string styleName = (selectedProfile != null || selectedMapStyle != null)
                ? CartographicProfileRules.GetDisplayName(selectedProfile, selectedMapStyle)
                : "default";
            LastAddedLayerCount = sortedLayers.Count;
            LastAppliedStyleName = styleName;
            System.Diagnostics.Debug.WriteLine($"Adding {sortedLayers.Count} layers using style-aware stacking order ({styleName}):");
            System.Diagnostics.Debug.WriteLine(
                $"Draw order rank source: style map count={effectiveStyle?.DrawOrderRanks?.Count ?? 0}, fallback map count={DefaultDrawOrderRanks.Count}");

            if (VerbosePerLayerDebugLogging)
            {
                foreach (var layer in sortedLayers)
                {
                    int drawRank = GetLayerDrawingRank(layer, effectiveStyle);
                    string resolvedType = ResolveLayerType(layer);
                    System.Diagnostics.Debug.WriteLine(
                        $"  {layer.LayerName} ({layer.GeometryType}, type={resolvedType ?? "unknown"}, draw rank {drawRank}, base priority {layer.StackingPriority})");
                }
            }

            int pointCount = sortedLayers.Count(l => l.StackingPriority == 3);
            int lineCount = sortedLayers.Count(l => l.StackingPriority == 2);
            int polygonCount = sortedLayers.Count(l => l.StackingPriority == 1);
            progress?.Report($"Layer summary: {pointCount} point layers, {lineCount} line layers, {polygonCount} polygon layers");

            if (sortedLayers.Count == 1)
            {
                progress?.Report("Single layer detected, using individual layer creation...");
                System.Diagnostics.Debug.WriteLine("Using individual layer creation for single layer (bulk creation may not work reliably with 1 layer)");
                await FallbackToIndividualLayerCreation(sortedLayers, selectedMapStyle, selectedProfile, progress);
            }
            else
            {
                List<LayerCreationInfo> missingLayersForIndividualCreation = null;

                try
                {
                    progress?.Report("Starting bulk layer creation process...");

                    await QueuedTask.Run(() =>
                    {
                        var map = MapView.Active?.Map;
                        if (map == null)
                        {
                            progress?.Report("Error: No active map found to add layers.");
                            System.Diagnostics.Debug.WriteLine("AddAllLayersToMapAsync: No active map found.");
                            return;
                        }

                        missingLayersForIndividualCreation = AddLayerBatch(map, sortedLayers, selectedMapStyle, selectedProfile, progress);
                    });

                    if (missingLayersForIndividualCreation?.Any() == true)
                    {
                        await FallbackToIndividualLayerCreation(missingLayersForIndividualCreation, selectedMapStyle, selectedProfile, progress);
                    }
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error during bulk layer creation: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Error in AddAllLayersToMapAsync: {ex.Message}");

                    progress?.Report("Falling back to individual layer creation...");
                    await FallbackToIndividualLayerCreation(sortedLayers, selectedMapStyle, selectedProfile, progress);
                }
            }

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map != null)
                {
                    EnforceLayerDrawOrder(map, sortedLayers, progress);
                }
            });

            _pendingLayers.Clear();
        }

        private static MapStyleDefinition GetEffectiveSelectedStyle(CartographicProfile profile, MapStyleDefinition style)
        {
            return (profile != null || style != null)
                ? CartographicProfileRules.ResolveStyle(profile, style)
                : null;
        }

        private static List<LayerCreationInfo> SortLayersByGeometryPriority(List<LayerCreationInfo> layers, MapStyleDefinition style)
        {
            return layers
                .OrderByDescending(layer => GetLayerDrawingRank(layer, style))
                .ThenBy(layer => layer.ParentTheme)
                .ThenBy(layer => layer.ActualType)
                .ThenBy(layer => layer.LayerName)
                .ToList();
        }

        private static int GetLayerDrawingRank(LayerCreationInfo layerInfo, MapStyleDefinition style)
        {
            if (layerInfo == null)
                return 0;

            string actualType = ResolveLayerType(layerInfo);
            string geometryGroup = GetGeometryGroup(layerInfo.GeometryType);
            var styleOrder = style?.DrawOrderRanks;
            string exactKey = string.IsNullOrWhiteSpace(actualType) ? null : $"{actualType}:{geometryGroup}";

            if (!string.IsNullOrWhiteSpace(exactKey))
            {
                if (styleOrder != null && styleOrder.TryGetValue(exactKey, out int exactStyleRank))
                    return exactStyleRank;

                if (DefaultDrawOrderRanks.TryGetValue(exactKey, out int exactFallbackRank))
                    return exactFallbackRank;
            }

            if (!string.IsNullOrWhiteSpace(actualType))
            {
                if (styleOrder != null && styleOrder.TryGetValue(actualType, out int typeStyleRank))
                    return typeStyleRank;

                if (DefaultDrawOrderRanks.TryGetValue(actualType, out int typeFallbackRank))
                    return typeFallbackRank;
            }

            return geometryGroup switch
            {
                "point" => 300,
                "line" => 200,
                _ => 100
            };
        }

        private static string ResolveLayerType(LayerCreationInfo layerInfo)
        {
            if (layerInfo == null)
                return null;

            if (!string.IsNullOrWhiteSpace(layerInfo.ActualType))
                return layerInfo.ActualType.Trim().ToLowerInvariant();

            if (!string.IsNullOrWhiteSpace(layerInfo.ParentTheme))
                return layerInfo.ParentTheme.Trim().ToLowerInvariant();

            string layerName = layerInfo.LayerName?.ToLowerInvariant() ?? string.Empty;
            if (layerName.Contains("building part")) return "building_part";
            if (layerName.Contains("building")) return "building";
            if (layerName.Contains("land use")) return "land_use";
            if (layerName.Contains("land cover")) return "land_cover";
            if (layerName.Contains("division boundary")) return "division_boundary";
            if (layerName.Contains("division area")) return "division_area";
            if (layerName.Contains("infrastructure")) return "infrastructure";
            if (layerName.Contains("bathymetry")) return "bathymetry";
            if (layerName.Contains("water")) return "water";
            if (layerName.Contains("land")) return "land";
            if (layerName.Contains("segment")) return "segment";
            if (layerName.Contains("connector")) return "connector";
            if (layerName.Contains("address")) return "address";
            if (layerName.Contains("place")) return "place";
            if (layerName.Contains("division")) return "division";

            return null;
        }

        private static string GetGeometryGroup(string geometryType)
        {
            if (string.IsNullOrWhiteSpace(geometryType))
                return "polygon";

            if (geometryType.Equals("POINT", StringComparison.OrdinalIgnoreCase) ||
                geometryType.Equals("MULTIPOINT", StringComparison.OrdinalIgnoreCase))
                return "point";

            if (geometryType.Equals("LINESTRING", StringComparison.OrdinalIgnoreCase) ||
                geometryType.Equals("MULTILINESTRING", StringComparison.OrdinalIgnoreCase))
                return "line";

            return "polygon";
        }

        private List<LayerCreationInfo> AddLayerBatch(Map map, List<LayerCreationInfo> sortedLayers, MapStyleDefinition selectedMapStyle, CartographicProfile selectedProfile, IProgress<string> progress)
        {
            progress?.Report("Validating layer files...");

            var uris = new List<Uri>();
            var layerNames = new List<string>();
            var validLayerInfos = new List<LayerCreationInfo>();
            int validFiles = 0;

            foreach (var layerInfo in sortedLayers)
            {
                if (File.Exists(layerInfo.FilePath))
                {
                    uris.Add(new Uri(layerInfo.FilePath));
                    layerNames.Add(layerInfo.LayerName);
                    validLayerInfos.Add(layerInfo);
                    validFiles++;
                    if (VerbosePerLayerDebugLogging)
                    {
                        System.Diagnostics.Debug.WriteLine($"Valid file for {layerInfo.LayerName}: {layerInfo.FilePath}");
                    }
                }
                else
                {
                    progress?.Report($"Warning: File not found for {layerInfo.LayerName}");
                    System.Diagnostics.Debug.WriteLine($"Warning: File not found for layer {layerInfo.LayerName}: {layerInfo.FilePath}");
                }
            }

            progress?.Report($"Validated {validFiles} layer files. Creating layers...");

            if (!uris.Any())
                return null;

            int removedExisting = RemoveExistingMembersForTargetParquetFiles(map, validLayerInfos);
            if (removedExisting > 0)
            {
                progress?.Report($"Removed {removedExisting} existing map member(s) that used the same output files.");
            }

            progress?.Report("Executing bulk layer creation (this may take a moment)...");
            var layers = LayerFactory.Instance.CreateLayers(uris, map);

            progress?.Report($"Bulk creation completed. Applying settings to {layers.Count} layers...");
            MapStyleDefinition effectiveStyle = GetEffectiveSelectedStyle(selectedProfile, selectedMapStyle);
            string effectiveCartographyName = CartographicProfileRules.GetDisplayName(selectedProfile, effectiveStyle);

            for (int i = 0; i < layers.Count && i < layerNames.Count; i++)
            {
                if (layers[i] != null)
                {
                    layers[i].SetName(layerNames[i]);

                    if (layers[i] is FeatureLayer featureLayer)
                    {
                        CIMRenderer renderer = null;
                        var layerInfo = i < validLayerInfos.Count ? validLayerInfos[i] : null;
                        if (effectiveStyle != null && layerInfo != null)
                        {
                            renderer = CartographyService.CreateRendererForLayer(layerInfo, effectiveStyle);
                        }
                        ApplyLayerSettings(featureLayer, renderer, layerInfo, effectiveStyle, selectedProfile);
                    }

                    if (i % 3 == 0 || i == layers.Count - 1)
                    {
                        progress?.Report($"Configured layer {i + 1} of {layers.Count}: {layerNames[i]}");
                    }
                }
            }

            progress?.Report($"✅ Successfully added {layers.Count} layers with optimal stacking order!");

            if (layers.Count < uris.Count)
            {
                progress?.Report($"Some layers missing from bulk creation ({layers.Count}/{uris.Count}). Creating missing layers individually...");
                var createdLayerNames = layers.Select(l => l.Name).ToHashSet();
                return sortedLayers.Where(layerInfo =>
                    File.Exists(layerInfo.FilePath) &&
                    !createdLayerNames.Contains(layerInfo.LayerName)).ToList();
            }

            return null;
        }

        private static void EnforceLayerDrawOrder(Map map, List<LayerCreationInfo> desiredOrder, IProgress<string> progress)
        {
            if (map == null || desiredOrder == null || desiredOrder.Count == 0)
                return;

            var desiredNames = desiredOrder
                .Select(l => l?.LayerName)
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .ToList();

            if (desiredNames.Count == 0)
                return;

            var liveLayerMap = map.Layers
                .Where(l => l != null)
                .GroupBy(l => l.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

            var presentLayers = desiredNames
                .Where(name => liveLayerMap.ContainsKey(name))
                .Select(name => liveLayerMap[name])
                .ToList();

            if (presentLayers.Count <= 1)
                return;

            int targetStartIndex = presentLayers
                .Select(l => map.Layers.IndexOf(l))
                .Where(i => i >= 0)
                .DefaultIfEmpty(0)
                .Min();

            for (int i = desiredNames.Count - 1; i >= 0; i--)
            {
                string layerName = desiredNames[i];
                if (!liveLayerMap.TryGetValue(layerName, out Layer layerToMove) || layerToMove == null)
                    continue;

                map.MoveLayer(layerToMove, targetStartIndex);
            }

            progress?.Report("Applied deterministic layer draw order");
        }

        private static int RemoveExistingMembersForTargetParquetFiles(Map map, IEnumerable<LayerCreationInfo> targetLayers)
        {
            if (map == null || targetLayers == null)
                return 0;

            var targetLayerList = targetLayers.Where(t => t != null).ToList();
            if (!targetLayerList.Any())
                return 0;

            var targetFilePaths = new HashSet<string>(
                targetLayerList
                    .Select(t => NormalizeToParquetFilePath(t.FilePath))
                    .Where(p => !string.IsNullOrWhiteSpace(p)),
                StringComparer.OrdinalIgnoreCase);

            var targetLayerNames = new HashSet<string>(
                targetLayerList
                    .Select(t => t.LayerName)
                    .Where(n => !string.IsNullOrWhiteSpace(n)),
                StringComparer.OrdinalIgnoreCase);

            var membersToRemove = new List<MapMember>();

            foreach (var layer in map.GetLayersAsFlattenedList().OfType<FeatureLayer>())
            {
                bool removeByPath = false;
                bool pathCheckFailed = false;

                try
                {
                    using var fc = layer.GetFeatureClass();
                    var pathUri = fc?.GetPath();
                    var normalizedLayerPath = NormalizeToParquetFilePath(pathUri != null
                        ? (pathUri.IsFile ? pathUri.LocalPath : pathUri.OriginalString)
                        : null);

                    if (!string.IsNullOrWhiteSpace(normalizedLayerPath) && targetFilePaths.Contains(normalizedLayerPath))
                        removeByPath = true;
                }
                catch (Exception ex)
                {
                    pathCheckFailed = true;
                    System.Diagnostics.Debug.WriteLine($"RemoveExistingMembersForTargetParquetFiles: Failed to inspect layer path for '{layer.Name}': {ex.Message}");
                }

                if (removeByPath || (pathCheckFailed && targetLayerNames.Contains(layer.Name)))
                {
                    membersToRemove.Add(layer);
                }
            }

            foreach (var table in map.GetStandaloneTablesAsFlattenedList().OfType<StandaloneTable>())
            {
                bool removeByPath = false;
                bool pathCheckFailed = false;

                try
                {
                    using var tbl = table.GetTable();
                    var pathUri = tbl?.GetPath();
                    var normalizedTablePath = NormalizeToParquetFilePath(pathUri != null
                        ? (pathUri.IsFile ? pathUri.LocalPath : pathUri.OriginalString)
                        : null);

                    if (!string.IsNullOrWhiteSpace(normalizedTablePath) && targetFilePaths.Contains(normalizedTablePath))
                        removeByPath = true;
                }
                catch (Exception ex)
                {
                    pathCheckFailed = true;
                    System.Diagnostics.Debug.WriteLine($"RemoveExistingMembersForTargetParquetFiles: Failed to inspect table path for '{table.Name}': {ex.Message}");
                }

                if (removeByPath || (pathCheckFailed && targetLayerNames.Contains(table.Name)))
                {
                    membersToRemove.Add(table);
                }
            }

            int removedCount = 0;
            foreach (var member in membersToRemove.Distinct())
            {
                try
                {
                    if (member is Layer layer)
                    {
                        map.RemoveLayer(layer);
                        (layer as IDisposable)?.Dispose();
                        removedCount++;
                    }
                    else if (member is StandaloneTable table)
                    {
                        map.RemoveStandaloneTable(table);
                        (table as IDisposable)?.Dispose();
                        removedCount++;
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"RemoveExistingMembersForTargetParquetFiles: Failed removing '{member.Name}': {ex.Message}");
                }
            }

            return removedCount;
        }

        private static string NormalizeToParquetFilePath(string rawPath)
        {
            if (string.IsNullOrWhiteSpace(rawPath))
                return null;

            try
            {
                string fullPath = Path.GetFullPath(rawPath);
                string lowerPath = fullPath.ToLowerInvariant();
                int parquetIndex = lowerPath.IndexOf(".parquet", StringComparison.OrdinalIgnoreCase);
                if (parquetIndex >= 0)
                {
                    return fullPath[..(parquetIndex + ".parquet".Length)].ToLowerInvariant();
                }

                return fullPath.ToLowerInvariant();
            }
            catch
            {
                return null;
            }
        }

        private async Task FallbackToIndividualLayerCreation(List<LayerCreationInfo> layers, MapStyleDefinition selectedMapStyle, CartographicProfile selectedProfile, IProgress<string> progress)
        {
            if (layers == null || layers.Count == 0)
                return;

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map == null)
                {
                    progress?.Report("Error: No active map found to add layer(s).");
                    return;
                }

                MapStyleDefinition effectiveStyle = GetEffectiveSelectedStyle(selectedProfile, selectedMapStyle);

                foreach (var layerInfo in layers)
                {
                    if (!File.Exists(layerInfo.FilePath))
                    {
                        progress?.Report($"Error: Parquet file not found at {layerInfo.FilePath}");
                        continue;
                    }

                    try
                    {
                        Uri dataUri = new(layerInfo.FilePath);
                        Layer newLayer = LayerFactory.Instance.CreateLayer(dataUri, map, layerName: layerInfo.LayerName);
                        if (newLayer == null)
                        {
                            progress?.Report($"Error: Could not create layer for {layerInfo.LayerName}.");
                            continue;
                        }

                        if (newLayer is FeatureLayer featureLayer)
                        {
                            CIMRenderer renderer = null;
                            if (effectiveStyle != null)
                            {
                                renderer = CartographyService.CreateRendererForLayer(layerInfo, effectiveStyle);
                            }
                            ApplyLayerSettings(featureLayer, renderer, layerInfo, effectiveStyle, selectedProfile);
                        }

                        progress?.Report($"Successfully added layer: {newLayer?.Name ?? layerInfo.LayerName}");
                    }
                    catch (Exception ex)
                    {
                        progress?.Report($"Error adding layer {layerInfo.LayerName}: {ex.Message}");
                    }
                }
            });
        }

        private static void ApplyLayerSettings(FeatureLayer featureLayer, CIMRenderer renderer = null, LayerCreationInfo layerInfo = null, MapStyleDefinition mapStyle = null, CartographicProfile profile = null)
        {
            try
            {
                if (featureLayer.GetDefinition() is CIMFeatureLayer layerDef)
                {
                    layerDef.DisplayCacheType = ArcGIS.Core.CIM.DisplayCacheType.None;
                    layerDef.FeatureCacheType = ArcGIS.Core.CIM.FeatureCacheType.None;

                    if (layerDef.FeatureReduction is CIMBinningFeatureReduction binningReduction && binningReduction.Enabled)
                    {
                        bool isPointLayer =
                            layerInfo != null &&
                            (string.Equals(layerInfo.GeometryType, "POINT", StringComparison.OrdinalIgnoreCase) ||
                             string.Equals(layerInfo.GeometryType, "MULTIPOINT", StringComparison.OrdinalIgnoreCase));

                        if (!isPointLayer)
                        {
                            binningReduction.Enabled = false;
                        }
                    }

                    if (renderer != null)
                    {
                        layerDef.Renderer = renderer;
                    }

                    bool labelsAdded = false;
                    if (layerInfo != null)
                    {
                        labelsAdded = CartographyService.ApplyLabelClasses(layerDef, layerInfo.ActualType, layerInfo.GeometryType, mapStyle, profile);
                    }

                    featureLayer.SetDefinition(layerDef);
                    if (renderer != null)
                    {
                        try
                        {
                            featureLayer.SetRenderer(renderer);
                        }
                        catch (Exception rendererEx)
                        {
                            System.Diagnostics.Debug.WriteLine($"Warning: SetRenderer failed for {featureLayer.Name}: {rendererEx.Message}");
                        }
                    }
                    if (labelsAdded)
                    {
                        featureLayer.SetLabelVisibility(true);
                        featureLayer.SetLabelVisibility(false);
                        featureLayer.SetLabelVisibility(true);
                    }
                    else
                    {
                        featureLayer.SetLabelVisibility(false);
                    }

                    if (layerInfo != null)
                    {
                        CartographyService.ApplyVisibilityDefaults(featureLayer, layerInfo.ActualType, layerInfo.GeometryType, profile);
                    }
                }
            }
            catch (ArgumentException ex) when (ex.Message.Contains("domain") || ex.Message.Contains("Domain") || ex.Message.Contains("not supported"))
            {
                System.Diagnostics.Debug.WriteLine($"Info: Skipping layer settings for {featureLayer.Name} (domain access not supported for Parquet in Pro 3.5): {ex.Message}");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Warning: Failed to apply settings to layer {featureLayer.Name}: {ex.Message}");
            }
        }

        public async Task RemoveLayersUsingFileAsync(string filePath)
        {
            if (string.IsNullOrEmpty(filePath)) return;

            await QueuedTask.Run(() =>
            {
                var project = Project.Current;
                if (project == null) return;

                var allMaps = project.GetItems<MapProjectItem>().Select(item => item.GetMap()).Where(map => map != null).ToList();
                var allMembersToRemove = new Dictionary<Map, List<MapMember>>();
                string normalizedTargetPath = Path.GetFullPath(filePath).ToLowerInvariant();

                static string GetActualFilePath(string dataSourcePath)
                {
                    if (string.IsNullOrEmpty(dataSourcePath)) return null;
                    string lowerPath = dataSourcePath.ToLowerInvariant();
                    if (lowerPath.Contains(".parquet\\") || lowerPath.Contains(".parquet/"))
                    {
                        int parquetIndex = lowerPath.IndexOf(".parquet");
                        if (parquetIndex > -1)
                        {
                            return Path.GetFullPath(dataSourcePath[..(parquetIndex + ".parquet".Length)]).ToLowerInvariant();
                        }
                    }
                    return Path.GetFullPath(dataSourcePath).ToLowerInvariant();
                }

                foreach (var map in allMaps)
                {
                    var membersToRemove = new List<MapMember>();
                    var allLayers = map.GetLayersAsFlattenedList().ToList();
                    var allTables = map.GetStandaloneTablesAsFlattenedList().ToList();

                    foreach (var layer in allLayers.OfType<FeatureLayer>())
                    {
                        try
                        {
                            using var fc = layer.GetFeatureClass();
                            var fcPathUri = fc?.GetPath();
                            if (fcPathUri != null && fcPathUri.IsFile)
                            {
                                if (GetActualFilePath(fcPathUri.LocalPath) == normalizedTargetPath)
                                    membersToRemove.Add(layer);
                            }
                        }
                        catch { }
                    }

                    foreach (var table in allTables.OfType<StandaloneTable>())
                    {
                        try
                        {
                            using var tbl = table.GetTable();
                            var tblPathUri = tbl?.GetPath();
                            if (tblPathUri != null && tblPathUri.IsFile)
                            {
                                if (GetActualFilePath(tblPathUri.LocalPath) == normalizedTargetPath)
                                    membersToRemove.Add(table);
                            }
                        }
                        catch { }
                    }

                    if (membersToRemove.Count > 0)
                        allMembersToRemove[map] = membersToRemove;
                }

                foreach (var kvp in allMembersToRemove)
                {
                    var map = kvp.Key;
                    foreach (var member in kvp.Value.Distinct())
                    {
                        if (member is Layer layerToRemove)
                        {
                            map.RemoveLayer(layerToRemove);
                            (layerToRemove as IDisposable)?.Dispose();
                        }
                        else if (member is StandaloneTable tableToRemove)
                        {
                            map.RemoveStandaloneTable(tableToRemove);
                            (tableToRemove as IDisposable)?.Dispose();
                        }
                    }
                }
            });
        }
    }
}
