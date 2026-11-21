using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        public void RelocatePendingLayers(string oldRoot, string newRoot)
        {
            foreach (var layer in _pendingLayers)
                layer.FilePath = Path.Combine(newRoot, Path.GetRelativePath(oldRoot, layer.FilePath));
        }

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

        /// <summary>Add a snapshot as a group, then retire matching prior layers only after full success.</summary>
        public async Task AddAllLayersToMapAsync(MapStyleDefinition selectedMapStyle, CartographicProfile selectedProfile,
            IProgress<string> progress = null, Map targetMap = null, string snapshotPath = null)
        {
            var map = targetMap ?? MapView.Active?.Map;
            if (map == null) throw new InvalidOperationException("The target map is unavailable.");
            LastAddedLayerCount = 0;
            var style = GetEffectiveSelectedStyle(selectedProfile, selectedMapStyle);
            LastAppliedStyleName = (selectedProfile != null || selectedMapStyle != null)
                ? CartographicProfileRules.GetDisplayName(selectedProfile, selectedMapStyle) : "default";
            var sorted = SortLayersByGeometryPriority(_pendingLayers, style);
            var replacedFiles = snapshotPath == null ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                : await Task.Run(() => SnapshotLayerPlan.GetSupersededFiles(snapshotPath));
            var stopwatch = Stopwatch.StartNew();
            try
            {
                await QueuedTask.Run(() =>
                {
                    var previousLayers = map.GetLayersAsFlattenedList().OfType<FeatureLayer>().ToList();
                    GroupLayer group = null;
                    if (sorted.Count > 0)
                    {
                        string name = snapshotPath == null ? "Overture data" : "Overture " + Path.GetFileName(snapshotPath);
                        group = LayerFactory.Instance.CreateGroupLayer(map, 0, name);
                        group.SetVisibility(false);
                    }
                    foreach (var info in sorted)
                    {
                        try
                        {
                            if (!File.Exists(info.FilePath)) throw new FileNotFoundException("Saved layer is missing.", info.FilePath);
                            var layer = LayerFactory.Instance.CreateLayer(new Uri(info.FilePath), group, -1, info.LayerName);
                            if (layer == null) throw new InvalidOperationException("ArcGIS could not open this GeoParquet file.");
                            LastAddedLayerCount++;
                            if (layer is FeatureLayer featureLayer)
                                ApplyLayerSettings(featureLayer, style == null ? null : CartographyService.CreateRendererForLayer(info, style), info, style, selectedProfile);
                            progress?.Report($"Prepared layer {LastAddedLayerCount} of {sorted.Count}: {info.LayerName}");
                        }
                        catch (Exception ex) { progress?.Report($"Could not add {info.LayerName}: {ex.Message}"); }
                    }
                    if (LastAddedLayerCount == sorted.Count)
                    {
                        foreach (var layer in previousLayers)
                        {
                            try
                            {
                                using var featureClass = layer.GetFeatureClass();
                                var uri = featureClass?.GetPath();
                                var file = NormalizeToParquetFilePath(uri == null ? null : uri.IsFile ? uri.LocalPath : uri.OriginalString);
                                if (file != null && replacedFiles.Contains(file)) layer.SetVisibility(false);
                            }
                            catch (Exception ex) { progress?.Report($"Could not inspect prior layer {layer.Name}: {ex.Message}"); }
                        }
                        group?.SetVisibility(true);
                    }
                    else
                    {
                        group?.SetName(group.Name + " (incomplete)");
                        progress?.Report("The incomplete group is hidden. Previous map layers remain visible; use the saved snapshot to recover missing layers.");
                    }
                });
            }
            finally
            {
                _pendingLayers.Clear();
                Debug.WriteLine($"[perf][layer-add] requested={sorted.Count} added={LastAddedLayerCount} elapsed={FormatElapsed(stopwatch.Elapsed)}");
            }
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

        private static void ApplyLayerSettings(FeatureLayer featureLayer, CIMRenderer renderer = null, LayerCreationInfo layerInfo = null, MapStyleDefinition mapStyle = null, CartographicProfile profile = null)
        {
            try
            {
                if (featureLayer.GetDefinition() is CIMFeatureLayer layerDef)
                {
                    // Immutable snapshots can retain the host's display and feature cache defaults.

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

        private static string FormatElapsed(TimeSpan elapsed)
        {
            return elapsed.TotalSeconds >= 1
                ? $"{elapsed.TotalSeconds:F2}s"
                : $"{elapsed.TotalMilliseconds:F0}ms";
        }
    }
}
