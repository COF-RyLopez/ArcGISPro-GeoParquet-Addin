using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ArcGIS.Core.CIM;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;

namespace DuckDBGeoparquet.Services
{
    public class DataProcessor : IDisposable
    {
        private readonly IDataService _dataService;

        // Option to enable/disable geometry repair during ingestion
        public bool EnableGeometryRepair
        {
            get => _dataService.EnableGeometryRepair;
            set => _dataService.EnableGeometryRepair = value;
        }

        public DataProcessor()
        {
            // Inject dependencies manually for now (since we don't have DI container setup)
            // In unit tests, we can use a constructor overload to inject mocks
            _dataService = new DuckDbDataService(new ArcGisFileHandler());
        }
        
        // Constructor for testing
        public DataProcessor(IDataService dataService)
        {
            _dataService = dataService;
        }

        public async Task InitializeDuckDBAsync()
        {
            await _dataService.InitializeAsync();
        }

        public async Task<bool> IngestFileAsync(string s3Path, dynamic extent = null, string actualS3Type = null, IProgress<string> progress = null)
        {
            ExtentInfo extentInfo = null;
            
            if (extent != null && extent is Envelope)
            {
                var env = (Envelope)extent;
                extentInfo = new ExtentInfo
                {
                    XMin = env.XMin,
                    YMin = env.YMin,
                    XMax = env.XMax,
                    YMax = env.YMax
                };
            }

            return await _dataService.IngestFileAsync(s3Path, extentInfo, actualS3Type, progress);
        }

        public async Task<DataTable> GetPreviewDataAsync()
        {
            return await _dataService.GetPreviewDataAsync();
        }

        public async Task CreateFeatureLayerAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot)
        {
            await _dataService.CreateFeatureLayerAsync(layerNameBase, progress, parentS3Theme, actualS3Type, dataOutputPathRoot);
        }

        public void ClearPendingLayers()
        {
            _dataService.ClearPendingLayers();
        }

        /// <summary>
        /// Adds all pending layers to the map in optimal stacking order (polygons → lines → points)
        /// </summary>
        public async Task AddAllLayersToMapAsync(IProgress<string> progress = null)
        {
            var pendingLayers = _dataService.GetPendingLayers();
            
            if (!pendingLayers.Any())
            {
                progress?.Report("No layers to add to map.");
                return;
            }

            // Sort all layers by stacking priority (higher priority = higher in Contents panel = drawn on top)
            // We want: Points on top (drawn last), Lines in middle, Polygons on bottom (drawn first)
            var sortedLayers = pendingLayers
                .OrderByDescending(layer => layer.StackingPriority) // Higher priority first (Points > Lines > Polygons)
                .ThenBy(layer => layer.ParentTheme) // Secondary sort by theme for consistency
                .ThenBy(layer => layer.ActualType)
                .ThenBy(layer => layer.GeometryType == "POLYGON" || layer.GeometryType == "MULTIPOLYGON" ? layer.Area : 0) // For polygons, sort by area (smaller area first for drawing last/on top)
                .ToList();

            progress?.Report($"Preparing to add {sortedLayers.Count} layers with optimal stacking order...");

            // Use individual layer creation for single layers, bulk creation for multiple layers
            if (sortedLayers.Count == 1)
            {
                progress?.Report("Single layer detected, using individual layer creation...");
                await FallbackToIndividualLayerCreation(sortedLayers, progress);
            }
            else
            {
                List<LayerCreationInfo> missingLayersForIndividualCreation = null;
                
                try
                {
                    progress?.Report("Starting bulk layer creation process...");

                    await QueuedTask.Run(async () =>
                    {
                        var map = MapView.Active?.Map;
                        if (map == null)
                        {
                            progress?.Report("Error: No active map found to add layers.");
                            return;
                        }

                        // Use BulkMapMemberCreationParams for optimal performance
                        var uris = new List<Uri>();
                        var layerNames = new List<string>();

                        foreach (var layerInfo in sortedLayers)
                        {
                            if (File.Exists(layerInfo.FilePath))
                            {
                                uris.Add(new Uri(layerInfo.FilePath));
                                layerNames.Add(layerInfo.LayerName);
                            }
                            else
                            {
                                progress?.Report($"Warning: File not found for {layerInfo.LayerName}");
                            }
                        }

                        if (uris.Any())
                        {
                            progress?.Report("Executing bulk layer creation (this may take a moment)...");

                            // Create layers using LayerFactory
                            var layers = LayerFactory.Instance.CreateLayers(uris, map);

                            progress?.Report($"Bulk creation completed. Applying settings to {layers.Count} layers...");

                            // Apply custom names and settings to the created layers
                            for (int i = 0; i < layers.Count && i < layerNames.Count; i++)
                            {
                                if (layers[i] != null)
                                {
                                    layers[i].SetName(layerNames[i]);

                                    // Apply cache and feature reduction settings
                                    if (layers[i] is FeatureLayer featureLayer)
                                    {
                                        ApplyLayerSettings(featureLayer);

                                        // Determine style file based on layer type and theme
                                        var layerInfo = sortedLayers[i]; // Get the original layer info
                                        string descriptiveGeomType = GetDescriptiveGeometryType(layerInfo.GeometryType);
                                        string styleFileName = $"{layerInfo.ActualType}-{descriptiveGeomType}.lyrx";
                                        await ApplyLayerSymbologyAsync(featureLayer, styleFileName, progress);
                                    }
                                    
                                    if (i % 3 == 0 || i == layers.Count - 1)
                                    {
                                        progress?.Report($"Configured layer {i + 1} of {layers.Count}: {layerNames[i]}");
                                    }
                                }
                            }

                            progress?.Report($"✅ Successfully added {layers.Count} layers with optimal stacking order!");
                            
                            // If bulk creation didn't create all expected layers, fall back to individual creation for missing ones
                            if (layers.Count < uris.Count)
                            {
                                progress?.Report($"Some layers missing from bulk creation ({layers.Count}/{uris.Count}). Creating missing layers individually...");
                                
                                var createdLayerNames = layers.Select(l => l.Name).ToHashSet();
                                missingLayersForIndividualCreation = sortedLayers.Where(layerInfo => 
                                    File.Exists(layerInfo.FilePath) && 
                                    !createdLayerNames.Contains(layerInfo.LayerName)).ToList();
                            }
                        }
                    });
                    
                    // Create missing layers individually outside QueuedTask
                    if (missingLayersForIndividualCreation?.Any() == true)
                    {
                        await FallbackToIndividualLayerCreation(missingLayersForIndividualCreation, progress);
                    }
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error during bulk layer creation: {ex.Message}");
                    // Fallback to individual layer creation if bulk creation fails
                    progress?.Report("Falling back to individual layer creation...");
                    await FallbackToIndividualLayerCreation(sortedLayers, progress);
                }
            }
            
            // Clear pending layers after processing
            _dataService.ClearPendingLayers();
        }

        private async Task FallbackToIndividualLayerCreation(List<LayerCreationInfo> layers, IProgress<string> progress)
        {
            foreach (var layerInfo in layers)
            {
                try
                {
                    await AddLayerToMapAsync(layerInfo.FilePath, layerInfo.LayerName, progress);
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error adding layer {layerInfo.LayerName}: {ex.Message}");
                }
            }
        }

        private static void ApplyLayerSettings(FeatureLayer featureLayer)
        {
            try
            {
                if (featureLayer.GetDefinition() is CIMFeatureLayer layerDef)
                {
                    layerDef.DisplayCacheType = DisplayCacheType.None;
                    layerDef.FeatureCacheType = FeatureCacheType.None;

                    if (layerDef.FeatureReduction is CIMBinningFeatureReduction binningReduction && binningReduction.Enabled)
                    {
                        binningReduction.Enabled = false;
                    }

                    featureLayer.SetDefinition(layerDef);
                }
            }
            catch (Exception) { /* Log warning */ }
        }

        private static async Task ApplyLayerSymbologyAsync(FeatureLayer featureLayer, string styleFileName, IProgress<string> progress = null)
        {
            if (featureLayer == null || string.IsNullOrEmpty(styleFileName)) return;

            string addInFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            string styleFilePath = Path.Combine(addInFolder, "Styles", styleFileName);

            if (!File.Exists(styleFilePath))
            {
                progress?.Report($"Warning: Style file not found at {styleFilePath}. Cannot apply symbology.");
                return;
            }

            progress?.Report($"Applying symbology from {styleFileName}...");

            await QueuedTask.Run(async () =>
            {
                try
                {
                    var parameters = Geoprocessing.MakeValueArray(featureLayer, styleFilePath);
                    await Geoprocessing.ExecuteToolAsync("management.ApplySymbologyFromLayer", parameters, null, null, GPExecuteToolFlags.Default);
                }
                catch (Exception ex)
                {
                    progress?.Report($"Unexpected error applying symbology: {ex.Message}");
                }
            });
        }

        private static async Task AddLayerToMapAsync(string parquetFilePath, string layerName, IProgress<string> progress = null)
        {
            progress?.Report($"Adding layer {layerName} to map...");

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map == null) return;

                if (!File.Exists(parquetFilePath))
                {
                    progress?.Report($"Error: Parquet file not found at {parquetFilePath}");
                    return;
                }

                try
                {
                    Uri dataUri = new(parquetFilePath);
                    Layer newLayer = LayerFactory.Instance.CreateLayer(dataUri, map, layerName: layerName);

                    if (newLayer is FeatureLayer featureLayer)
                    {
                        ApplyLayerSettings(featureLayer);
                    }

                    progress?.Report($"Successfully added layer: {newLayer.Name}");
                }
                catch (Exception ex)
                {
                    progress?.Report($"Error adding layer {layerName}: {ex.Message}");
                }
            });
        }
        
        private static string GetDescriptiveGeometryType(string geomType) => geomType switch
        {
            "POINT" => "points",
            "LINESTRING" => "lines",
            "POLYGON" => "polygons",
            "MULTIPOINT" => "multipoints",
            "MULTILINESTRING" => "multilines",
            "MULTIPOLYGON" => "multipolygons",
            _ => geomType.ToLowerInvariant()
        };

        public void Dispose()
        {
            _dataService?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
