using ArcGIS.Core.CIM;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using ArcGIS.Core.Geometry;
using System;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Linq;
using System.Data;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Microsoft.Win32;
using ArcGIS.Desktop.Catalog;
using System.Net.Http;
using System.Text.Json;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Threading;
using ArcGIS.Desktop.Core;
using System.Net;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;


namespace DuckDBGeoparquet.Views
{
    internal partial class WizardDockpaneViewModel : DockPane
    {
        #region Helper Methods
        private void AddToLog(string message)
        {
            // Append the new log entry to the end of the log
            LogOutput.AppendLine($"[{DateTime.Now:HH:mm:ss}] {message}");

            // Update the text property
            LogOutputText = LogOutput.ToString();
            NotifyPropertyChanged(nameof(LogOutputText));
        }

        private void UpdateThemePreview()
        {
            string description = "Select a theme or sub-theme to see details.";
            string icon = "GlobeIcon"; // Default

            var itemForPreview = SelectedItemForPreview; // The item currently focused in TreeView

            if (itemForPreview != null)
            {
                // Get description and icon from the parent theme typically
                string parentS3Key = itemForPreview.IsSelectable && itemForPreview.SubItems.Count == 0 && Themes.Any(t => t.ActualType == itemForPreview.ParentThemeForS3 && t.SubItems.Count == 0) ?
                                     itemForPreview.ActualType : // If it's a leaf parent (like "places")
                                     itemForPreview.ParentThemeForS3; // Otherwise, use the parent key

                description = ThemeDescriptions.TryGetValue(parentS3Key, out var desc)
                    ? desc
                    : "No description available.";

                if (itemForPreview.IsSelectable && itemForPreview.ParentThemeForS3 != itemForPreview.ActualType) // It's a sub-item
                {
                    description += "\nSub-theme: " + itemForPreview.DisplayName;
                }

                icon = ThemeIcons.TryGetValue(parentS3Key, out var iconName) ? iconName : "GlobeIcon";
            }

            // Calculate combined estimates for ALL selected leaf themes
            var allSelectedLeaves = GetSelectedLeafItems();
            if (allSelectedLeaves.Count > 0)
            {
                int totalEstimatedFeatures = 0;
                double totalSizeInKb = 0;

                foreach (var selectedLeaf in allSelectedLeaves)
                {
                    // Use the ActualType of the leaf item to get its specific estimate
                    if (ThemeFeatureEstimates.TryGetValue(selectedLeaf.ActualType, out int itemEstimate))
                    {
                        totalEstimatedFeatures += itemEstimate;
                        totalSizeInKb += itemEstimate * 2.5; // Assuming 2.5KB per feature
                    }
                    else
                    {
                        // Optional: Log if an estimate is missing for an actual type
                        System.Diagnostics.Debug.WriteLine($"Warning: No feature estimate found for ActualType: {selectedLeaf.ActualType}");
                    }
                }
                EstimatedFeatures = $"{totalEstimatedFeatures} total per sq km (approx.)";
                EstimatedSize = totalSizeInKb > 1024
                    ? $"{totalSizeInKb / 1024:F1} MB total per sq km (approx.)"
                    : $"{totalSizeInKb:F0} KB total per sq km (approx.)";

                if (allSelectedLeaves.Count == 1 && itemForPreview != null && itemForPreview == allSelectedLeaves.First()) // If only one item is selected, and it's the one being previewed
                {
                    // Use the ActualType of the itemForPreview to get its specific estimate
                    if (ThemeFeatureEstimates.TryGetValue(itemForPreview.ActualType, out int itemFeatures))
                    {
                        double itemSizeKb = itemFeatures * 2.5;
                        EstimatedFeatures = $"{itemFeatures} per sq km (approx. for {itemForPreview.DisplayName})";
                        EstimatedSize = itemSizeKb > 1024
                            ? $"{itemSizeKb / 1024:F1} MB per sq km (approx. for {itemForPreview.DisplayName})"
                            : $"{itemSizeKb:F0} KB per sq km (approx. for {itemForPreview.DisplayName})";
                    }
                    else
                    {
                        // Fallback if specific estimate is missing for the single selected item
                        EstimatedFeatures = $"-- per sq km (approx. for {itemForPreview.DisplayName})";
                        EstimatedSize = $"-- MB/KB per sq km (approx. for {itemForPreview.DisplayName})";
                        System.Diagnostics.Debug.WriteLine($"Warning: No feature estimate for single selected ActualType: {itemForPreview.ActualType}");
                    }
                }
            }
            else // No items are selected
            {
                // If nothing is selected, but an item is focused for preview, show its individual estimate
                if (itemForPreview != null && itemForPreview.IsSelectable) // Check if the preview item is a selectable leaf
                {
                    // Use the ActualType of the itemForPreview to get its specific estimate
                    if (ThemeFeatureEstimates.TryGetValue(itemForPreview.ActualType, out int itemFeat))
                    {
                        double itemSzKb = itemFeat * 2.5;
                        EstimatedFeatures = $"{itemFeat} per sq km (approx. for {itemForPreview.DisplayName})";
                        EstimatedSize = itemSzKb > 1024
                            ? $"{itemSzKb / 1024:F1} MB per sq km (approx. for {itemForPreview.DisplayName})"
                            : $"{itemSzKb:F0} KB per sq km (approx. for {itemForPreview.DisplayName})";
                    }
                    else
                    {
                        // Fallback if specific estimate is missing for the focused item
                        EstimatedFeatures = $"-- per sq km (approx. for {itemForPreview.DisplayName})";
                        EstimatedSize = $"-- MB/KB per sq km (approx. for {itemForPreview.DisplayName})";
                        System.Diagnostics.Debug.WriteLine($"Warning: No feature estimate for focused ActualType: {itemForPreview.ActualType}");
                    }
                }
                else // Nothing selected and no specific leaf item focused for preview
                {
                    EstimatedFeatures = "--";
                    EstimatedSize = "--";
                }
            }

            ThemeDescription = description;
            ThemeIconText = icon;
            // EstimatedFeatures and EstimatedSize are set above
            NotifyPropertyChanged(nameof(ThemeDescription));
            NotifyPropertyChanged(nameof(EstimatedFeatures));
            NotifyPropertyChanged(nameof(EstimatedSize));
            NotifyPropertyChanged(nameof(ThemeIconText));
            NotifyPropertyChanged(nameof(SelectedLeafItemCount));
            NotifyPropertyChanged(nameof(AllSelectedLeafItemsForPreview));
            UpdateIsSelectAllCheckedStatus(); // Ensure "Select All" checkbox reflects current state
        }

        private void ShowThemeInfo()
        {
            if (SelectedItemForPreview == null) return;

            var item = SelectedItemForPreview;
            string parentS3Key = item.ParentThemeForS3;

            string description = ThemeDescriptions.TryGetValue(parentS3Key, out string themeDesc)
                ? themeDesc
                : "No detailed information available.";

            string typesInfo = $"S3 Theme: {parentS3Key}";
            if (item.IsSelectable && item.ParentThemeForS3 != item.ActualType) // It's a sub-item
            {
                description = $"Parent: {MakeFriendlyName(parentS3Key)}\nSub-theme: {item.DisplayName}\n\n{description}";
                typesInfo += $", S3 Type: {item.ActualType}";
            }
            else // It's a parent item (either leaf or just for preview)
            {
                typesInfo += $", S3 Type(s): {_overtureS3ThemeTypes[parentS3Key]}";
            }

            var selectedLeafItems = GetSelectedLeafItems();
            string selectedCount = selectedLeafItems.Count > 0 ?
                $"\n\nYou have selected {selectedLeafItems.Count} specific data type(s) in total."
                : "";

            ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                $"{description}\n\n{typesInfo}{selectedCount}",
                $"About '{item.DisplayName}'",
                System.Windows.MessageBoxButton.OK,
                System.Windows.MessageBoxImage.Information);
        }

        private void SetCustomExtent()
        {
            try
            {
                // Add diagnostic logging
                System.Diagnostics.Debug.WriteLine("SetCustomExtent method called");
                AddToLog("SetCustomExtent method called - attempting to activate drawing tool");

                // Ensure the custom extent radio button is selected
                UseCustomExtent = true;

                // Make sure we're subscribed to the static event
                // We already subscribed in the constructor, but ensure it's still active
                try
                {
                    // Remove any existing subscription and add it again to be safe
                    // This prevents multiple handlers if called multiple times
                    CustomExtentTool.ExtentCreatedStatic -= OnExtentCreated;
                    CustomExtentTool.ExtentCreatedStatic += OnExtentCreated;
                    System.Diagnostics.Debug.WriteLine("Re-established event subscription for CustomExtentTool");
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error managing event subscriptions: {ex.Message}");
                }

                // Create the instance tool as well for backward compatibility
                if (_customExtentTool == null)
                {
                    _customExtentTool = new CustomExtentTool();
                    _customExtentTool.ExtentCreated += OnExtentCreated;
                }

                // Use ArcGIS Pro's drawing tool to select an extent
                QueuedTask.Run(async () =>
                {
                    System.Diagnostics.Debug.WriteLine("Inside QueuedTask.Run");
                    AddToLog("Starting custom extent drawing operation...");

                    // Get a reference to the active map and make sure one exists
                    var mapView = MapView.Active;
                    if (mapView == null)
                    {
                        AddToLog("Unable to set custom extent: No active map view");
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                            "Please open a map before setting a custom extent.",
                            "No Active Map");
                        return;
                    }

                    AddToLog($"Active map view found: {mapView.Map.Name}");

                    try
                    {
                        // Activate our custom tool
                        AddToLog("Activating custom drawing tool...");
                        System.Diagnostics.Debug.WriteLine("Activating custom extent tool");

                        // Use our custom tool by ID as defined in the Config.daml
                        await FrameworkApplication.SetCurrentToolAsync("DuckDBGeoparquet_CustomExtentTool");
                        AddToLog("Draw a rectangle on the map to set the custom extent...");
                        System.Diagnostics.Debug.WriteLine("Custom tool activated successfully");
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"Error in tool activation: {ex.Message}");
                        System.Diagnostics.Debug.WriteLine($"Exception in tool activation: {ex}");
                    }
                });
            }
            catch (Exception ex)
            {
                AddToLog($"Error setting custom extent: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Exception in SetCustomExtent: {ex}");
            }
        }

        // Handler for when our custom tool creates an extent
        private void OnExtentCreated(Envelope extent)
        {
            // Add more detailed logging
            System.Diagnostics.Debug.WriteLine($"Custom extent created: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");

            Envelope extentInWGS84 = extent;
            if (extent.SpatialReference == null || extent.SpatialReference.Wkid != 4326)
            {
                AddToLog("Custom extent is not in WGS84. Projecting...");
                System.Diagnostics.Debug.WriteLine($"Original SR WKID: {extent.SpatialReference?.Wkid}");
                SpatialReference wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                try
                {
                    extentInWGS84 = GeometryEngine.Instance.Project(extent, wgs84) as Envelope;
                    if (extentInWGS84 != null)
                    {
                        AddToLog($"Successfully projected custom extent to WGS84: {extentInWGS84.XMin:F6}, {extentInWGS84.YMin:F6}, {extentInWGS84.XMax:F6}, {extentInWGS84.YMax:F6}");
                        System.Diagnostics.Debug.WriteLine($"Projected extent: {extentInWGS84.XMin}, {extentInWGS84.YMin}, {extentInWGS84.XMax}, {extentInWGS84.YMax}");
                    }
                    else
                    {
                        AddToLog("ERROR: Projection to WGS84 resulted in a null envelope. Using original extent.");
                        System.Diagnostics.Debug.WriteLine("ERROR: Projection to WGS84 resulted in a null envelope.");
                        extentInWGS84 = extent; // Fallback to original if projection fails
                    }
                }
                catch (Exception ex)
                {
                    AddToLog($"ERROR: Failed to project custom extent to WGS84: {ex.Message}. Using original extent.");
                    System.Diagnostics.Debug.WriteLine($"ERROR projecting extent: {ex.Message}");
                    extentInWGS84 = extent; // Fallback to original on error
                }
            }
            else
            {
                AddToLog("Custom extent is already in WGS84 or has no spatial reference, assuming WGS84.");
                System.Diagnostics.Debug.WriteLine("Custom extent is already WGS84 or no SR defined.");
            }

            // Store the extent (potentially projected) - this will trigger the property change handlers
            CustomExtent = extentInWGS84;

            // Explicitly set these properties to ensure UI updates
            HasCustomExtent = true;
            UpdateCustomExtentDisplay();
            NotifyPropertyChanged(nameof(CustomExtentDisplay));
            NotifyPropertyChanged(nameof(HasCustomExtent));

            // Make sure custom extent radio is selected
            UseCustomExtent = true; // This will also set UseCurrentMapExtent = false via its setter

            // Ensure tool is deactivated and provide feedback
            QueuedTask.Run(async () => {
                try
                {
                    var mapView = MapView.Active;
                    if (mapView != null)
                    {
                        mapView.CancelDrawing();
                        System.Diagnostics.Debug.WriteLine("MapView.CancelDrawing() called.");
                    }

                    // Deactivate the custom drawing tool and return to the default explore tool
                    // First, explicitly deactivate the current tool (which should be our CustomExtentTool)
                    await FrameworkApplication.SetCurrentToolAsync(null);
                    System.Diagnostics.Debug.WriteLine("Current tool explicitly deactivated (set to null).");

                    // Then, activate the default explore tool
                    await FrameworkApplication.SetCurrentToolAsync("esri_mapping_exploreTool");
                    System.Diagnostics.Debug.WriteLine("Default explore tool activated.");

                    // Give the UI thread a moment for the cursor to update etc.
                    await Task.Delay(300); // Increased delay slightly just in case

                    // Now that the tool is reset, log the next steps and show confirmation
                    AddToLog("Custom extent successfully set and drawing tool deactivated.");
                    AddToLog("Custom extent will be used for data loading."); // User's referenced log
                    AddToLog("You may now select data themes and click 'Load Data'.");

                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                        $"Custom extent set successfully:\nMin X,Y: {extent.XMin:F4}, {extent.YMin:F4}\nMax X,Y: {extent.XMax:F4}, {extent.YMax:F4}",
                        "Custom Extent Set",
                        System.Windows.MessageBoxButton.OK,
                        System.Windows.MessageBoxImage.Information);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Error during tool deactivation or showing custom extent feedback: {ex.Message}");
                    AddToLog($"Error after setting extent: {ex.Message}");
                }
            });

            // The NotifyPropertyChanged calls for UseCustomExtent and UseCurrentMapExtent
            // are handled by their respective property setters when 'UseCustomExtent = true;' is executed.
            // Thus, the explicit calls previously here are redundant.
        }

        private void SetPolygonExtent()
        {
            UsePolygonExtent = true;
            QueuedTask.Run(async () =>
            {
                var mapView = MapView.Active;
                if (mapView == null)
                {
                    AddToLog("Unable to draw a polygon area: no active map view.");
                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                        "Please open a map before drawing a polygon area.",
                        "No Active Map");
                    return;
                }
                try
                {
                    await FrameworkApplication.SetCurrentToolAsync("DuckDBGeoparquet_CustomPolygonTool");
                    AddToLog("Draw a polygon on the map (double-click to finish) to set the download area...");
                }
                catch (Exception ex)
                {
                    AddToLog($"Error activating the polygon tool: {ex.Message}");
                }
            });
        }

        // Handler for when the polygon tool completes a sketch
        private void OnAoiPolygonCreated(Geometry geometry)
        {
            try
            {
                if (geometry is not Polygon polygon || polygon.IsEmpty)
                {
                    AddToLog("Polygon sketch was empty; draw again.");
                    return;
                }

                if (geometry.SpatialReference == null || geometry.SpatialReference.Wkid != ExtentResolution.Wgs84Wkid)
                {
                    SpatialReference wgs84 = SpatialReferenceBuilder.CreateSpatialReference(ExtentResolution.Wgs84Wkid);
                    polygon = GeometryEngine.Instance.Project(polygon, wgs84) as Polygon;
                    if (polygon == null || polygon.IsEmpty)
                    {
                        AddToLog("ERROR: Could not project the drawn polygon to WGS84. Draw again on a map with a known coordinate system.");
                        return;
                    }
                }

                // ponytail: sketches are single-part; a multipart polygon would need
                // MULTIPOLYGON WKT — add if a workflow ever produces one.
                if (polygon.PartCount > 1)
                    AddToLog($"Polygon has {polygon.PartCount} parts; only the outer ring of the first part is used.");

                var ring = polygon.Parts[0]
                    .Select(segment => (segment.StartPoint.X, segment.StartPoint.Y))
                    .ToList();
                _aoiPolygonWkt = GeoParquetSql.BuildPolygonWkt(ring);
                _aoiPolygonEnvelope = polygon.Extent;
                HasAoiPolygon = true;
                AoiPolygonDisplay = $"{ring.Count} vertices; extent {polygon.Extent.XMin:F4}, {polygon.Extent.YMin:F4} to {polygon.Extent.XMax:F4}, {polygon.Extent.YMax:F4}";
                UsePolygonExtent = true;
                AddToLog($"Polygon area set ({AoiPolygonDisplay}).");

                QueuedTask.Run(async () =>
                {
                    try
                    {
                        MapView.Active?.CancelDrawing();
                        await FrameworkApplication.SetCurrentToolAsync("esri_mapping_exploreTool");
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"Polygon tool deactivation: {ex.Message}");
                    }
                });
            }
            catch (Exception ex)
            {
                AddToLog($"Error setting the polygon area: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"OnAoiPolygonCreated: {ex}");
            }
        }

        private const int SelectedFeatureMaxVertices = 4000;

        /// <summary>
        /// Captures the active map's selected polygon features as the download
        /// area: union, project to WGS84, generalize big boundaries (a state
        /// outline can carry 100k+ vertices and the WKT is embedded in SQL —
        /// the envelope pushdown does the coarse filtering, so light
        /// generalization barely changes results), then export WKT.
        /// </summary>
        private void CaptureSelectedFeatures()
        {
            UseSelectedFeatureExtent = true;
            QueuedTask.Run(() =>
            {
                try
                {
                    var map = MapView.Active?.Map;
                    if (map == null)
                    {
                        AddToLog("Unable to use selected features: no active map.");
                        return;
                    }
                    SpatialReference wgs84 = SpatialReferenceBuilder.CreateSpatialReference(ExtentResolution.Wgs84Wkid);
                    var polygons = new List<Polygon>();
                    int featureCount = 0;
                    foreach (var entry in map.GetSelection().ToDictionary())
                    {
                        if (entry.Key is not FeatureLayer featureLayer ||
                            featureLayer.ShapeType != esriGeometryType.esriGeometryPolygon)
                            continue;
                        using var selection = featureLayer.GetSelection();
                        using var cursor = selection.Search(null);
                        while (cursor.MoveNext())
                        {
                            if (cursor.Current is not ArcGIS.Core.Data.Feature feature) continue;
                            if (feature.GetShape() is not Polygon shape || shape.IsEmpty) continue;
                            var projected = ExtentResolution.NeedsProjectionToWgs84(shape.SpatialReference?.Wkid)
                                ? GeometryEngine.Instance.Project(shape, wgs84) as Polygon
                                : shape;
                            if (projected != null && !projected.IsEmpty)
                            {
                                polygons.Add(projected);
                                featureCount++;
                            }
                        }
                    }
                    if (polygons.Count == 0)
                    {
                        AddToLog("No selected polygon features found. Select one or more polygons on the map (e.g. a county boundary), then click Use Selected Features again.");
                        return;
                    }

                    Geometry combined = polygons.Count == 1 ? polygons[0] : GeometryEngine.Instance.Union(polygons);
                    if (combined is not Polygon aoi || aoi.IsEmpty)
                    {
                        AddToLog("Could not combine the selected features into a polygon area.");
                        return;
                    }

                    int originalVertices = aoi.PointCount;
                    double deviation = 0.0001; // ~11 m in degrees
                    while (aoi.PointCount > SelectedFeatureMaxVertices && deviation <= 0.01)
                    {
                        if (GeometryEngine.Instance.Generalize(aoi, deviation) is Polygon simpler && !simpler.IsEmpty)
                            aoi = simpler;
                        deviation *= 2;
                    }

                    string wkt = GeometryEngine.Instance.ExportToWKT(
                        WktExportFlags.WktExportStripZs | WktExportFlags.WktExportStripMs, aoi);
                    if (!GeoParquetSql.IsValidAoiWkt(wkt))
                    {
                        AddToLog("Could not convert the selected features to a usable polygon area.");
                        return;
                    }

                    _selectedFeatureWkt = wkt;
                    _selectedFeatureEnvelope = aoi.Extent;
                    HasSelectedFeatureAoi = true;
                    SelectedFeatureAoiDisplay =
                        $"{featureCount} feature(s), {aoi.PointCount} vertices" +
                        (aoi.PointCount < originalVertices ? $" (generalized from {originalVertices})" : "") +
                        $"; extent {aoi.Extent.XMin:F4}, {aoi.Extent.YMin:F4} to {aoi.Extent.XMax:F4}, {aoi.Extent.YMax:F4}";
                    AddToLog($"Selected-feature area set ({SelectedFeatureAoiDisplay}).");
                }
                catch (Exception ex)
                {
                    AddToLog($"Error using the selected features: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"CaptureSelectedFeatures: {ex}");
                }
            });
        }

        private void ClearSelectedFeatureAoi()
        {
            _selectedFeatureWkt = null;
            _selectedFeatureEnvelope = null;
            HasSelectedFeatureAoi = false;
            SelectedFeatureAoiDisplay = "No features captured";
        }

        private void ClearAoiPolygon()
        {
            _aoiPolygonWkt = null;
            _aoiPolygonEnvelope = null;
            HasAoiPolygon = false;
            AoiPolygonDisplay = "No polygon drawn";
        }

        private async Task RefreshExtentLayersAsync()
        {
            try
            {
                var mapView = MapView.Active;
                if (mapView?.Map == null)
                {
                    AvailableExtentLayers = new ObservableCollection<Layer>();
                    SelectedExtentLayer = null;
                    AddToLog("No active map view; open a map to list its layers.");
                    return;
                }
                var layers = await QueuedTask.Run(() => mapView.Map.GetLayersAsFlattenedList().ToList());
                var previous = SelectedExtentLayer;
                AvailableExtentLayers = new ObservableCollection<Layer>(layers);
                SelectedExtentLayer = previous != null && layers.Contains(previous) ? previous : layers.FirstOrDefault();
            }
            catch (Exception ex)
            {
                AddToLog($"Could not list map layers: {ex.Message}");
            }
        }

        private void BrowseMfcLocation()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select folder for MFC (.mfc) file",
                UseDescriptionForTitle = true,
                SelectedPath = MfcOutputPath ?? Path.Combine(DeterminedDefaultMfcBasePath, "Connections")
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                // Set the MfcOutputPath to the folder where the .mfc file itself will be saved.
                MfcOutputPath = dialog.SelectedPath;

                // Display helpful information to the user
                AddToLog($"MFC connection file will be saved in: {MfcOutputPath}");
                AddToLog($"Ensure your GeoParquet data files are located in: {DataOutputPath}");
            }
        }

        private void BrowseDataLocation()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select folder for GeoParquet data files",
                UseDescriptionForTitle = true,
                SelectedPath = DataOutputPath ?? Path.Combine(DeterminedDefaultMfcBasePath, "Data")
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                DataOutputPath = dialog.SelectedPath;
                AddToLog($"Data files will be saved to: {DataOutputPath}");
            }
        }

        private void BrowseFileGeodatabase()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select a file geodatabase (.gdb) or a folder that should contain Overture.gdb",
                UseDescriptionForTitle = true,
                SelectedPath = ResolveFileGeodatabaseBrowseStart()
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                FileGeodatabasePath = dialog.SelectedPath;
                AddToLog($"File geodatabase output set to: {FileGeodatabaseOutput.ResolveGeodatabasePath(DataOutputPath, FileGeodatabasePath)}");
            }
        }

        private string ResolveFileGeodatabaseBrowseStart()
        {
            if (!string.IsNullOrWhiteSpace(FileGeodatabasePath) && Directory.Exists(FileGeodatabasePath))
                return FileGeodatabasePath;

            if (!string.IsNullOrWhiteSpace(DataOutputPath) && Directory.Exists(DataOutputPath))
                return DataOutputPath;

            return Path.Combine(DeterminedDefaultMfcBasePath, "Data");
        }

        private void BrowseCustomDataFolder()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select folder containing GeoParquet data files",
                UseDescriptionForTitle = true,
                SelectedPath = CustomDataFolderPath ?? DataOutputPath ?? Path.Combine(DeterminedDefaultMfcBasePath, "Data")
            };

            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                CustomDataFolderPath = dialog.SelectedPath;
                AddToLog($"Custom data folder set to: {CustomDataFolderPath}");

                // Update command can execute
                (CreateMfcCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private void UpdateCustomExtentDisplay()
        {
            if (_customExtent != null)
            {
                CustomExtentDisplay = $"Min X: {_customExtent.XMin:F4}\nMin Y: {_customExtent.YMin:F4}\nMax X: {_customExtent.XMax:F4}\nMax Y: {_customExtent.YMax:F4}";
            }
            else
            {
                CustomExtentDisplay = "No custom extent set";
            }
        }
        #endregion
    }
}
