using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using System;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Custom map tool for drawing an extent rectangle
    /// </summary>
    internal class CustomExtentTool : MapTool
    {
        // Static event to notify when an extent is created - this will work across different instances
        public static event Action<Envelope> ExtentCreatedStatic;

        // Keep the instance event for backward compatibility
        public event Action<Envelope> ExtentCreated;

        public CustomExtentTool()
        {
            // Standard rectangle tool
            SketchType = SketchGeometryType.Rectangle;
            // Don't keep the sketch on the map
            SketchOutputMode = SketchOutputMode.Map;
            // Use standard cursor
            IsSketchTool = true;
            // No special properties for sketch
            SketchSymbol = null;

            System.Diagnostics.Debug.WriteLine("CustomExtentTool constructor called");
        }

        protected override Task OnToolActivateAsync(bool active)
        {
            System.Diagnostics.Debug.WriteLine($"CustomExtentTool activated: {active}");
            return base.OnToolActivateAsync(active);
        }

        protected override Task<bool> OnSketchCompleteAsync(Geometry geometry)
        {
            // Log the completion
            System.Diagnostics.Debug.WriteLine("OnSketchCompleteAsync called with geometry type: " +
                (geometry != null ? geometry.GeometryType.ToString() : "null"));

            try
            {
                if (geometry != null)
                {
                    System.Diagnostics.Debug.WriteLine($"Geometry created: {geometry.GeometryType}");

                    // Get the envelope from the drawn geometry
                    Envelope extent = geometry.Extent;
                    System.Diagnostics.Debug.WriteLine($"Extent: {extent.XMin}, {extent.YMin}, {extent.XMax}, {extent.YMax}");

                    // Check if instance event has subscribers
                    if (ExtentCreated != null)
                    {
                        System.Diagnostics.Debug.WriteLine("Instance ExtentCreated event has subscribers, invoking");
                        // Invoke the event with the new extent
                        ExtentCreated?.Invoke(extent);
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("Instance ExtentCreated event has no subscribers");
                    }

                    // Check if static event has subscribers
                    if (ExtentCreatedStatic != null)
                    {
                        System.Diagnostics.Debug.WriteLine("Static ExtentCreatedStatic event has subscribers, invoking");
                        // Invoke the static event with the new extent
                        ExtentCreatedStatic?.Invoke(extent);
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("WARNING: ExtentCreatedStatic event has no subscribers!");
                    }

                    // The ViewModel (WizardDockpaneViewModel) is now responsible for deactivating the tool
                    // after this event is processed. Remove tool deactivation from here.
                    // await FrameworkApplication.SetCurrentToolAsync("esri_mapping_exploreTool");
                    // System.Diagnostics.Debug.WriteLine("Returned to default tool");

                    // Force a UI update to make sure the cursor changes back
                    // await QueuedTask.Run(() => {
                    //     // Just a quick operation to ensure we're on the main thread
                    //     var activeView = MapView.Active;
                    //     if (activeView != null)
                    //     {
                    //        System.Diagnostics.Debug.WriteLine("Forcing cursor update on active map view");
                    //     }
                    // });
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("WARNING: Geometry is null in OnSketchCompleteAsync");
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error in OnSketchCompleteAsync: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Stack trace: {ex.StackTrace}");
            }

            return Task.FromResult(true);
        }
    }
}