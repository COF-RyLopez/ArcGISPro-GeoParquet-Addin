using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Mapping;
using System;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Map tool for sketching a polygon area of interest. Mirrors
    /// <see cref="CustomExtentTool"/> but keeps the full polygon geometry so
    /// downloads can ST_Intersects against — and optionally clip to — the
    /// drawn shape rather than its bounding box.
    /// </summary>
    internal class CustomPolygonTool : MapTool
    {
        /// <summary>Raised with the sketched geometry (map spatial reference).</summary>
        public static event Action<Geometry> PolygonCreatedStatic;

        public CustomPolygonTool()
        {
            SketchType = SketchGeometryType.Polygon;
            SketchOutputMode = SketchOutputMode.Map;
            IsSketchTool = true;
            SketchSymbol = null;
        }

        protected override Task<bool> OnSketchCompleteAsync(Geometry geometry)
        {
            try
            {
                if (geometry != null)
                    PolygonCreatedStatic?.Invoke(geometry);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"CustomPolygonTool.OnSketchCompleteAsync: {ex.Message}");
            }
            return Task.FromResult(true);
        }
    }
}
