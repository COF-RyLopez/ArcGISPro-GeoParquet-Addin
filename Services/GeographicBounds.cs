using DuckDBGeoparquet.Models;
using System;

namespace DuckDBGeoparquet.Services
{
    public static class GeographicBounds
    {
        public static void Validate(ExtentBounds bounds)
        {
            if (bounds == null || !double.IsFinite(bounds.XMin) || !double.IsFinite(bounds.XMax) ||
                !double.IsFinite(bounds.YMin) || !double.IsFinite(bounds.YMax) ||
                bounds.XMin < -180 || bounds.XMax > 180 || bounds.YMin < -90 || bounds.YMax > 90 ||
                bounds.XMin > bounds.XMax || bounds.YMin > bounds.YMax)
                throw new ArgumentException("The area must be a valid WGS84 extent. Draw a new area on the map.");
        }

        /// <summary>Conservative candidate bounds; exact geodesic distance remains the final test.</summary>
        public static ExtentBounds Expand(ExtentBounds bounds, double meters)
        {
            Validate(bounds);
            if (!double.IsFinite(meters) || meters < 0) throw new ArgumentOutOfRangeException(nameof(meters));
            double latitudePadding = meters / 110000.0;
            double south = Math.Max(-90, bounds.YMin - latitudePadding);
            double north = Math.Min(90, bounds.YMax + latitudePadding);
            double extremeLatitude = Math.Max(Math.Abs(south), Math.Abs(north));
            double longitudePadding = extremeLatitude >= 89.9 ? 180 : meters / (110000.0 * Math.Cos(extremeLatitude * Math.PI / 180));
            double west = bounds.XMin - longitudePadding;
            double east = bounds.XMax + longitudePadding;
            // A single non-wrapping envelope must include both sides of a dateline/pole search.
            if (west < -180 || east > 180 || extremeLatitude >= 89.9) { west = -180; east = 180; }
            return new ExtentBounds(west, south, east, north);
        }
    }
}
