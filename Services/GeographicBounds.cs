using DuckDBGeoparquet.Models;
using System;
using System.Globalization;

namespace DuckDBGeoparquet.Services
{
    public static class GeographicBounds
    {
        /// <summary>
        /// Parses a user-typed coordinate. Always accepts '.' as the decimal
        /// separator; a lone ',' with no '.' is treated as a decimal comma so
        /// values pasted from comma-decimal locales work. Thousands separators
        /// are never accepted — "1,234" means 1.234, not 1234.
        /// </summary>
        public static bool TryParseCoordinate(string text, out double value)
        {
            value = 0;
            text = text?.Trim();
            if (string.IsNullOrEmpty(text)) return false;
            if (!text.Contains('.') && CountChar(text, ',') == 1)
                text = text.Replace(',', '.');
            return double.TryParse(text,
                NumberStyles.Float & ~NumberStyles.AllowThousands,
                CultureInfo.InvariantCulture, out value) && double.IsFinite(value);
        }

        private static int CountChar(string text, char c)
        {
            int count = 0;
            foreach (char ch in text) if (ch == c) count++;
            return count;
        }

        /// <summary>
        /// Parses the four typed bbox fields into a validated WGS84 extent.
        /// Returns a user-facing error message, or null on success.
        /// </summary>
        public static string TryParseBbox(string west, string south, string east, string north, out ExtentBounds bounds)
        {
            bounds = null;
            if (!TryParseCoordinate(west, out double xMin)) return "West must be a number (decimal degrees).";
            if (!TryParseCoordinate(south, out double yMin)) return "South must be a number (decimal degrees).";
            if (!TryParseCoordinate(east, out double xMax)) return "East must be a number (decimal degrees).";
            if (!TryParseCoordinate(north, out double yMax)) return "North must be a number (decimal degrees).";
            if (xMin < -180 || xMin > 180 || xMax < -180 || xMax > 180)
                return "West and East must be between -180 and 180.";
            if (yMin < -90 || yMin > 90 || yMax < -90 || yMax > 90)
                return "South and North must be between -90 and 90.";
            if (xMin >= xMax) return "West must be less than East.";
            if (yMin >= yMax) return "South must be less than North.";
            bounds = new ExtentBounds(xMin, yMin, xMax, yMax);
            return null;
        }

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
