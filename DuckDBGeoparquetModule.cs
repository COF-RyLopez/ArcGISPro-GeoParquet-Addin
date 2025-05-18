using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using System;
using System.Windows;
using System.Windows.Media;

namespace DuckDBGeoparquet
{
    /// <summary>
    /// Main module for the Overture Maps GeoParquet add-in
    /// </summary>
    internal class DuckDBGeoparquetModule : Module
    {
        private static DuckDBGeoparquetModule _this = null;

        /// <summary>
        /// Gets the singleton instance of this module
        /// </summary>
        public static DuckDBGeoparquetModule Current
        {
            get
            {
                return _this ?? (_this = (DuckDBGeoparquetModule)FrameworkApplication.FindModule("DuckDBGeoparquet_Module"));
            }
        }

        /// <summary>
        /// Gets whether the application is currently using the dark theme
        /// </summary>
        public static bool IsDarkTheme
        {
            get
            {
                try
                {
                    return FrameworkApplication.ApplicationTheme == ApplicationTheme.Dark;
                }
                catch
                {
                    // If we can't access the framework (e.g., design time), return default
                    return false;
                }
            }
        }

        /// <summary>
        /// Initialize the module
        /// </summary>
        protected override bool Initialize()
        {
            System.Diagnostics.Debug.WriteLine($"Initializing with ArcGIS Pro theme: {(IsDarkTheme ? "Dark" : "Light")}");
            return true;
        }

        /// <summary>
        /// Cleanup when unloading the module
        /// </summary>
        protected override bool CanUnload()
        {
            // Allow ArcGIS Pro to close if your add-in is not busy.
            return true;
        }
    }
}
