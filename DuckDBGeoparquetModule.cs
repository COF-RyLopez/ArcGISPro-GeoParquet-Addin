using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;

namespace DuckDBGeoparquet
{
    internal class DuckDBGeoparquetModule : Module
    {
        private static DuckDBGeoparquetModule _this = null;

        public static DuckDBGeoparquetModule Current
        {
            get
            {
                return _this ?? (_this = (DuckDBGeoparquetModule)FrameworkApplication.FindModule("DuckDBGeoparquet_Module"));
            }
        }

        protected override bool CanUnload()
        {
            // Allow ArcGIS Pro to close if your add‑in is not busy.
            return true;
        }
    }
}
