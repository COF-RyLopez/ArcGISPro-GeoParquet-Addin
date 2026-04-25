using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;

namespace DuckDBGeoparquet
{
    internal class OvertureGeocoderModule : Module
    {
        private static OvertureGeocoderModule _this;

        public static OvertureGeocoderModule Current =>
            _this ??= (OvertureGeocoderModule)FrameworkApplication.FindModule("DuckDBGeoparquet_Geocoder_Module");

        protected override bool Initialize()
        {
            System.Diagnostics.Debug.WriteLine("OvertureGeocoderModule initialized");
            return true;
        }

        protected override bool CanUnload()
        {
            return true;
        }
    }
}
