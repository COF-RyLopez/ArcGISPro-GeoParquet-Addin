using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public class ExtentInfo
    {
        public double XMin { get; set; }
        public double YMin { get; set; }
        public double XMax { get; set; }
        public double YMax { get; set; }
    }

    public interface IDataService : IDisposable
    {
        bool EnableGeometryRepair { get; set; }

        Task InitializeAsync();
        
        Task<bool> IngestFileAsync(string s3Path, ExtentInfo extent = null, string actualS3Type = null, IProgress<string> progress = null);
        
        Task<DataTable> GetPreviewDataAsync();
        
        Task CreateFeatureLayerAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot);
        
        List<LayerCreationInfo> GetPendingLayers();
        
        void ClearPendingLayers();
    }
}
