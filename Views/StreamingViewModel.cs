using System;
using System.Data;
using System.Collections.Generic;
using System.Windows.Input;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
// Assume a DuckDB .NET library reference:
using DuckDB.NET;
using ArcGIS.Core.Data.Realtime;
using ArcGIS.Desktop.Framework.Contracts;
using DuckDB.NET.Data;

namespace DuckDBGeoparquet.Views
{
    internal class StreamingViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_Streaming";
        private string _heading = "My Streaming DockPane";
        public string Heading
        {
            get => _heading;
            set => SetProperty(ref _heading, value);
        }

        // Command to refresh streaming data.
        private ICommand _refreshCommand;
        public ICommand RefreshCommand => _refreshCommand ?? (_refreshCommand = new RelayCommand(() => RefreshStreamingData()));

        protected StreamingViewModel() { }

        internal static void Show()
        {
            DockPane pane = FrameworkApplication.DockPaneManager.Find(_dockPaneID);
            if (pane != null)
                pane.Activate();
        }

        private async void RefreshStreamingData()
        {
            await QueuedTask.Run(() =>
            {
                // Connect to DuckDB (in-memory example – adjust connection string as needed)
                using (var conn = new DuckDBConnection("Data Source=:memory:"))
                {
                    conn.Open();
                    // Here we assume you've already loaded your spatial data into "spatial_table"
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT X, Y, Field1, Field2 FROM spatial_table";
                        using (var reader = cmd.ExecuteReader())
                        {
                            // Clear previous features from our custom realtime feature class.
                            CustomRealtimeFeatureClass.Instance.ClearFeatures();

                            while (reader.Read())
                            {
                                var feature = CreateFeatureFromReader(reader);
                                CustomRealtimeFeatureClass.Instance.AddFeature(feature);
                            }
                        }
                    }
                }
                // Start streaming the updated features.
                CustomRealtimeFeatureClass.Instance.StartStreaming();
            });
            Heading = $"Streaming refreshed at {DateTime.Now:T}";
        }

        // Converts a DuckDB row into a RealtimeFeature.
        private RealtimeFeature CreateFeatureFromReader(IDataReader reader)
        {
            double x = Convert.ToDouble(reader["X"]);
            double y = Convert.ToDouble(reader["Y"]);
            // Create a point geometry (using WGS84 here; adjust as needed)
            MapPoint point = MapPointBuilder.CreateMapPoint(x, y, SpatialReferences.WGS84);
            var attributes = new Dictionary<string, object>
            {
                { "Field1", reader["Field1"] },
                { "Field2", reader["Field2"] }
            };
            return new RealtimeFeature(point, attributes);
        }
    }
}
