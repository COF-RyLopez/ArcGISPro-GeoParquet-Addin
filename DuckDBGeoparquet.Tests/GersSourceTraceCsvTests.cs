using DuckDBGeoparquet.Services;
using System;
using System.IO;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GersSourceTraceCsvTests
    {
        [Fact]
        public void ExportGersIdsFromBridgeCsv_ReadsIdColumn()
        {
            string bridgePath = Path.Combine(Path.GetTempPath(), $"gers_bridge_{Guid.NewGuid():N}.csv");
            File.WriteAllText(bridgePath,
                "id,record_id,theme,type\n" +
                "abc-1,100,addresses,address\n" +
                "abc-2,101,addresses,address\n" +
                "abc-1,100,addresses,address\n");

            try
            {
                string traceInput = GersSourceTraceCsv.ExportGersIdsFromBridgeCsv(bridgePath);
                string[] lines = File.ReadAllLines(traceInput);
                Assert.Equal(3, lines.Length);
                Assert.Equal("gers_id", lines[0]);
                Assert.Contains("abc-1", lines[1]);
                Assert.Contains("abc-2", lines[2]);
            }
            finally
            {
                File.Delete(bridgePath);
            }
        }

        [Fact]
        public void EnrichTraceOutputCsv_AddsEditColumns()
        {
            string csvPath = Path.Combine(Path.GetTempPath(), $"gers_trace_{Guid.NewGuid():N}.csv");
            File.WriteAllText(csvPath,
                "gers_id,dataset,record_id,update_time,theme,type,source_url\n" +
                "g1,OpenStreetMap,w42@1,2024-01-01,addresses,address,\n");

            try
            {
                GersSourceTraceCsv.EnrichTraceOutputCsv(csvPath);
                string[] lines = File.ReadAllLines(csvPath);
                Assert.Contains("edit_platform", lines[0]);
                Assert.Contains("OpenStreetMap", lines[1]);
                Assert.Contains("https://www.openstreetmap.org/way/42", lines[1]);
            }
            finally
            {
                File.Delete(csvPath);
            }
        }
    }
}
