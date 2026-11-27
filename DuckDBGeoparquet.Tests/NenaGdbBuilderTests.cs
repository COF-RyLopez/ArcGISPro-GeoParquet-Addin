using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class NenaGdbBuilderTests
    {
        // The shape open-ng911-addresses' export writes next to the GeoPackage (<gpkg>.report.json)
        private const string Report = @"{
          ""template"": ""NG911_GISDataModelTemplate_v3.0.gpkg"",
          ""layers"": {""SiteStructureAddressPoint"": 10, ""RoadCenterLine"": 4, ""FirePolygon"": 2, ""DistanceMarkerPoint"": 0},
          ""agency_owed"": {""FirePolygon"": {""Agency_ID"": 2, ""ServiceURI"": 2, ""AVcard_URI"": 2}},
          ""problems"": {
            ""RoadCenterLine"": {""St_Name"": {""missing_required"": 1}, ""St_PosTyp"": {""outside_domain"": 3, ""examples"": [""Private""]}}
          }
        }";

        [Fact]
        public void Plan_NullsTheFieldsTheReportFlags_AndLoadsLayersWithFeatures()
        {
            var plan = NenaGdbPlan.FromReport(Report);

            Assert.Equal(new[] { "SiteStructureAddressPoint", "RoadCenterLine", "FirePolygon" }, plan.Layers);
            Assert.Equal(
                new[] { ("FirePolygon", "Agency_ID"), ("FirePolygon", "ServiceURI"), ("FirePolygon", "AVcard_URI"), ("RoadCenterLine", "St_Name") },
                plan.NullableFields);
            Assert.Empty(plan.SkippedLayers);
        }

        [Fact]
        public void Plan_SkipsALayerTheExportDidNotWrite()
        {
            var plan = NenaGdbPlan.FromReport(@"{""layers"": {""RoadCenterLine"": 4},
                ""problems"": {""RoadCenterLine"": {""St_Name"": {""too_long"": 1, ""missing_required"": 2}}}}");

            Assert.Empty(plan.Layers);
            Assert.Empty(plan.NullableFields);
            Assert.Equal(new[] { "RoadCenterLine" }, plan.SkippedLayers);
        }

        [Fact]
        public async Task Build_CopiesTemplate_AltersFlaggedFields_ThenAppendsEachLayer()
        {
            using var dir = new TempDir();
            string gpkg = dir.File("us-vt.gpkg", "gpkg");
            File.WriteAllText(NenaGdbBuilder.ReportPath(gpkg), Report);
            string template = dir.Template();
            string gdb = NenaGdbBuilder.DefaultGeodatabasePath(gpkg);
            var runner = new FakeGeoprocessingRunner();

            var result = await new NenaGdbBuilder(runner).BuildAsync(gpkg, gdb, template, overwriteExisting: false);

            Assert.True(result.Succeeded, string.Join("\n", result.Messages));
            Assert.Equal(Path.Combine(dir.Path, "us-vt.gdb"), gdb);
            Assert.True(File.Exists(Path.Combine(gdb, "gdb")), "the template's own files");
            Assert.Equal(3, result.LoadedLayers);

            var alters = runner.Calls.Where(c => c.Tool == NenaGdbBuilder.AlterFieldTool).ToList();
            Assert.Equal(4, alters.Count);
            Assert.Equal(new object[] { Path.Combine(gdb, "RoadCenterLine"), "St_Name", "", "", "", "", "NULLABLE" }, alters[3].Values);

            var appends = runner.Calls.Where(c => c.Tool == NenaGdbBuilder.AppendTool).ToList();
            Assert.Equal(new object[] { Path.Combine(gpkg, "main.FirePolygon"), Path.Combine(gdb, "FirePolygon"), "NO_TEST" }, appends[2].Values);
            Assert.True(runner.Calls.FindLastIndex(c => c.Tool == NenaGdbBuilder.AlterFieldTool)
                        < runner.Calls.FindIndex(c => c.Tool == NenaGdbBuilder.AppendTool),
                "fields are altered while the template is still empty");
        }

        [Fact]
        public async Task Build_StopsWithoutTheReport()
        {
            using var dir = new TempDir();
            string gpkg = dir.File("us-vt.gpkg", "gpkg");
            var runner = new FakeGeoprocessingRunner();

            var result = await new NenaGdbBuilder(runner).BuildAsync(gpkg, dir.Path + "/x.gdb", dir.Template(), false);

            Assert.False(result.Succeeded);
            Assert.Contains("report", result.Messages.Last());
            Assert.Empty(runner.Calls);
        }

        [Fact]
        public async Task Build_KeepsAnExistingGeodatabaseUnlessOverwriting()
        {
            using var dir = new TempDir();
            string gpkg = dir.File("us-vt.gpkg", "gpkg");
            File.WriteAllText(NenaGdbBuilder.ReportPath(gpkg), Report);
            string gdb = NenaGdbBuilder.DefaultGeodatabasePath(gpkg);
            Directory.CreateDirectory(gdb);
            File.WriteAllText(Path.Combine(gdb, "keep"), "x");
            var runner = new FakeGeoprocessingRunner();

            var kept = await new NenaGdbBuilder(runner).BuildAsync(gpkg, gdb, dir.Template(), overwriteExisting: false);
            Assert.False(kept.Succeeded);
            Assert.True(File.Exists(Path.Combine(gdb, "keep")));
            Assert.Empty(runner.Calls);

            var replaced = await new NenaGdbBuilder(runner).BuildAsync(gpkg, gdb, dir.Template(), overwriteExisting: true);
            Assert.True(replaced.Succeeded);
            Assert.False(File.Exists(Path.Combine(gdb, "keep")));
        }

        [Fact]
        public async Task Build_ReportsAFailedAppend()
        {
            using var dir = new TempDir();
            string gpkg = dir.File("us-vt.gpkg", "gpkg");
            File.WriteAllText(NenaGdbBuilder.ReportPath(gpkg), Report);
            var runner = new FakeGeoprocessingRunner
            {
                Handler = (tool, values) => tool == NenaGdbBuilder.AppendTool && ((string)values[1]).EndsWith("RoadCenterLine")
                    ? GeoprocessingToolResult.Failure("ERROR 000224")
                    : GeoprocessingToolResult.Success()
            };

            var result = await new NenaGdbBuilder(runner).BuildAsync(gpkg, NenaGdbBuilder.DefaultGeodatabasePath(gpkg), dir.Template(), false);

            Assert.False(result.Succeeded);
            Assert.Equal(1, result.LoadedLayers);
            Assert.Contains("RoadCenterLine", result.Messages.Last());
            Assert.Contains("ERROR 000224", result.Messages.Last());
        }

        [Fact]
        public void BundledTemplate_IsInTheRepository()
        {
            string repo = AppContext.BaseDirectory;
            while (repo != null && !File.Exists(Path.Combine(repo, "Config.daml")))
                repo = Path.GetDirectoryName(repo);
            Assert.NotNull(repo);
            string zip = NenaGdbBuilder.BundledTemplatePath(repo);
            using var archive = ZipFile.OpenRead(zip);
            Assert.Contains(archive.Entries, e => e.FullName.StartsWith("NG911_GISDataModelTemplate_v3.0.gdb/"));
        }

        private sealed class TempDir : IDisposable
        {
            public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "nena_gdb_" + Guid.NewGuid().ToString("N"));
            public TempDir() => Directory.CreateDirectory(Path);

            public string File(string name, string content)
            {
                string p = System.IO.Path.Combine(Path, name);
                System.IO.File.WriteAllText(p, content);
                return p;
            }

            /// <summary>A stand-in template: a zip holding one .gdb folder.</summary>
            public string Template()
            {
                string zip = System.IO.Path.Combine(Path, "template.zip");
                if (System.IO.File.Exists(zip))
                    return zip;
                using var archive = ZipFile.Open(zip, ZipArchiveMode.Create);
                var entry = archive.CreateEntry("NG911_GISDataModelTemplate_v3.0.gdb/gdb");
                using var w = new StreamWriter(entry.Open());
                w.Write("template");
                return zip;
            }

            public void Dispose()
            {
                if (Directory.Exists(Path))
                    Directory.Delete(Path, recursive: true);
            }
        }

        private sealed class FakeGeoprocessingRunner : IGeoprocessingRunner
        {
            public List<(string Tool, object[] Values)> Calls { get; } = new();
            public Func<string, object[], GeoprocessingToolResult> Handler { get; set; }

            public Task<GeoprocessingToolResult> ExecuteAsync(
                string toolName, IReadOnlyList<object> values, bool overwriteOutput, CancellationToken cancellationToken = default)
            {
                var v = values.ToArray();
                Calls.Add((toolName, v));
                return Task.FromResult(Handler?.Invoke(toolName, v) ?? GeoprocessingToolResult.Success("ok"));
            }
        }
    }
}
