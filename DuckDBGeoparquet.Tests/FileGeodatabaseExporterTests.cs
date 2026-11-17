using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class FileGeodatabaseExporterTests
    {
        [Fact]
        public async Task DisabledSettings_DoesNotCallGeoprocessing()
        {
            var runner = new FakeGeoprocessingRunner();
            var exporter = new FileGeodatabaseExporter(runner);
            var layers = new List<LayerCreationInfo> { CreateLayer(CreateTempParquet()) };

            var result = await exporter.ExportPendingLayersAsync(
                layers,
                FileGeodatabaseOutputSettings.GeoParquetDefault,
                Path.GetTempPath());

            Assert.False(result.Attempted);
            Assert.Empty(runner.Calls);
            Assert.False(layers[0].IsFileGeodatabase);
        }

        [Fact]
        public async Task SuccessfulExport_RewritesPendingLayerToFeatureClass()
        {
            string parquet = CreateTempParquet();
            string outputFolder = Path.Combine(Path.GetTempPath(), $"fgdb_out_{Guid.NewGuid():N}");
            Directory.CreateDirectory(outputFolder);
            string gdbPath = Path.Combine(outputFolder, FileGeodatabaseOutput.DefaultGeodatabaseName);
            var runner = new FakeGeoprocessingRunner
            {
                Handler = (tool, values, _) =>
                {
                    if (tool == FileGeodatabaseOutput.CreateFileGdbTool)
                        Directory.CreateDirectory(gdbPath);
                    return GeoprocessingToolResult.Success("ok");
                }
            };
            var exporter = new FileGeodatabaseExporter(runner);
            var layer = CreateLayer(parquet);
            var settings = new FileGeodatabaseOutputSettings { Enabled = true, OverwriteExisting = true };

            try
            {
                var result = await exporter.ExportPendingLayersAsync(
                    new List<LayerCreationInfo> { layer },
                    settings,
                    outputFolder);

                Assert.True(result.GeodatabaseReady);
                Assert.Equal(1, result.ExportedCount);
                Assert.Equal(0, result.FailedCount);
                Assert.True(layer.IsFileGeodatabase);
                Assert.Equal(Path.Combine(gdbPath, "buildings_building_polygons"), layer.FilePath);
                Assert.Contains(runner.Calls, call => call.Tool == FileGeodatabaseOutput.CreateFileGdbTool);
                Assert.Contains(runner.Calls, call =>
                    call.Tool == FileGeodatabaseOutput.ExportFeaturesTool &&
                    Equals(call.Values[0], parquet) &&
                    call.Overwrite);
            }
            finally
            {
                Cleanup(parquet, outputFolder);
            }
        }

        [Fact]
        public async Task ExistingGdb_SkipsCreateFileGdb()
        {
            string parquet = CreateTempParquet();
            string gdbPath = Path.Combine(Path.GetTempPath(), $"existing_{Guid.NewGuid():N}.gdb");
            Directory.CreateDirectory(gdbPath);
            var runner = new FakeGeoprocessingRunner();
            var exporter = new FileGeodatabaseExporter(runner);
            var layer = CreateLayer(parquet);
            var settings = new FileGeodatabaseOutputSettings
            {
                Enabled = true,
                GeodatabasePath = gdbPath
            };

            try
            {
                var result = await exporter.ExportPendingLayersAsync(
                    new List<LayerCreationInfo> { layer },
                    settings,
                    Path.GetTempPath());

                Assert.Equal(1, result.ExportedCount);
                Assert.DoesNotContain(runner.Calls, call => call.Tool == FileGeodatabaseOutput.CreateFileGdbTool);
                Assert.Contains(runner.Calls, call => call.Tool == FileGeodatabaseOutput.ExportFeaturesTool);
            }
            finally
            {
                Cleanup(parquet, gdbPath);
            }
        }

        [Fact]
        public async Task ExportFailure_KeepsParquetPathAndRetriesWithSuffix()
        {
            string parquet = CreateTempParquet();
            string gdbPath = Path.Combine(Path.GetTempPath(), $"fail_{Guid.NewGuid():N}.gdb");
            Directory.CreateDirectory(gdbPath);
            var runner = new FakeGeoprocessingRunner
            {
                Handler = (_, _, _) => GeoprocessingToolResult.Failure("ERROR 000210: Cannot create output")
            };
            var exporter = new FileGeodatabaseExporter(runner);
            var layer = CreateLayer(parquet);
            string originalPath = layer.FilePath;
            var settings = new FileGeodatabaseOutputSettings
            {
                Enabled = true,
                GeodatabasePath = gdbPath,
                OverwriteExisting = true
            };

            try
            {
                var result = await exporter.ExportPendingLayersAsync(
                    new List<LayerCreationInfo> { layer },
                    settings,
                    Path.GetTempPath());

                Assert.Equal(0, result.ExportedCount);
                Assert.Equal(1, result.FailedCount);
                Assert.False(layer.IsFileGeodatabase);
                Assert.Equal(originalPath, layer.FilePath);
                Assert.Equal(2, runner.Calls.Count(call => call.Tool == FileGeodatabaseOutput.ExportFeaturesTool));
                Assert.Contains(result.Messages, message => message.Contains("GeoParquet files were kept"));
                string retryPath = (string)runner.Calls.Last(call => call.Tool == FileGeodatabaseOutput.ExportFeaturesTool).Values[1];
                Assert.NotEqual(Path.Combine(gdbPath, "buildings_building_polygons"), retryPath);
                Assert.StartsWith(gdbPath, retryPath);
            }
            finally
            {
                Cleanup(parquet, gdbPath);
            }
        }

        [Fact]
        public async Task CreateGdbFailure_LeavesAllLayersAsParquet()
        {
            string parquet = CreateTempParquet();
            string outputFolder = Path.Combine(Path.GetTempPath(), $"nogdb_{Guid.NewGuid():N}");
            Directory.CreateDirectory(outputFolder);
            var runner = new FakeGeoprocessingRunner
            {
                Handler = (tool, _, _) => tool == FileGeodatabaseOutput.CreateFileGdbTool
                    ? GeoprocessingToolResult.Failure("ERROR 000732: Folder does not exist")
                    : GeoprocessingToolResult.Success()
            };
            var exporter = new FileGeodatabaseExporter(runner);
            var layer = CreateLayer(parquet);
            string originalPath = layer.FilePath;

            try
            {
                var result = await exporter.ExportPendingLayersAsync(
                    new List<LayerCreationInfo> { layer },
                    new FileGeodatabaseOutputSettings { Enabled = true },
                    outputFolder);

                Assert.False(result.GeodatabaseReady);
                Assert.Equal(1, result.FailedCount);
                Assert.Equal(originalPath, layer.FilePath);
                Assert.DoesNotContain(runner.Calls, call => call.Tool == FileGeodatabaseOutput.ExportFeaturesTool);
                Assert.Contains(result.Messages, message => message.Contains("management.CreateFileGDB"));
            }
            finally
            {
                Cleanup(parquet, outputFolder);
            }
        }

        [Fact]
        public async Task OverwriteFalse_IsPassedToExportFeatures()
        {
            string parquet = CreateTempParquet();
            string gdbPath = Path.Combine(Path.GetTempPath(), $"keep_{Guid.NewGuid():N}.gdb");
            Directory.CreateDirectory(gdbPath);
            var runner = new FakeGeoprocessingRunner();
            var exporter = new FileGeodatabaseExporter(runner);
            var settings = new FileGeodatabaseOutputSettings
            {
                Enabled = true,
                GeodatabasePath = gdbPath,
                OverwriteExisting = false
            };

            try
            {
                await exporter.ExportPendingLayersAsync(
                    new List<LayerCreationInfo> { CreateLayer(parquet) },
                    settings,
                    Path.GetTempPath());

                Assert.Contains(runner.Calls, call =>
                    call.Tool == FileGeodatabaseOutput.ExportFeaturesTool &&
                    !call.Overwrite);
            }
            finally
            {
                Cleanup(parquet, gdbPath);
            }
        }

        private static LayerCreationInfo CreateLayer(string parquetPath)
        {
            return new LayerCreationInfo
            {
                FilePath = parquetPath,
                LayerName = "Buildings - building (polygons)",
                GeometryType = "POLYGON",
                ParentTheme = "buildings",
                ActualType = "building"
            };
        }

        private static string CreateTempParquet()
        {
            string path = Path.Combine(Path.GetTempPath(), $"layer_{Guid.NewGuid():N}.parquet");
            File.WriteAllText(path, "parquet");
            return path;
        }

        private static void Cleanup(string parquet, string folder)
        {
            if (File.Exists(parquet))
                File.Delete(parquet);
            if (Directory.Exists(folder))
                Directory.Delete(folder, recursive: true);
        }

        private sealed class FakeGeoprocessingRunner : IGeoprocessingRunner
        {
            public List<(string Tool, object[] Values, bool Overwrite)> Calls { get; } = new();
            public Func<string, IReadOnlyList<object>, bool, GeoprocessingToolResult> Handler { get; set; }

            public Task<GeoprocessingToolResult> ExecuteAsync(
                string toolName,
                IReadOnlyList<object> values,
                bool overwriteOutput,
                CancellationToken cancellationToken = default)
            {
                Calls.Add((toolName, values.ToArray(), overwriteOutput));
                return Task.FromResult(Handler != null
                    ? Handler(toolName, values, overwriteOutput)
                    : GeoprocessingToolResult.Success("ok"));
            }
        }
    }
}
