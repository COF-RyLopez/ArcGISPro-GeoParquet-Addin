using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDBGeoparquet.Models;

namespace DuckDBGeoparquet.Services
{
    public sealed class FileGeodatabaseExportResult
    {
        public bool Attempted { get; init; }
        public bool GeodatabaseReady { get; init; }
        public int ExportedCount { get; init; }
        public int FailedCount { get; init; }
        public string GeodatabasePath { get; init; }
        public IReadOnlyList<string> Messages { get; init; } = Array.Empty<string>();
    }

    /// <summary>
    /// After local GeoParquet files are written, copies each file into a file
    /// geodatabase with Pro's Export Features tool. ArcGIS types stay behind
    /// <see cref="IGeoprocessingRunner"/> so the flow can be unit-tested.
    /// Failed exports leave the GeoParquet files and pending parquet layers in place.
    /// </summary>
    public sealed class FileGeodatabaseExporter
    {
        private readonly IGeoprocessingRunner _geoprocessing;

        public FileGeodatabaseExporter(IGeoprocessingRunner geoprocessing)
        {
            _geoprocessing = geoprocessing ?? throw new ArgumentNullException(nameof(geoprocessing));
        }

        public async Task<FileGeodatabaseExportResult> ExportPendingLayersAsync(
            IList<LayerCreationInfo> pendingLayers,
            FileGeodatabaseOutputSettings settings,
            string dataOutputFolder,
            IProgress<string> progress = null,
            CancellationToken cancellationToken = default)
        {
            if (!FileGeodatabaseOutput.IsEnabled(settings))
            {
                return new FileGeodatabaseExportResult { Attempted = false };
            }

            if (pendingLayers == null || pendingLayers.Count == 0)
            {
                progress?.Report("No GeoParquet layers to export to a file geodatabase.");
                return new FileGeodatabaseExportResult
                {
                    Attempted = true,
                    Messages = new[] { "No GeoParquet layers to export to a file geodatabase." }
                };
            }

            var messages = new List<string>();
            string gdbPath;
            try
            {
                gdbPath = FileGeodatabaseOutput.ResolveGeodatabasePath(dataOutputFolder, settings.GeodatabasePath);
            }
            catch (Exception ex)
            {
                string failure = FileGeodatabaseOutput.FormatToolFailure(
                    FileGeodatabaseOutput.CreateFileGdbTool,
                    "Resolving file geodatabase path",
                    GeoprocessingToolResult.Failure(ex.Message, ex));
                progress?.Report(failure);
                messages.Add(failure);
                return new FileGeodatabaseExportResult
                {
                    Attempted = true,
                    FailedCount = pendingLayers.Count,
                    Messages = messages
                };
            }

            progress?.Report($"Preparing file geodatabase: {gdbPath}");
            bool gdbReady = await EnsureGeodatabaseAsync(gdbPath, settings.OverwriteExisting, progress, messages, cancellationToken);
            if (!gdbReady)
            {
                return new FileGeodatabaseExportResult
                {
                    Attempted = true,
                    GeodatabasePath = gdbPath,
                    FailedCount = pendingLayers.Count,
                    Messages = messages
                };
            }

            var reservedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int exported = 0;
            int failed = 0;

            foreach (var layer in pendingLayers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (layer == null)
                    continue;

                if (layer.IsFileGeodatabase)
                {
                    reservedNames.Add(Path.GetFileName(layer.FilePath));
                    exported++;
                    continue;
                }

                if (!File.Exists(layer.FilePath))
                {
                    string missing = $"Skipping file geodatabase export for {layer.LayerName}: GeoParquet file not found at {layer.FilePath}. GeoParquet files were kept.";
                    progress?.Report(missing);
                    messages.Add(missing);
                    failed++;
                    continue;
                }

                string proposed = FileGeodatabaseOutput.BuildFeatureClassName(
                    layer.ParentTheme, layer.ActualType, layer.GeometryType);
                bool exportedLayer = await ExportLayerAsync(
                    layer,
                    gdbPath,
                    proposed,
                    reservedNames,
                    settings.OverwriteExisting,
                    progress,
                    messages,
                    cancellationToken);

                if (exportedLayer)
                    exported++;
                else
                    failed++;
            }

            string summary = failed == 0
                ? $"Exported {exported} feature class(es) to {gdbPath}."
                : $"File geodatabase export finished with {exported} succeeded, {failed} failed. GeoParquet files were kept for failed layers.";
            progress?.Report(summary);
            messages.Add(summary);

            return new FileGeodatabaseExportResult
            {
                Attempted = true,
                GeodatabaseReady = true,
                ExportedCount = exported,
                FailedCount = failed,
                GeodatabasePath = gdbPath,
                Messages = messages
            };
        }

        private async Task<bool> EnsureGeodatabaseAsync(
            string gdbPath,
            bool overwriteOutput,
            IProgress<string> progress,
            List<string> messages,
            CancellationToken cancellationToken)
        {
            if (Directory.Exists(gdbPath))
                return true;

            FileGeodatabaseOutput.SplitGeodatabasePath(gdbPath, out string parentFolder, out string gdbName);
            Directory.CreateDirectory(parentFolder);

            progress?.Report($"Creating file geodatabase {gdbName}...");
            var createResult = await _geoprocessing.ExecuteAsync(
                FileGeodatabaseOutput.CreateFileGdbTool,
                new object[] { parentFolder, gdbName },
                overwriteOutput,
                cancellationToken);

            if (createResult.Succeeded && Directory.Exists(gdbPath))
                return true;

            // CreateFileGDB can report success before the folder is visible, or
            // the gdb may already exist from a previous run.
            if (Directory.Exists(gdbPath))
                return true;

            if (createResult.Succeeded)
                return true;

            string failure = FileGeodatabaseOutput.FormatToolFailure(
                FileGeodatabaseOutput.CreateFileGdbTool,
                $"Creating file geodatabase {gdbName}",
                createResult);
            progress?.Report(failure);
            messages.Add(failure);
            return false;
        }

        private async Task<bool> ExportLayerAsync(
            LayerCreationInfo layer,
            string gdbPath,
            string proposedName,
            ISet<string> reservedNames,
            bool overwriteExisting,
            IProgress<string> progress,
            List<string> messages,
            CancellationToken cancellationToken)
        {
            string firstName = FileGeodatabaseOutput.AllocateFeatureClassName(proposedName, reservedNames);
            var first = await TryExportAsync(layer, gdbPath, firstName, overwriteExisting, progress, cancellationToken);
            if (first.Succeeded)
                return true;

            string retryName = FileGeodatabaseOutput.AllocateFeatureClassName(
                proposedName,
                reservedNames,
                forceSuffix: true,
                conflictSuffix: DateTime.UtcNow.ToString("yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture));

            progress?.Report($"Retrying {layer.LayerName} as {retryName} after Export Features could not write '{firstName}'...");
            var retry = await TryExportAsync(layer, gdbPath, retryName, overwriteExisting, progress, cancellationToken);
            if (retry.Succeeded)
                return true;

            var combined = GeoprocessingToolResult.Failure(
                string.Join(" | ", new[]
                {
                    $"Could not write '{firstName}' or suffixed name '{retryName}' into {gdbPath}.",
                    first.Messages,
                    retry.Messages
                }.Where(part => !string.IsNullOrWhiteSpace(part))),
                retry.Exception ?? first.Exception);

            string failure = FileGeodatabaseOutput.FormatToolFailure(
                FileGeodatabaseOutput.ExportFeaturesTool,
                $"Export Features for {layer.LayerName}",
                combined);
            progress?.Report(failure);
            messages.Add(failure);
            return false;
        }

        private async Task<GeoprocessingToolResult> TryExportAsync(
            LayerCreationInfo layer,
            string gdbPath,
            string featureClassName,
            bool overwriteExisting,
            IProgress<string> progress,
            CancellationToken cancellationToken)
        {
            string featureClassPath = Path.Combine(gdbPath, featureClassName);
            progress?.Report($"Exporting {layer.LayerName} to {featureClassName}...");

            var result = await _geoprocessing.ExecuteAsync(
                FileGeodatabaseOutput.ExportFeaturesTool,
                new object[] { layer.FilePath, featureClassPath },
                overwriteExisting,
                cancellationToken);

            if (!result.Succeeded)
                return result;

            layer.FilePath = featureClassPath;
            layer.IsFileGeodatabase = true;
            progress?.Report($"Exported {layer.LayerName} to {featureClassName}.");
            return result;
        }
    }
}
