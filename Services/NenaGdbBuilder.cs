using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// What a NENA GeoPackage's export report says the file geodatabase needs: the layers to load, and the fields
    /// that must be allowed empty because a source left them empty (agency-owned fields such as Agency_ID, and
    /// NENA-required fields a source did not fill). Nothing is filled in.
    /// </summary>
    public sealed class NenaGdbPlan
    {
        public IReadOnlyList<string> Layers { get; init; } = Array.Empty<string>();
        public IReadOnlyList<(string Layer, string Field)> NullableFields { get; init; } = Array.Empty<(string, string)>();
        /// <summary>Layers the export did not write because a value was too long for NENA's field.</summary>
        public IReadOnlyList<string> SkippedLayers { get; init; } = Array.Empty<string>();

        /// <summary>
        /// Reads the report open-ng911-addresses writes next to the GeoPackage (<c>&lt;gpkg&gt;.report.json</c>):
        /// <c>layers</c> (feature counts), <c>agency_owed</c> and <c>problems</c> (<c>missing_required</c>,
        /// <c>too_long</c>).
        /// </summary>
        public static NenaGdbPlan FromReport(string json)
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var nullable = new List<(string, string)>();
            var skipped = new HashSet<string>(StringComparer.Ordinal);

            if (root.TryGetProperty("agency_owed", out var owed) && owed.ValueKind == JsonValueKind.Object)
                foreach (var layer in owed.EnumerateObject())
                    foreach (var field in layer.Value.EnumerateObject())
                        nullable.Add((layer.Name, field.Name));

            if (root.TryGetProperty("problems", out var problems) && problems.ValueKind == JsonValueKind.Object)
                foreach (var layer in problems.EnumerateObject())
                    foreach (var field in layer.Value.EnumerateObject())
                    {
                        if (Count(field.Value, "missing_required") > 0)
                            nullable.Add((layer.Name, field.Name));
                        if (Count(field.Value, "too_long") > 0)
                            skipped.Add(layer.Name);
                    }

            var layers = new List<string>();
            if (root.TryGetProperty("layers", out var counts) && counts.ValueKind == JsonValueKind.Object)
                foreach (var layer in counts.EnumerateObject())
                    if (layer.Value.ValueKind == JsonValueKind.Number && layer.Value.GetInt64() > 0 && !skipped.Contains(layer.Name))
                        layers.Add(layer.Name);

            return new NenaGdbPlan
            {
                Layers = layers,
                NullableFields = nullable.Distinct().Where(f => layers.Contains(f.Item1)).ToList(),
                SkippedLayers = skipped.OrderBy(s => s, StringComparer.Ordinal).ToList(),
            };
        }

        private static long Count(JsonElement e, string name)
            => e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number
                ? v.GetInt64() : 0;
    }

    public sealed class NenaGdbResult
    {
        public bool Succeeded { get; init; }
        public string GeodatabasePath { get; init; }
        public int LoadedLayers { get; init; }
        public IReadOnlyList<string> Messages { get; init; } = Array.Empty<string>();
    }

    /// <summary>
    /// Builds a NENA NG9-1-1 file geodatabase from a statewide NENA GeoPackage (open-ng911-addresses' export):
    /// copies NENA's own v3.0 template (bundled; Apache-2.0), makes the fields the report flags nullable with
    /// Alter Field (the template is still empty, which Alter Field needs), then appends each layer. Layer and field
    /// names already match the template, so Append runs without a field map. ArcGIS stays behind
    /// <see cref="IGeoprocessingRunner"/> so the flow can be unit-tested.
    /// </summary>
    public sealed class NenaGdbBuilder
    {
        public const string TemplateZipName = "NG911_GISDataModelTemplate_v3.0.gdb.zip";
        public const string AlterFieldTool = "management.AlterField";
        public const string AppendTool = "management.Append";

        private readonly IGeoprocessingRunner _geoprocessing;

        public NenaGdbBuilder(IGeoprocessingRunner geoprocessing)
        {
            _geoprocessing = geoprocessing ?? throw new ArgumentNullException(nameof(geoprocessing));
        }

        /// <summary>The bundled template inside the add-in's install folder.</summary>
        public static string BundledTemplatePath(string addInFolder) => Path.Combine(addInFolder, "Nena", TemplateZipName);

        /// <summary>The report the export writes next to the GeoPackage.</summary>
        public static string ReportPath(string geoPackagePath) => geoPackagePath + ".report.json";

        /// <summary>Where the geodatabase goes by default: beside the GeoPackage, same name.</summary>
        public static string DefaultGeodatabasePath(string geoPackagePath)
            => Path.Combine(Path.GetDirectoryName(Path.GetFullPath(geoPackagePath)) ?? ".",
                Path.GetFileNameWithoutExtension(geoPackagePath) + ".gdb");

        /// <summary>A GeoPackage table as geoprocessing tools name it.</summary>
        public static string GeoPackageTable(string geoPackagePath, string layer) => Path.Combine(geoPackagePath, "main." + layer);

        public async Task<NenaGdbResult> BuildAsync(
            string geoPackagePath,
            string geodatabasePath,
            string templateZipPath,
            bool overwriteExisting,
            IProgress<string> progress = null,
            CancellationToken cancellationToken = default)
        {
            var messages = new List<string>();
            void Say(string m) { messages.Add(m); progress?.Report(m); }
            NenaGdbResult Fail(string m, int loaded = 0)
            {
                Say(m);
                return new NenaGdbResult { Succeeded = false, GeodatabasePath = geodatabasePath, LoadedLayers = loaded, Messages = messages };
            }

            if (!File.Exists(geoPackagePath))
                return Fail($"GeoPackage not found: {geoPackagePath}");
            string reportPath = ReportPath(geoPackagePath);
            if (!File.Exists(reportPath))
                return Fail($"Export report not found: {reportPath}. It is written next to the GeoPackage by the export and says which fields may be empty.");
            if (!File.Exists(templateZipPath))
                return Fail($"NENA template not found: {templateZipPath}");

            NenaGdbPlan plan;
            try
            {
                plan = NenaGdbPlan.FromReport(await File.ReadAllTextAsync(reportPath, cancellationToken));
            }
            catch (JsonException ex)
            {
                return Fail($"Could not read the export report {reportPath}: {ex.Message}");
            }
            foreach (string layer in plan.SkippedLayers)
                Say($"{layer} was not written to the GeoPackage (a value is too long for NENA's field; see the report), so it stays empty here.");

            try
            {
                CopyTemplate(templateZipPath, geodatabasePath, overwriteExisting);
            }
            catch (Exception ex) when (ex is IOException || ex is InvalidDataException || ex is UnauthorizedAccessException)
            {
                return Fail($"Could not copy NENA's template to {geodatabasePath}: {ex.Message}");
            }
            Say($"Copied NENA's v3.0 template to {geodatabasePath}.");

            foreach (var (layer, field) in plan.NullableFields)
            {
                cancellationToken.ThrowIfCancellationRequested();
                // in_table, field, new_field_name, new_field_alias, field_type, field_length, field_is_nullable
                var r = await _geoprocessing.ExecuteAsync(AlterFieldTool,
                    new object[] { Path.Combine(geodatabasePath, layer), field, "", "", "", "", "NULLABLE" },
                    overwriteOutput: false, cancellationToken);
                if (!r.Succeeded)
                    return Fail($"Alter Field could not make {layer}.{field} nullable: {r.Messages}");
                Say($"{layer}.{field} may be empty: the source left it empty (listed in the report).");
            }

            int loaded = 0;
            foreach (string layer in plan.Layers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var r = await _geoprocessing.ExecuteAsync(AppendTool,
                    new object[] { GeoPackageTable(geoPackagePath, layer), Path.Combine(geodatabasePath, layer), "NO_TEST" },
                    overwriteOutput: false, cancellationToken);
                if (!r.Succeeded)
                    return Fail($"Append into {layer} failed: {r.Messages}", loaded);
                loaded++;
                Say($"Loaded {layer}.");
            }

            Say($"NENA file geodatabase ready: {loaded} layer(s) in {geodatabasePath}.");
            return new NenaGdbResult { Succeeded = true, GeodatabasePath = geodatabasePath, LoadedLayers = loaded, Messages = messages };
        }

        /// <summary>Unzips the template's .gdb folder to <paramref name="geodatabasePath"/>.</summary>
        public static void CopyTemplate(string templateZipPath, string geodatabasePath, bool overwriteExisting)
        {
            if (Directory.Exists(geodatabasePath))
            {
                if (!overwriteExisting)
                    throw new IOException($"{geodatabasePath} already exists.");
                Directory.Delete(geodatabasePath, recursive: true);
            }

            string staging = Path.Combine(Path.GetTempPath(), "nena_template_" + Guid.NewGuid().ToString("N"));
            try
            {
                ZipFile.ExtractToDirectory(templateZipPath, staging);
                string gdb = Directory.GetDirectories(staging, "*.gdb", SearchOption.AllDirectories).FirstOrDefault()
                    ?? throw new InvalidDataException($"{templateZipPath} holds no .gdb folder.");
                Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(geodatabasePath)) ?? ".");
                Directory.CreateDirectory(geodatabasePath);
                foreach (string file in Directory.GetFiles(gdb))
                    File.Copy(file, Path.Combine(geodatabasePath, Path.GetFileName(file)));
            }
            finally
            {
                if (Directory.Exists(staging))
                    Directory.Delete(staging, recursive: true);
            }
        }
    }
}
