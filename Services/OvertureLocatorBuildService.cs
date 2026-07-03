using ArcGIS.Core.Data;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDBGeoparquet.Models;
using static DuckDBGeoparquet.Models.AddinConstants;

namespace DuckDBGeoparquet.Services
{
    public class LocatorBuildMetadata
    {
        public string ReleaseFolderName { get; set; }
        public string LocatorPath { get; set; }
        public string CompositeLocatorPath { get; set; }
        public DateTime LastBuiltUtc { get; set; }
    }

    public class LocatorBuildResult
    {
        public bool Succeeded { get; set; }
        public string Message { get; set; }
        public string LocatorPath { get; set; }
    }

    public class OvertureLocatorBuildService
    {
        // Data subfolder constant is now in AddinConstants.DataSubfolder
        private const string MetadataFileName = "overture_locator_metadata.json";

        public LocatorBuildMetadata ReadBuildMetadata()
        {
            string metadataPath = GetMetadataPath();
            if (!File.Exists(metadataPath))
            {
                return null;
            }

            try
            {
                string json = File.ReadAllText(metadataPath);
                return JsonSerializer.Deserialize<LocatorBuildMetadata>(json);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to read locator metadata: {ex.Message}");
                return null;
            }
        }

        public bool IsLocatorReady()
        {
            var metadata = ReadBuildMetadata();
            if (metadata == null || string.IsNullOrWhiteSpace(metadata.LocatorPath))
            {
                return false;
            }

            return File.Exists(metadata.LocatorPath) || Directory.Exists(metadata.LocatorPath);
        }

        public async Task<LocatorBuildResult> BuildOrRebuildLocatorAsync(bool forceRebuild)
        {
            var latestReleaseDir = ResolveLatestReleaseDirectory();
            if (latestReleaseDir == null)
            {
                return new LocatorBuildResult
                {
                    Succeeded = false,
                    Message = "No local Overture release data was found. Load address/place data first."
                };
            }

            string addressDir = Path.Combine(latestReleaseDir.FullName, "address");
            string placeDir = Path.Combine(latestReleaseDir.FullName, "place");
            if (!Directory.Exists(addressDir) || !Directory.Exists(placeDir))
            {
                return new LocatorBuildResult
                {
                    Succeeded = false,
                    Message = "Both address and place datasets are required to build the hybrid locator."
                };
            }

            // Load cleanups skip locked files, so these folders can hold
            // several session-stamped parquet files — build from the newest.
            static string NewestParquet(string dir) => Directory
                .EnumerateFiles(dir, "*.parquet")
                .OrderByDescending(File.GetLastWriteTimeUtc)
                .FirstOrDefault();

            string addressParquet = NewestParquet(addressDir);
            string placeParquet = NewestParquet(placeDir);
            if (string.IsNullOrWhiteSpace(addressParquet) || string.IsNullOrWhiteSpace(placeParquet))
            {
                return new LocatorBuildResult
                {
                    Succeeded = false,
                    Message = "Address/place parquet files are missing. Load both themes before building."
                };
            }

            string locatorRoot = ResolveLocatorRootPath();
            Directory.CreateDirectory(locatorRoot);
            string scratchGdbFolder = Path.Combine(locatorRoot, "scratch");
            Directory.CreateDirectory(scratchGdbFolder);

            // A feature locator holds a live reference to its input feature
            // classes, so once registered it locks the scratch geodatabase and
            // its own .loc files — the previous build's outputs can never be
            // deleted or overwritten. Unregister the old Overture locators to
            // release those locks, then use fresh timestamped names so this
            // build can never be blocked by a still-locked predecessor.
            await UnregisterProjectLocatorsAsync(locatorRoot);
            CleanupOldLocatorArtifacts(locatorRoot, scratchGdbFolder);

            string buildStamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
            string scratchGdbName = $"locator_input_{buildStamp}.gdb";
            string scratchGdbPath = Path.Combine(scratchGdbFolder, scratchGdbName);

            var createGdbResult = await RunGpToolAsync("management.CreateFileGDB", [scratchGdbFolder, scratchGdbName]);
            if (createGdbResult.IsFailed)
            {
                return Fail("Failed to create scratch geodatabase.", createGdbResult);
            }

            string addressFc = Path.Combine(scratchGdbPath, "address_points");
            string placeFc = Path.Combine(scratchGdbPath, "place_points");

            var exportAddressResult = await RunGpToolAsync("conversion.ExportFeatures", [addressParquet, addressFc]);
            if (exportAddressResult.IsFailed)
            {
                return Fail("Failed to export address parquet to feature class.", exportAddressResult);
            }

            var exportPlaceResult = await RunGpToolAsync("conversion.ExportFeatures", [placeParquet, placeFc]);
            if (exportPlaceResult.IsFailed)
            {
                return Fail("Failed to export place parquet to feature class.", exportPlaceResult);
            }

            string addressLocatorPath = Path.Combine(locatorRoot, $"OvertureAddress_{buildStamp}.loc");
            string placeLocatorPath = Path.Combine(locatorRoot, $"OverturePlace_{buildStamp}.loc");
            string compositeLocatorPath = Path.Combine(locatorRoot, $"OvertureHybrid_{buildStamp}.loc");

            var addressFields = await GetFeatureClassFieldNamesAsync(addressFc);
            var placeFields = await GetFeatureClassFieldNamesAsync(placeFc);

            // Attempt explicit role-based field mapping first.
            var addressRoleMap = BuildAddressRoleFieldMap(addressFields);
            var poiRoleMap = BuildPoiRoleFieldMap(placeFields);

            bool addressBuilt = false;
            bool placeBuilt = false;
            string addressBuildNotes = string.Empty;
            string placeBuildNotes = string.Empty;

            if (!string.IsNullOrWhiteSpace(addressRoleMap))
            {
                string addressPrimaryRef = $"PointAddress '{addressFc}'";
                var addressCreateLocatorResult = await RunGpToolAsync(
                    "geocoding.CreateLocator",
                    ["", addressPrimaryRef, addressRoleMap, addressLocatorPath]);
                if (!addressCreateLocatorResult.IsFailed)
                {
                    addressBuilt = true;
                }
                else
                {
                    addressBuildNotes = $"Address role mapping failed; falling back to feature locator. ({GpMessages(addressCreateLocatorResult)})";
                }
            }

            if (!addressBuilt)
            {
                string fallbackAddressField = SelectFieldName(addressFields,
                    "street", "street_name", "road", "name", "names_primary", "full_address", "address");
                if (string.IsNullOrWhiteSpace(fallbackAddressField))
                {
                    return new LocatorBuildResult
                    {
                        Succeeded = false,
                        Message = "Address locator mapping could not find a usable street/name field. " +
                                  $"Available fields: {string.Join(", ", addressFields.OrderBy(f => f))}"
                    };
                }

                // The search-fields parameter is a mapping table entry in the
                // form "*Name <inputField> VISIBLE NONE" (per Esri's tool
                // examples); anything else fails with ERROR 003057.
                var addressLocatorResult = await RunGpToolAsync("geocoding.CreateFeatureLocator", [addressFc, $"*Name {fallbackAddressField} VISIBLE NONE", addressLocatorPath]);
                if (addressLocatorResult.IsFailed)
                {
                    return Fail("Failed to create address locator after role mapping fallback.", addressLocatorResult);
                }

                addressBuilt = true;
            }

            if (!string.IsNullOrWhiteSpace(poiRoleMap))
            {
                string poiPrimaryRef = $"POI '{placeFc}'";
                var placeCreateLocatorResult = await RunGpToolAsync(
                    "geocoding.CreateLocator",
                    ["", poiPrimaryRef, poiRoleMap, placeLocatorPath]);
                if (!placeCreateLocatorResult.IsFailed)
                {
                    placeBuilt = true;
                }
                else
                {
                    placeBuildNotes = $"POI role mapping failed; falling back to feature locator. ({GpMessages(placeCreateLocatorResult)})";
                }
            }

            if (!placeBuilt)
            {
                // Overture 'names'/'categories' are nested structs that the
                // parquet→GDB conversion flattens (e.g. names_primary).
                string fallbackPlaceField = SelectFieldName(placeFields,
                    "name", "name_primary", "names_primary", "primary_name", "names",
                    "brand", "category", "categories_primary", "category_primary", "categories");
                if (string.IsNullOrWhiteSpace(fallbackPlaceField))
                {
                    return new LocatorBuildResult
                    {
                        Succeeded = false,
                        Message = "Place locator mapping could not find a usable name/category field. " +
                                  $"Available fields: {string.Join(", ", placeFields.OrderBy(f => f))}"
                    };
                }

                var placeLocatorResult = await RunGpToolAsync("geocoding.CreateFeatureLocator", [placeFc, $"*Name {fallbackPlaceField} VISIBLE NONE", placeLocatorPath]);
                if (placeLocatorResult.IsFailed)
                {
                    return Fail("Failed to create place locator after role mapping fallback.", placeLocatorResult);
                }

                placeBuilt = true;
            }

            // Paths contain spaces (e.g. 'OneDrive - County of Fresno') and
            // must be single-quoted or the value-table parser splits them.
            // The tool's positional parameters are (locators, field_map, output).
            var compositeResult = await RunGpToolAsync(
                "geocoding.CreateCompositeAddressLocator",
                [$"'{addressLocatorPath}' Address;'{placeLocatorPath}' Place", "", compositeLocatorPath]);

            bool compositeBuilt = !compositeResult.IsFailed;
            string compositeNote = compositeBuilt
                ? string.Empty
                : $" Composite creation failed ({GpMessages(compositeResult)}); the address and place locators are registered individually instead.";

            var metadata = new LocatorBuildMetadata
            {
                ReleaseFolderName = latestReleaseDir.Name,
                LocatorPath = compositeBuilt ? compositeLocatorPath : addressLocatorPath,
                CompositeLocatorPath = compositeBuilt ? compositeLocatorPath : null,
                LastBuiltUtc = DateTime.UtcNow
            };

            string metadataPath = GetMetadataPath();
            File.WriteAllText(metadataPath, JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true }));

            // Register the locator(s) with the project so they become locator
            // providers — LocatorManager.GeocodeAsync only searches
            // registered providers. If the composite exists, one registration
            // covers both; otherwise register address and place separately.
            string registrationNote = string.Empty;
            try
            {
                string[] locatorsToRegister = compositeBuilt
                    ? [compositeLocatorPath]
                    : [addressLocatorPath, placeLocatorPath];
                int registered = await QueuedTask.Run(() =>
                {
                    int count = 0;
                    foreach (var path in locatorsToRegister)
                    {
                        var locatorItem = ItemFactory.Instance.Create(path) as IProjectItem;
                        if (locatorItem != null && Project.Current.AddItem(locatorItem))
                        {
                            count++;
                        }
                    }
                    return count;
                });
                registrationNote = registered > 0 ? $" {registered} locator(s) registered with the project." : string.Empty;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Could not register locator with project: {ex.Message}");
            }

            return new LocatorBuildResult
            {
                Succeeded = true,
                LocatorPath = metadata.LocatorPath,
                Message = $"Hybrid locator built: {Path.GetFileName(metadata.LocatorPath)}{registrationNote}{compositeNote}" +
                    (string.IsNullOrWhiteSpace(addressBuildNotes) ? string.Empty : $" {addressBuildNotes}") +
                    (string.IsNullOrWhiteSpace(placeBuildNotes) ? string.Empty : $" {placeBuildNotes}")
            };
        }

        private static string GpMessages(IGPResult result) =>
            result == null ? string.Empty : string.Join(" | ", result.Messages.Select(m => m.Text));

        private static LocatorBuildResult Fail(string message, IGPResult result)
        {
            string gpMessages = result == null ? string.Empty : string.Join(Environment.NewLine, result.Messages.Select(m => m.Text));
            return new LocatorBuildResult
            {
                Succeeded = false,
                Message = $"{message}{(string.IsNullOrWhiteSpace(gpMessages) ? string.Empty : $"{Environment.NewLine}{gpMessages}")}"
            };
        }

        /// <summary>
        /// Unregisters this add-in's previously built locators (any locator
        /// project item under <paramref name="locatorRoot"/>) so the files
        /// they reference are released and can be cleaned up.
        /// </summary>
        private static async Task UnregisterProjectLocatorsAsync(string locatorRoot)
        {
            try
            {
                string rootFull = Path.GetFullPath(locatorRoot);
                await QueuedTask.Run(() =>
                {
                    foreach (var item in Project.Current.GetItems<LocatorsConnectionProjectItem>().ToList())
                    {
                        if (string.IsNullOrWhiteSpace(item.Path))
                        {
                            continue;
                        }
                        if (Path.GetFullPath(item.Path).StartsWith(rootFull, StringComparison.OrdinalIgnoreCase))
                        {
                            Project.Current.RemoveItem(item as IProjectItem);
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Unregister project locators skipped: {ex.Message}");
            }
        }

        /// <summary>
        /// Best-effort removal of prior builds' locator files and scratch
        /// geodatabases. Anything still locked (e.g. a locator Pro hasn't
        /// released yet) is left for a future run; the current build uses
        /// fresh timestamped names, so leftovers never block it.
        /// </summary>
        private static void CleanupOldLocatorArtifacts(string locatorRoot, string scratchGdbFolder)
        {
            try
            {
                foreach (var loc in Directory.EnumerateFiles(locatorRoot, "Overture*.loc"))
                {
                    TryDeleteFile(loc);
                    TryDeleteFile(loc + ".xml");
                    TryDeleteFile(Path.ChangeExtension(loc, ".loz"));
                }
                foreach (var gdb in Directory.EnumerateDirectories(scratchGdbFolder, "*.gdb"))
                {
                    try { Directory.Delete(gdb, true); }
                    catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"Leaving locked scratch gdb {gdb}: {ex.Message}"); }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Old locator artifact cleanup skipped: {ex.Message}");
            }
        }

        private static void TryDeleteFile(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); }
            catch (Exception ex) { System.Diagnostics.Debug.WriteLine($"Leaving locked file {path}: {ex.Message}"); }
        }

        private static async Task<IGPResult> RunGpToolAsync(string toolName, IEnumerable<object> values)
        {
            return await QueuedTask.Run(async () =>
            {
                var parameters = Geoprocessing.MakeValueArray(values.ToArray());
                var environment = Geoprocessing.MakeEnvironmentArray(overwriteoutput: true);
                // GPExecuteToolFlags.Default adds tool outputs to the active
                // map — the staged scratch feature classes would appear as
                // layers AND hold locks on the scratch geodatabase, breaking
                // every subsequent rebuild.
                return await Geoprocessing.ExecuteToolAsync(
                    toolName,
                    parameters,
                    environment,
                    flags: GPExecuteToolFlags.None);
            });
        }

        private static async Task<HashSet<string>> GetFeatureClassFieldNamesAsync(string featureClassPath)
        {
            return await QueuedTask.Run(() =>
            {
                var fields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                string gdbPath = Path.GetDirectoryName(featureClassPath);
                string featureClassName = Path.GetFileName(featureClassPath);
                var gdbConnectionPath = new FileGeodatabaseConnectionPath(new Uri(gdbPath));
                using var gdb = new Geodatabase(gdbConnectionPath);
                using var fc = gdb.OpenDataset<FeatureClass>(featureClassName);
                foreach (var field in fc.GetDefinition().GetFields())
                {
                    fields.Add(field.Name);
                }

                return fields;
            });
        }

        private static string BuildAddressRoleFieldMap(HashSet<string> fields)
        {
            var roleFieldMap = new Dictionary<string, string>
            {
                ["HouseNumber"] = SelectFieldName(fields, "number", "housenumber", "house_number"),
                ["StreetName"] = SelectFieldName(fields, "street", "street_name", "road", "name"),
                ["City"] = SelectFieldName(fields, "city", "locality", "district"),
                ["Region"] = SelectFieldName(fields, "region", "state", "province", "admin1"),
                ["Postal"] = SelectFieldName(fields, "postcode", "postal_code", "zip"),
                ["CountryCode"] = SelectFieldName(fields, "country_code", "country")
            };

            return BuildRoleFieldMap("PointAddress", roleFieldMap);
        }

        private static string BuildPoiRoleFieldMap(HashSet<string> fields)
        {
            var roleFieldMap = new Dictionary<string, string>
            {
                ["POIName"] = SelectFieldName(fields, "name", "name_primary", "brand"),
                ["Category"] = SelectFieldName(fields, "category", "class", "kind", "subtype"),
                ["City"] = SelectFieldName(fields, "city", "locality", "district"),
                ["Region"] = SelectFieldName(fields, "region", "state", "province", "admin1"),
                ["Postal"] = SelectFieldName(fields, "postcode", "postal_code", "zip"),
                ["CountryCode"] = SelectFieldName(fields, "country_code", "country")
            };

            return BuildRoleFieldMap("POI", roleFieldMap);
        }

        private static string BuildRoleFieldMap(string roleName, IDictionary<string, string> roleFieldMap)
        {
            var entries = roleFieldMap
                .Where(kvp => !string.IsNullOrWhiteSpace(kvp.Value))
                .Select(kvp => $"{roleName}.{kvp.Key} {kvp.Value} VISIBLE NONE")
                .ToList();

            return entries.Count == 0 ? string.Empty : string.Join(";", entries);
        }

        private static string SelectFieldName(HashSet<string> fields, params string[] candidates)
        {
            foreach (var candidate in candidates)
            {
                if (fields.Contains(candidate))
                {
                    return candidate;
                }
            }

            return null;
        }

        private static DirectoryInfo ResolveLatestReleaseDirectory()
        {
            string dataRoot = ResolveDataRootPath();
            if (string.IsNullOrWhiteSpace(dataRoot) || !Directory.Exists(dataRoot))
            {
                return null;
            }

            return new DirectoryInfo(dataRoot)
                .GetDirectories()
                .OrderByDescending(d => d.LastWriteTimeUtc)
                .FirstOrDefault();
        }

        private static string ResolveDataRootPath()
        {
            var project = Project.Current;
            if (project != null && !string.IsNullOrWhiteSpace(project.HomeFolderPath))
            {
                return Path.Combine(project.HomeFolderPath, DataSubfolder, "Data");
            }

            if (project != null && !string.IsNullOrWhiteSpace(project.Path))
            {
                string projectDir = Path.GetDirectoryName(project.Path);
                if (!string.IsNullOrWhiteSpace(projectDir))
                {
                    return Path.Combine(projectDir, DataSubfolder, "Data");
                }
            }

            return null;
        }

        private static string ResolveLocatorRootPath()
        {
            var project = Project.Current;
            if (project != null && !string.IsNullOrWhiteSpace(project.HomeFolderPath))
            {
                return Path.Combine(project.HomeFolderPath, DataSubfolder, "Locators");
            }

            if (project != null && !string.IsNullOrWhiteSpace(project.Path))
            {
                string projectDir = Path.GetDirectoryName(project.Path);
                if (!string.IsNullOrWhiteSpace(projectDir))
                {
                    return Path.Combine(projectDir, DataSubfolder, "Locators");
                }
            }

            return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), DataSubfolder, "Locators");
        }

        private static string GetMetadataPath()
        {
            string locatorRoot = ResolveLocatorRootPath();
            Directory.CreateDirectory(locatorRoot);
            return Path.Combine(locatorRoot, MetadataFileName);
        }
    }
}
