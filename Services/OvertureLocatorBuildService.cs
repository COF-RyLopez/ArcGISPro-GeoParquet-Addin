using ArcGIS.Core.Data;
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
            string scratchGdbPath = Path.Combine(scratchGdbFolder, "overture_locator_input.gdb");

            if (Directory.Exists(scratchGdbPath) && forceRebuild)
            {
                try
                {
                    Directory.Delete(scratchGdbPath, true);
                }
                catch (Exception ex)
                {
                    // Continuing into a half-cleared, locked geodatabase makes
                    // the export steps fail confusingly later — stop here with
                    // an actionable message instead.
                    System.Diagnostics.Debug.WriteLine($"Could not clear scratch geodatabase: {ex.Message}");
                    return new LocatorBuildResult
                    {
                        Succeeded = false,
                        Message = "The previous locator scratch geodatabase is locked by another process " +
                                  "(usually ArcGIS Pro itself — a locator registered in the project or a layer " +
                                  "referencing it). Remove those references, restart ArcGIS Pro, and rebuild. " +
                                  $"Locked path: {scratchGdbPath} ({ex.Message})"
                    };
                }
            }

            if (!Directory.Exists(scratchGdbPath))
            {
                var createGdbResult = await RunGpToolAsync("management.CreateFileGDB", [scratchGdbFolder, "overture_locator_input.gdb"]);
                if (createGdbResult.IsFailed)
                {
                    return Fail("Failed to create scratch geodatabase.", createGdbResult);
                }
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

            string addressLocatorPath = Path.Combine(locatorRoot, "OvertureAddress.loc");
            string placeLocatorPath = Path.Combine(locatorRoot, "OverturePlace.loc");
            string compositeLocatorPath = Path.Combine(locatorRoot, "OvertureHybrid.loc");

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
                string fallbackAddressField = SelectFieldName(addressFields, "street", "street_name", "road", "name");
                if (string.IsNullOrWhiteSpace(fallbackAddressField))
                {
                    return new LocatorBuildResult
                    {
                        Succeeded = false,
                        Message = "Address locator mapping could not find a usable street/name field."
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
                string fallbackPlaceField = SelectFieldName(placeFields, "name", "name_primary", "brand", "category");
                if (string.IsNullOrWhiteSpace(fallbackPlaceField))
                {
                    return new LocatorBuildResult
                    {
                        Succeeded = false,
                        Message = "Place locator mapping could not find a usable name/category field."
                    };
                }

                var placeLocatorResult = await RunGpToolAsync("geocoding.CreateFeatureLocator", [placeFc, $"*Name {fallbackPlaceField} VISIBLE NONE", placeLocatorPath]);
                if (placeLocatorResult.IsFailed)
                {
                    return Fail("Failed to create place locator after role mapping fallback.", placeLocatorResult);
                }

                placeBuilt = true;
            }

            var compositeResult = await RunGpToolAsync(
                "geocoding.CreateCompositeAddressLocator",
                [$"{addressLocatorPath} Address;{placeLocatorPath} Place", compositeLocatorPath]);

            if (compositeResult.IsFailed)
            {
                return Fail("Failed to create composite locator from address/place locators.", compositeResult);
            }

            var metadata = new LocatorBuildMetadata
            {
                ReleaseFolderName = latestReleaseDir.Name,
                LocatorPath = compositeLocatorPath,
                CompositeLocatorPath = compositeLocatorPath,
                LastBuiltUtc = DateTime.UtcNow
            };

            string metadataPath = GetMetadataPath();
            File.WriteAllText(metadataPath, JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true }));

            // Register the composite locator with the project so it becomes a
            // locator provider — LocatorManager.GeocodeAsync only searches
            // registered providers.
            string registrationNote = string.Empty;
            try
            {
                bool added = await QueuedTask.Run(() =>
                {
                    var locatorItem = ItemFactory.Instance.Create(compositeLocatorPath) as IProjectItem;
                    return locatorItem != null && Project.Current.AddItem(locatorItem);
                });
                registrationNote = added ? " Locator registered with the project." : string.Empty;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Could not register locator with project: {ex.Message}");
            }

            return new LocatorBuildResult
            {
                Succeeded = true,
                LocatorPath = compositeLocatorPath,
                Message = $"Hybrid locator built: {Path.GetFileName(compositeLocatorPath)}{registrationNote}" +
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
