using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using DuckDBGeoparquet.Models;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// ArcGIS-free helpers for optional file-geodatabase output: path resolution,
    /// FGDB-valid feature class names, overwrite-or-suffix allocation, and
    /// geoprocessing error text.
    /// </summary>
    public static class FileGeodatabaseOutput
    {
        public const string DefaultGeodatabaseName = "Overture.gdb";
        public const string CreateFileGdbTool = "management.CreateFileGDB";
        public const string ExportFeaturesTool = "conversion.ExportFeatures";

        public static bool IsEnabled(FileGeodatabaseOutputSettings settings)
            => settings != null && settings.Enabled;

        public static bool IsGeodatabasePath(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return false;

            string trimmed = path.Trim().TrimEnd('\\', '/');
            return trimmed.EndsWith(".gdb", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Resolves the destination .gdb: a user-chosen geodatabase, a user-chosen
        /// folder containing <see cref="DefaultGeodatabaseName"/>, or that default
        /// name under the data output folder.
        /// </summary>
        public static string ResolveGeodatabasePath(string dataOutputFolder, string userChosenPath)
        {
            if (!string.IsNullOrWhiteSpace(userChosenPath))
            {
                string trimmed = userChosenPath.Trim();
                if (IsGeodatabasePath(trimmed))
                    return Path.GetFullPath(trimmed);

                return Path.GetFullPath(Path.Combine(trimmed, DefaultGeodatabaseName));
            }

            if (string.IsNullOrWhiteSpace(dataOutputFolder))
                throw new ArgumentException("A data output folder is required when no geodatabase path is specified.", nameof(dataOutputFolder));

            return Path.GetFullPath(Path.Combine(dataOutputFolder.Trim(), DefaultGeodatabaseName));
        }

        public static void SplitGeodatabasePath(string gdbPath, out string parentFolder, out string gdbName)
        {
            if (string.IsNullOrWhiteSpace(gdbPath))
                throw new ArgumentException("Geodatabase path is required.", nameof(gdbPath));

            string full = Path.GetFullPath(gdbPath);
            parentFolder = Path.GetDirectoryName(full);
            gdbName = Path.GetFileName(full);
            if (string.IsNullOrWhiteSpace(parentFolder) || string.IsNullOrWhiteSpace(gdbName))
                throw new ArgumentException($"Could not split geodatabase path: {gdbPath}", nameof(gdbPath));
        }

        public static string ToDescriptiveGeometryType(string geomType)
        {
            if (string.IsNullOrWhiteSpace(geomType))
                return null;

            return geomType.Trim().ToUpperInvariant() switch
            {
                "POINT" => "points",
                "LINESTRING" => "lines",
                "POLYGON" => "polygons",
                "MULTIPOINT" => "multipoints",
                "MULTILINESTRING" => "multilines",
                "MULTIPOLYGON" => "multipolygons",
                _ => geomType.Trim().ToLowerInvariant()
            };
        }

        /// <summary>
        /// Builds an FGDB-valid feature class name from Overture theme, type, and
        /// optional geometry type (for example <c>buildings_building_polygons</c>).
        /// </summary>
        public static string BuildFeatureClassName(string theme, string type, string geometryType = null)
        {
            var parts = new[] { theme, type, ToDescriptiveGeometryType(geometryType) }
                .Where(part => !string.IsNullOrWhiteSpace(part))
                .Select(part => part.Trim());

            string combined = string.Join("_", parts);
            return ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(combined, "overture_output");
        }

        /// <summary>
        /// Picks a unique feature class name for this export session. The stable
        /// theme/type name is used first so geoprocessing overwrite can replace
        /// an existing class. When that name is already reserved this session,
        /// or <paramref name="forceSuffix"/> is set, a suffix is appended — the
        /// same fallback parquet uses when a delete fails.
        /// </summary>
        public static string AllocateFeatureClassName(
            string proposedName,
            ISet<string> reservedNames,
            bool forceSuffix = false,
            string conflictSuffix = null)
        {
            string baseName = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(
                proposedName, "overture_output");
            reservedNames ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (!forceSuffix && !reservedNames.Contains(baseName))
            {
                reservedNames.Add(baseName);
                return baseName;
            }

            return AllocateSuffixedName(baseName, reservedNames, conflictSuffix);
        }

        public static string AllocateSuffixedName(
            string baseName,
            ISet<string> reservedNames,
            string conflictSuffix = null)
        {
            string sanitizedBase = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(
                baseName, "overture_output");
            reservedNames ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            string suffix = string.IsNullOrWhiteSpace(conflictSuffix)
                ? DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture)
                : conflictSuffix.Trim();

            string withSuffix = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(
                $"{sanitizedBase}_{suffix}", "overture_output");
            if (!reservedNames.Contains(withSuffix))
            {
                reservedNames.Add(withSuffix);
                return withSuffix;
            }

            for (int i = 2; i <= 999; i++)
            {
                string numbered = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(
                    $"{sanitizedBase}_{i}", "overture_output");
                if (!reservedNames.Contains(numbered))
                {
                    reservedNames.Add(numbered);
                    return numbered;
                }
            }

            throw new InvalidOperationException(
                $"Could not allocate a unique file geodatabase feature class name from '{baseName}'.");
        }

        /// <summary>
        /// True when <paramref name="filePath"/> is a readable parquet file or a
        /// feature class inside an existing .gdb directory.
        /// </summary>
        public static bool LayerSourceExists(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                return false;

            if (File.Exists(filePath))
                return true;

            string parent = Path.GetDirectoryName(filePath);
            return IsGeodatabasePath(parent) && Directory.Exists(parent);
        }

        public static bool IsParquetPath(string filePath)
            => !string.IsNullOrWhiteSpace(filePath) &&
               filePath.IndexOf(".parquet", StringComparison.OrdinalIgnoreCase) >= 0;

        /// <summary>
        /// Parquet layers in Pro cannot read attribute domains; file geodatabase
        /// feature classes can. Skip the domain ArgumentException only for parquet.
        /// </summary>
        public static bool ShouldSkipParquetDomainLimitation(
            bool isFileGeodatabase,
            string filePath,
            string exceptionMessage)
        {
            if (isFileGeodatabase)
                return false;

            if (!IsParquetPath(filePath))
                return false;

            if (string.IsNullOrWhiteSpace(exceptionMessage))
                return false;

            return exceptionMessage.Contains("domain", StringComparison.OrdinalIgnoreCase) ||
                   exceptionMessage.Contains("not supported", StringComparison.OrdinalIgnoreCase);
        }

        public static string FormatToolFailure(string toolName, string context, GeoprocessingToolResult result)
        {
            var details = new List<string>();
            if (!string.IsNullOrWhiteSpace(result?.Messages))
                details.Add(result.Messages);
            if (result?.Exception != null && !string.IsNullOrWhiteSpace(result.Exception.Message))
                details.Add(result.Exception.Message);

            string detailText = details.Count > 0
                ? string.Join(" | ", details)
                : "No geoprocessing messages were returned.";

            string tool = string.IsNullOrWhiteSpace(toolName) ? "geoprocessing" : toolName;
            string action = string.IsNullOrWhiteSpace(context) ? "File geodatabase export" : context;
            return $"{action} failed ({tool}). {detailText} GeoParquet files were kept.";
        }
    }
}
