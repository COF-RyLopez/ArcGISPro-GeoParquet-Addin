using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ArcGIS.Desktop.Framework.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Provides utilities to transform Overture themes into NG911-compliant outputs
    /// according to the NENA-STA-006.x GIS Data Model. Initial focus is on
    /// Address Points and Road Centerlines with incremental coverage.
    /// </summary>
    public sealed class Ng911Mapper
    {
        private readonly DataProcessor _dataProcessor;

        public Ng911Mapper(DataProcessor dataProcessor)
        {
            _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
        }

        /// <summary>
        /// Entry point to map currently loaded Overture table (in DuckDB via DataProcessor)
        /// to an NG911 target. This scaffolds the flow and will be expanded with concrete mappings.
        /// </summary>
        /// <param name="ng911TargetName">E.g., "SiteStructureAddressPoint" or "RoadCenterline"</param>
        /// <param name="outputFolder">Where NG911 GeoParquet should be written</param>
        /// <param name="progress">Optional progress reporter</param>
        public async Task MapCurrentToNg911Async(string ng911TargetName, string outputFolder, IProgress<string> progress = null)
        {
            if (string.IsNullOrWhiteSpace(ng911TargetName))
                throw new ArgumentException("Target name is required", nameof(ng911TargetName));
            if (string.IsNullOrWhiteSpace(outputFolder))
                throw new ArgumentException("Output folder is required", nameof(outputFolder));

            Directory.CreateDirectory(outputFolder);

            progress?.Report($"Preparing NG911 mapping for {ng911TargetName}...");

            switch (ng911TargetName)
            {
                case "SiteStructureAddressPoint":
                    await MapAddressPointsAsync(outputFolder, progress);
                    break;
                case "RoadCenterline":
                    await MapRoadCenterlinesAsync(outputFolder, progress);
                    break;
                default:
                    progress?.Report($"No mapping defined yet for '{ng911TargetName}'.");
                    break;
            }
        }

        // Placeholder mapping: Derive NG911 Address Points from Overture addresses where feasible
        private async Task MapAddressPointsAsync(string outputFolder, IProgress<string> progress)
        {
            progress?.Report("Mapping Overture addresses → NG911 SiteStructureAddressPoint (v0)...");

            // Ensure the correct dataset is loaded into current_table
            if (!await _dataProcessor.LoadLocalDatasetAsCurrentAsync("address", progress))
            {
                progress?.Report("NG911 AddressPoints: Could not load local 'address' dataset. Aborting.");
                return;
            }

            // Build a SELECT that adapts to available columns in current_table
            var cols = await _dataProcessor.GetCurrentTableColumnsAsync();
            string Col(string name) => cols.Contains(name) ? name : "NULL";
            string ColAny(params string[] names)
            {
                foreach (var n in names)
                {
                    if (cols.Contains(n)) return n;
                }
                return "NULL";
            }

            string outputPath = Path.Combine(outputFolder, "SiteStructureAddressPoint", "NG911_SiteStructureAddressPoint.parquet");
            var addNumExpr = ColAny("address_number", "number", "house_number", "housenumber");
            var preDirExpr = Col("street_name_pre_dir");
            var preTypeExpr = Col("street_name_pre_type");
            var stNameExpr = ColAny("street_name", "name");
            var stTypeExpr = Col("street_name_post_type");
            var postDirExpr = Col("street_name_post_dir");
            var streetRawExpr = cols.Contains("street") ? "street" : stNameExpr; // fallback to generic name
            var landUnitExpr = $"COALESCE({Col("unit")}, {Col("unit_number")})";
            var cityExpr = ColAny("postal_city", "city", "locality");
            var regionExpr = ColAny("region", "state");
            var postcodeExpr = ColAny("postcode", "postal_code", "zip");
            var countryExpr = Col("country");
            var idExpr = Col("id");
            // Use a constant Source to avoid complex struct types from Overture's 'source' column
            const string sourceLiteral = "'Overture'";

            // Derive fields where source columns are missing
            string addNumPre = addNumExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(regexp_extract({addNumExpr}, '^(?i)(\\\
D*)(\\\
d+)(.*)$', 1), '')";
            string addNumCore = addNumExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(regexp_extract({addNumExpr}, '^(?i)(\\\
D*)(\\\
d+)(.*)$', 2), '')";
            string addNumSuf = addNumExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(regexp_extract({addNumExpr}, '^(?i)(\\\
D*)(\\\
d+)(.*)$', 3), '')";

            string preDirOut = preDirExpr != "NULL" ? preDirExpr : (streetRawExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(UPPER(regexp_extract({streetRawExpr}, '^(?i)(N|S|E|W|NE|NW|SE|SW)\\\\b', 1)), '')");
            string preTypeOut = preTypeExpr != "NULL" ? preTypeExpr : "CAST(NULL AS VARCHAR)";
            string stTypeOut = stTypeExpr != "NULL" ? stTypeExpr : (streetRawExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(UPPER(regexp_extract({streetRawExpr}, '\\\\b(AVE|BLVD|CIR|CT|DR|RD|ST|STREET|AVENUE|BOULEVARD|LANE|LN|WAY|ROAD|HIGHWAY|HWY|PKWY|PATH|TERRACE|TER|PLACE|PL|COURT|CT)\\\\b', 1)), '')");
            string postDirOut = postDirExpr != "NULL" ? postDirExpr : (streetRawExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(UPPER(regexp_extract({streetRawExpr}, '\\\\b(N|S|E|W|NE|NW|SE|SW)$', 1)), '')");
            string stNameOut = stNameExpr != "NULL" ? stNameExpr : (streetRawExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : $"TRIM(regexp_replace(regexp_replace(regexp_replace({streetRawExpr}, '^(?i)(?:\\\\s*(?:N|S|E|W|NE|NW|SE|SW)\\\\s+)', ''), '(?i)\\\\s+(?:N|S|E|W|NE|NW|SE|SW)\\\\s*$', ''), '(?i)\\\\s+(?:AVE|BLVD|CIR|CT|DR|RD|ST|STREET|AVENUE|BOULEVARD|LANE|LN|WAY|ROAD|HIGHWAY|HWY|PKWY|PATH|TERRACE|TER|PLACE|PL|COURT|CT)\\\\s*$', ''))");

            string select =
                "SELECT " +
                idExpr + " AS NGUID, " +
                addNumPre + " AS AddNum_Pre, " +
                addNumCore + " AS AddNum, " +
                addNumSuf + " AS AddNum_Suf, " +
                preDirOut + " AS PreDir, " +
                preTypeOut + " AS PreType, " +
                stNameOut + " AS StName, " +
                stTypeOut + " AS StType, " +
                postDirOut + " AS PostDir, " +
                landUnitExpr + " AS LandUnit, " +
                cityExpr + " AS Municipal, " +
                regionExpr + " AS State, " +
                postcodeExpr + " AS ZipCode, " +
                (countryExpr == "NULL" ? "CAST(NULL AS VARCHAR)" : countryExpr) + " AS Country, " +
                "CAST(NULL AS VARCHAR) AS PlaceType, " +
                "CAST(NULL AS VARCHAR) AS Validation, " +
                idExpr + " AS SourceOID, " +
                sourceLiteral + " AS Source, " +
                "geometry " +
                "FROM current_table WHERE geometry IS NOT NULL";

            string actualPath = await _dataProcessor.ExportSelectToGeoParquetAsync(select, outputPath, "NG911_SiteStructureAddressPoint", progress);
            await _dataProcessor.AddLayerFileToMapAsync(actualPath, "NG911 SiteStructure Address Point", progress);
        }

        // Placeholder mapping: Derive NG911 RoadCenterline from Overture base/transportation where feasible
        private async Task MapRoadCenterlinesAsync(string outputFolder, IProgress<string> progress)
        {
            progress?.Report("Mapping Overture roads → NG911 RoadCenterline (v0)...");

            // Ensure the correct dataset is loaded into current_table
            if (!await _dataProcessor.LoadLocalDatasetAsCurrentAsync("segment", progress))
            {
                progress?.Report("NG911 RoadCenterline: Could not load local 'segment' dataset. Aborting.");
                return;
            }

            // Minimal viable mapping for centerlines. Adapts to available columns.
            var cols = await _dataProcessor.GetCurrentTableColumnsAsync();
            string Col(string name) => cols.Contains(name) ? name : "NULL";
            string ColAny(params string[] names)
            {
                foreach (var n in names)
                {
                    if (cols.Contains(n)) return n;
                }
                return "NULL";
            }
            string outputPath = Path.Combine(outputFolder, "RoadCenterline", "NG911_RoadCenterline.parquet");
            var preDirRCol = Col("street_name_pre_dir");
            var preTypeRCol = Col("street_name_pre_type");
            var stNameRCol = ColAny("street_name", "name");
            var stTypeRCol = Col("street_name_post_type");
            var postDirRCol = Col("street_name_post_dir");
            var nameBaseR = stNameRCol != "NULL" ? stNameRCol : (cols.Contains("names_primary") ? "names_primary" : (cols.Contains("names") ? "names.primary" : "NULL"));
            var roadClass = $"COALESCE({Col("road_class")}, {Col("class")})";
            var oneWay = Col("one_way");
            var idR = Col("id");
            // Use a constant for Source to avoid complex struct types
            const string sourceLiteralR = "'Overture'";

            string preDirR = preDirRCol != "NULL" ? preDirRCol : (nameBaseR == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(UPPER(regexp_extract({nameBaseR}, '^(?i)(N|S|E|W|NE|NW|SE|SW)\\\\b', 1)), '')");
            string preTypeR = preTypeRCol != "NULL" ? preTypeRCol : "CAST(NULL AS VARCHAR)";
            string stTypeR = stTypeRCol != "NULL" ? stTypeRCol : (nameBaseR == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(UPPER(regexp_extract({nameBaseR}, '\\\\b(AVE|BLVD|CIR|CT|DR|RD|ST|STREET|AVENUE|BOULEVARD|LANE|LN|WAY|ROAD|HIGHWAY|HWY|PKWY|PATH|TERRACE|TER|PLACE|PL|COURT|CT)\\\\b', 1)), '')");
            string postDirR = postDirRCol != "NULL" ? postDirRCol : (nameBaseR == "NULL" ? "CAST(NULL AS VARCHAR)" : $"NULLIF(UPPER(regexp_extract({nameBaseR}, '\\\\b(N|S|E|W|NE|NW|SE|SW)$', 1)), '')");
            string stNameR = nameBaseR == "NULL" ? "CAST(NULL AS VARCHAR)" : $"TRIM(regexp_replace(regexp_replace(regexp_replace({nameBaseR}, '^(?i)(?:\\\\s*(?:N|S|E|W|NE|NW|SE|SW)\\\\s+)', ''), '(?i)\\\\s+(?:N|S|E|W|NE|NW|SE|SW)\\\\s*$', ''), '(?i)\\\\s+(?:AVE|BLVD|CIR|CT|DR|RD|ST|STREET|AVENUE|BOULEVARD|LANE|LN|WAY|ROAD|HIGHWAY|HWY|PKWY|PATH|TERRACE|TER|PLACE|PL|COURT|CT)\\\\s*$', ''))";

            string select =
                "SELECT " +
                idR + " AS NGUID, " +
                preDirR + " AS PreDir, " +
                preTypeR + " AS PreType, " +
                stNameR + " AS StName, " +
                stTypeR + " AS StType, " +
                postDirR + " AS PostDir, " +
                roadClass + " AS RoadClass, " +
                "CAST(" + oneWay + " AS VARCHAR) AS OneWay, " +
                "CAST(NULL AS VARCHAR) AS ParityLeft, " +
                "CAST(NULL AS VARCHAR) AS ParityRight, " +
                idR + " AS SourceOID, " +
                sourceLiteralR + " AS Source, " +
                "geometry " +
                "FROM current_table WHERE geometry IS NOT NULL";

            string actualPath = await _dataProcessor.ExportSelectToGeoParquetAsync(select, outputPath, "NG911_RoadCenterline", progress);
            await _dataProcessor.AddLayerFileToMapAsync(actualPath, "NG911 Road Centerline", progress);
        }
    }
}


