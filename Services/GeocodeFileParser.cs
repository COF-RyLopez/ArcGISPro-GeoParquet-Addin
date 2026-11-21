using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DuckDBGeoparquet.Services
{
    public static class GeocodeFileParser
    {
        public static ParsedGeocodeFile Parse(string[] lines, int maxRows)
        {
            if (lines == null || lines.Length == 0) return new ParsedGeocodeFile(Array.Empty<GeocodeFileQuery>(), false);
            if (maxRows < 1) throw new ArgumentOutOfRangeException(nameof(maxRows));
            var records = ReadRecords(lines);
            char delimiter = DetectDelimiter(records[0].Raw);
            string[] header = SplitDelimitedLine(records[0].Raw, delimiter);
            string[] normalizedHeader = [.. header.Select(NormalizeHeaderToken)];

            bool looksLikeHeader = normalizedHeader.Any(IsKnownGeocodeHeaderName);
            int firstRow = looksLikeHeader ? 1 : 0;

            int addressColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "address", "singleline", "single_line", "single line input", "full_address")
                : (header.Length == 1 ? 0 : -1);
            int streetColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "street", "street_name", "road", "name", "saddstr")
                : -1;
            int houseNumberColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "number", "housenumber", "house_number", "saddno")
                : -1;
            int housePrefixColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "prefix", "predir", "streetprefix", "saddpref")
                : -1;
            int streetTypeColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "streettype", "street_type", "sttype", "saddsttyp")
                : -1;
            int streetSuffixColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "suffix", "postdir", "streetsuffix", "saddstsuf")
                : -1;
            int unitColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "unit", "apt", "apartment", "suite", "sunit")
                : -1;
            int cityColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "city", "scity", "locality")
                : -1;
            int stateColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "state", "state2", "region", "province")
                : -1;
            int zipColumn = looksLikeHeader
                ? FindColumn(normalizedHeader, "zip", "zip5", "szip", "szip5", "postcode", "postalcode")
                : -1;

            if (looksLikeHeader && addressColumn < 0 && streetColumn < 0)
            {
                throw new InvalidOperationException(
                    "Could not find an address column. Include an 'address' field or separate street fields in the file.");
            }

            int idColumn = looksLikeHeader ? FindColumn(normalizedHeader, "id", "objectid", "record_id", "globalid") : -1;
            var queries = new List<GeocodeFileQuery>();
            for (int i = firstRow; i < records.Count && queries.Count < maxRows; i++)
            {
                var cells = SplitDelimitedLine(records[i].Raw, delimiter);
                var searchQueries = BuildGeocodeQueries(
                    cells,
                    addressColumn,
                    houseNumberColumn,
                    housePrefixColumn,
                    streetColumn,
                    streetTypeColumn,
                    streetSuffixColumn,
                    unitColumn,
                    cityColumn,
                    stateColumn,
                    zipColumn);

                if (searchQueries.Count == 0 && !looksLikeHeader && cells.Length > 0)
                {
                    string fallback = CleanCell(cells[0]);
                    if (!string.IsNullOrWhiteSpace(fallback))
                    {
                        searchQueries = [fallback];
                    }
                }

                if (searchQueries.Count > 0)
                {
                    queries.Add(new GeocodeFileQuery(searchQueries[0], searchQueries, records[i].Line,
                        idColumn >= 0 && idColumn < cells.Length ? CleanCell(cells[idColumn]) : null, records[i].Raw));
                }
                else
                {
                    queries.Add(new GeocodeFileQuery("", Array.Empty<string>(), records[i].Line,
                        idColumn >= 0 && idColumn < cells.Length ? CleanCell(cells[idColumn]) : null, records[i].Raw));
                }
            }

            return new ParsedGeocodeFile(queries, records.Count - firstRow > maxRows);
        }

        private static IReadOnlyList<string> BuildGeocodeQueries(
            string[] cells,
            int addressColumn,
            int houseNumberColumn,
            int housePrefixColumn,
            int streetColumn,
            int streetTypeColumn,
            int streetSuffixColumn,
            int unitColumn,
            int cityColumn,
            int stateColumn,
            int zipColumn)
        {
            string address = CleanCell(GetCell(cells, addressColumn));
            string unit = CleanCell(GetCell(cells, unitColumn));
            if (!string.IsNullOrWhiteSpace(address) && !string.IsNullOrWhiteSpace(unit))
            {
                // Never weaken a supplied unit to a base-building address. A duplicated or
                // contradictory unit can go to review rather than silently disappear.
                address += " " + unit;
            }
            if (string.IsNullOrWhiteSpace(address))
            {
                var addressParts = new[]
                {
                    CleanCell(GetCell(cells, houseNumberColumn)),
                    CleanCell(GetCell(cells, housePrefixColumn)),
                    CleanCell(GetCell(cells, streetColumn)),
                    CleanCell(GetCell(cells, streetTypeColumn)),
                    CleanCell(GetCell(cells, streetSuffixColumn)),
                    CleanCell(GetCell(cells, unitColumn))
                };

                address = string.Join(" ", addressParts.Where(part => !string.IsNullOrWhiteSpace(part)));
            }

            if (string.IsNullOrWhiteSpace(address))
            {
                return [];
            }

            string city = CleanCell(GetCell(cells, cityColumn));
            string state = CleanCell(GetCell(cells, stateColumn));
            string zip = CleanCell(GetCell(cells, zipColumn));

            return GeocodeFileQueryBuilder.BuildQueryVariants(address, city, state, zip);
        }

        private static int FindColumn(string[] normalizedHeader, params string[] aliases)
        {
            var normalizedAliases = aliases
                .Select(NormalizeHeaderToken)
                .Where(alias => !string.IsNullOrWhiteSpace(alias))
                .ToHashSet(StringComparer.Ordinal);

            for (int i = 0; i < normalizedHeader.Length; i++)
            {
                if (normalizedAliases.Contains(normalizedHeader[i]))
                {
                    return i;
                }
            }

            return -1;
        }

        private static bool IsKnownGeocodeHeaderName(string name)
        {
            return FindColumn(
                [name],
                "address",
                "singleline",
                "single_line",
                "single line input",
                "full_address",
                "street",
                "street_name",
                "road",
                "name",
                "saddstr",
                "saddno",
                "city",
                "scity",
                "state",
                "state2",
                "zip",
                "szip",
                "postcode") >= 0;
        }

        private static string NormalizeHeaderToken(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            var normalized = new StringBuilder(value.Length);
            foreach (char ch in value.Trim().Trim('"').ToLowerInvariant())
            {
                if (char.IsLetterOrDigit(ch))
                {
                    normalized.Append(ch);
                }
            }

            return normalized.ToString();
        }

        private static string GetCell(string[] cells, int index)
        {
            return index >= 0 && index < cells.Length ? cells[index] : string.Empty;
        }

        private static string CleanCell(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            return string.Join(" ",
                value.Trim().Trim('"')
                    .Split((char[])null, StringSplitOptions.RemoveEmptyEntries));
        }

        private static List<(string Raw, int Line)> ReadRecords(string[] lines)
        {
            var records = new List<(string Raw, int Line)>();
            var raw = new StringBuilder();
            bool inQuotes = false;
            int firstLine = 1;
            for (int i = 0; i < lines.Length; i++)
            {
                if (raw.Length > 0) raw.Append('\n');
                raw.Append(lines[i]);
                for (int j = 0; j < lines[i].Length; j++)
                {
                    if (lines[i][j] != '"') continue;
                    if (inQuotes && j + 1 < lines[i].Length && lines[i][j + 1] == '"') { j++; continue; }
                    inQuotes = !inQuotes;
                }
                if (!inQuotes)
                {
                    records.Add((raw.ToString(), firstLine));
                    raw.Clear();
                    firstLine = i + 2;
                }
            }
            if (inQuotes) throw new InvalidOperationException($"Unclosed quoted field starting at line {firstLine}. Fix the input file before geocoding.");
            return records;
        }

        private static char DetectDelimiter(string line)
        {
            char bestDelimiter = ',';
            int bestCount = -1;

            foreach (char candidate in new[] { '\t', ',', ';', '|' })
            {
                int count = 0;
                bool inQuotes = false;
                for (int i = 0; i < line.Length; i++)
                {
                    char ch = line[i];
                    if (ch == '"')
                    {
                        if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                        {
                            i++;
                            continue;
                        }

                        inQuotes = !inQuotes;
                    }
                    else if (ch == candidate && !inQuotes)
                    {
                        count++;
                    }
                }

                if (count > bestCount)
                {
                    bestDelimiter = candidate;
                    bestCount = count;
                }
            }

            return bestDelimiter;
        }

        private static string[] SplitDelimitedLine(string line, char delimiter)
        {
            // Minimal delimited split honoring double-quoted cells.
            var cells = new List<string>();
            var sb = new StringBuilder();
            bool inQuotes = false;
            for (int i = 0; i < line.Length; i++)
            {
                char ch = line[i];
                if (ch == '"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        sb.Append('"');
                        i++;
                        continue;
                    }

                    inQuotes = !inQuotes;
                }
                else if (ch == delimiter && !inQuotes)
                {
                    cells.Add(sb.ToString());
                    sb.Clear();
                }
                else
                {
                    sb.Append(ch);
                }
            }
            cells.Add(sb.ToString());
            return [.. cells];
        }

        public sealed record GeocodeFileQuery(string SourceQuery, IReadOnlyList<string> SearchQueries, int SourceLine, string SourceId, string RawInput);
        public sealed record ParsedGeocodeFile(IReadOnlyList<GeocodeFileQuery> Queries, bool WasTruncated);

    }
}
