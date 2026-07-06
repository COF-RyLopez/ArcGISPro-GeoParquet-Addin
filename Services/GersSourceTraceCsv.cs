using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace DuckDBGeoparquet.Services
{
    public static class GersSourceTraceCsv
    {
        public static string ExportGersIdsFromBridgeCsv(string bridgeCsvPath)
        {
            if (string.IsNullOrWhiteSpace(bridgeCsvPath) || !File.Exists(bridgeCsvPath))
                throw new FileNotFoundException("Bridge CSV was not found.", bridgeCsvPath);

            string outputPath = Path.Combine(Path.GetTempPath(), $"gers_trace_bridge_{Guid.NewGuid():N}.csv");
            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var sb = new StringBuilder();
            sb.AppendLine("gers_id");

            string[] lines = File.ReadAllLines(bridgeCsvPath);
            if (lines.Length <= 1)
            {
                File.WriteAllText(outputPath, sb.ToString(), Encoding.UTF8);
                return outputPath;
            }

            string[] headers = ParseCsvLine(lines[0]);
            int idIndex = FindColumnIndex(headers, "id", "gers_id");
            if (idIndex < 0)
                throw new InvalidOperationException("Bridge CSV is missing an id or gers_id column.");

            for (int lineIndex = 1; lineIndex < lines.Length; lineIndex++)
            {
                if (string.IsNullOrWhiteSpace(lines[lineIndex]))
                    continue;

                string[] values = ParseCsvLine(lines[lineIndex]);
                if (idIndex >= values.Length)
                    continue;

                string gersId = values[idIndex]?.Trim();
                if (!string.IsNullOrWhiteSpace(gersId) && ids.Add(gersId))
                {
                    sb.AppendLine(EscapeCsv(gersId));
                }
            }

            File.WriteAllText(outputPath, sb.ToString(), Encoding.UTF8);
            return outputPath;
        }

        public static void EnrichTraceOutputCsv(string csvPath)
        {
            if (string.IsNullOrWhiteSpace(csvPath) || !File.Exists(csvPath))
                return;

            string[] lines = File.ReadAllLines(csvPath);
            if (lines.Length <= 1)
                return;

            string[] headers = ParseCsvLine(lines[0]);
            if (FindColumnIndex(headers, "edit_url") >= 0)
                return;

            int gersIdIndex = FindColumnIndex(headers, "gers_id");
            int datasetIndex = FindColumnIndex(headers, "dataset");
            int recordIdIndex = FindColumnIndex(headers, "record_id");
            int updateTimeIndex = FindColumnIndex(headers, "update_time");
            int themeIndex = FindColumnIndex(headers, "theme");
            int typeIndex = FindColumnIndex(headers, "type");
            int sourceUrlIndex = FindColumnIndex(headers, "source_url");

            var output = new StringBuilder();
            output.AppendLine(string.Join(",",
                headers.Concat(new[] { "edit_url", "edit_platform", "contribution_url", "edit_instructions" })
                    .Select(EscapeCsv)));

            for (int lineIndex = 1; lineIndex < lines.Length; lineIndex++)
            {
                if (string.IsNullOrWhiteSpace(lines[lineIndex]))
                    continue;

                string[] values = ParseCsvLine(lines[lineIndex]);
                var record = GersSourceEditResolver.EnrichTraceRow(new GersSourceTraceRecord
                {
                    GersId = GetValue(values, gersIdIndex),
                    Dataset = GetValue(values, datasetIndex),
                    RecordId = GetValue(values, recordIdIndex),
                    UpdateTime = GetValue(values, updateTimeIndex),
                    Theme = GetValue(values, themeIndex),
                    Type = GetValue(values, typeIndex),
                    SourceUrl = GetValue(values, sourceUrlIndex)
                });

                output.AppendLine(string.Join(",",
                    values.Select(EscapeCsv)
                        .Concat(new[]
                        {
                            EscapeCsv(record.EditUrl),
                            EscapeCsv(record.EditPlatform),
                            EscapeCsv(record.ContributionUrl),
                            EscapeCsv(record.EditInstructions)
                        })));
            }

            File.WriteAllText(csvPath, output.ToString(), Encoding.UTF8);
        }

        public static IReadOnlyList<GersSourceTraceRecord> ReadTraceOutput(string csvPath, int maxRows = 50)
        {
            if (string.IsNullOrWhiteSpace(csvPath) || !File.Exists(csvPath))
                return [];

            string[] lines = File.ReadAllLines(csvPath);
            if (lines.Length <= 1)
                return [];

            string[] headers = ParseCsvLine(lines[0]);
            int gersIdIndex = FindColumnIndex(headers, "gers_id");
            int datasetIndex = FindColumnIndex(headers, "dataset");
            int recordIdIndex = FindColumnIndex(headers, "record_id");
            int updateTimeIndex = FindColumnIndex(headers, "update_time");
            int themeIndex = FindColumnIndex(headers, "theme");
            int typeIndex = FindColumnIndex(headers, "type");
            int sourceUrlIndex = FindColumnIndex(headers, "source_url");
            int editUrlIndex = FindColumnIndex(headers, "edit_url");
            int editPlatformIndex = FindColumnIndex(headers, "edit_platform");
            int contributionUrlIndex = FindColumnIndex(headers, "contribution_url");
            int instructionsIndex = FindColumnIndex(headers, "edit_instructions");

            var records = new List<GersSourceTraceRecord>();
            for (int lineIndex = 1; lineIndex < lines.Length && records.Count < maxRows; lineIndex++)
            {
                if (string.IsNullOrWhiteSpace(lines[lineIndex]))
                    continue;

                string[] values = ParseCsvLine(lines[lineIndex]);
                var record = new GersSourceTraceRecord
                {
                    GersId = GetValue(values, gersIdIndex),
                    Dataset = GetValue(values, datasetIndex),
                    RecordId = GetValue(values, recordIdIndex),
                    UpdateTime = GetValue(values, updateTimeIndex),
                    Theme = GetValue(values, themeIndex),
                    Type = GetValue(values, typeIndex),
                    SourceUrl = GetValue(values, sourceUrlIndex),
                    EditUrl = GetValue(values, editUrlIndex),
                    EditPlatform = GetValue(values, editPlatformIndex),
                    ContributionUrl = GetValue(values, contributionUrlIndex),
                    EditInstructions = GetValue(values, instructionsIndex)
                };

                records.Add(GersSourceEditResolver.EnrichTraceRow(record));
            }

            return records;
        }

        public static IReadOnlyDictionary<string, int> ReadDatasetCounts(string csvPath)
        {
            if (string.IsNullOrWhiteSpace(csvPath) || !File.Exists(csvPath))
                return new Dictionary<string, int>();

            string[] lines = File.ReadAllLines(csvPath);
            if (lines.Length <= 1)
                return new Dictionary<string, int>();

            string[] headers = ParseCsvLine(lines[0]);
            int datasetIndex = FindColumnIndex(headers, "dataset");
            if (datasetIndex < 0)
                return new Dictionary<string, int>();

            var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int lineIndex = 1; lineIndex < lines.Length; lineIndex++)
            {
                if (string.IsNullOrWhiteSpace(lines[lineIndex]))
                    continue;

                string[] values = ParseCsvLine(lines[lineIndex]);
                string dataset = GetValue(values, datasetIndex);
                if (string.IsNullOrWhiteSpace(dataset))
                    continue;

                counts[dataset] = counts.TryGetValue(dataset, out int count) ? count + 1 : 1;
            }

            return counts;
        }

        public static string FormatDatasetSummary(IReadOnlyDictionary<string, int> counts, int maxItems = 4)
        {
            if (counts == null || counts.Count == 0)
                return "No upstream source mappings were returned yet.";

            return string.Join(" • ", counts
                .OrderByDescending(item => item.Value)
                .Take(maxItems)
                .Select(item => $"{item.Key}: {item.Value:N0}"));
        }

        private static int FindColumnIndex(string[] headers, params string[] candidates)
        {
            for (int index = 0; index < headers.Length; index++)
            {
                string header = headers[index]?.Trim();
                if (candidates.Any(candidate =>
                        string.Equals(header, candidate, StringComparison.OrdinalIgnoreCase)))
                {
                    return index;
                }
            }

            return -1;
        }

        private static string GetValue(string[] values, int index) =>
            index >= 0 && index < values.Length ? values[index]?.Trim() : string.Empty;

        private static string[] ParseCsvLine(string line)
        {
            var values = new List<string>();
            var current = new StringBuilder();
            bool inQuotes = false;

            for (int index = 0; index < line.Length; index++)
            {
                char ch = line[index];
                if (ch == '"')
                {
                    if (inQuotes && index + 1 < line.Length && line[index + 1] == '"')
                    {
                        current.Append('"');
                        index++;
                    }
                    else
                    {
                        inQuotes = !inQuotes;
                    }

                    continue;
                }

                if (ch == ',' && !inQuotes)
                {
                    values.Add(current.ToString());
                    current.Clear();
                    continue;
                }

                current.Append(ch);
            }

            values.Add(current.ToString());
            return values.ToArray();
        }

        private static string EscapeCsv(string value)
        {
            if (string.IsNullOrEmpty(value))
                return "\"\"";

            if (value.Contains('"', StringComparison.Ordinal))
                return $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

            if (value.Contains(',', StringComparison.Ordinal))
                return $"\"{value}\"";

            return value;
        }
    }
}
