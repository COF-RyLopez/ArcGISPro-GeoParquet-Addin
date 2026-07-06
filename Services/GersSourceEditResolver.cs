using DuckDBGeoparquet.Models;
using System;
using System.Text.RegularExpressions;

namespace DuckDBGeoparquet.Services
{
    public static class GersSourceEditResolver
    {
        private static readonly Regex OsmNodePattern = new(@"^n(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex OsmWayPattern = new(@"^w(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex OsmRelationPattern = new(@"^r(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public static GersSourceTraceRecord EnrichTraceRow(GersSourceTraceRecord record)
        {
            if (record == null)
                return null;

            var edit = Resolve(record.Dataset, record.RecordId);
            return new GersSourceTraceRecord
            {
                GersId = record.GersId,
                Dataset = record.Dataset,
                RecordId = record.RecordId,
                UpdateTime = record.UpdateTime,
                Theme = record.Theme,
                Type = record.Type,
                SourceUrl = string.IsNullOrWhiteSpace(record.SourceUrl) ? edit.EditUrl : record.SourceUrl,
                EditUrl = edit.EditUrl,
                EditPlatform = edit.Platform,
                ContributionUrl = edit.ContributionUrl,
                EditInstructions = edit.Instructions
            };
        }

        public static (string Platform, string EditUrl, string ContributionUrl, string Instructions) Resolve(
            string dataset,
            string recordId)
        {
            string normalizedDataset = dataset?.Trim() ?? string.Empty;
            string lowerDataset = normalizedDataset.ToLowerInvariant();

            if (lowerDataset.Contains("openstreetmap", StringComparison.Ordinal))
            {
                string osmUrl = BuildOpenStreetMapUrl(recordId);
                return (
                    "OpenStreetMap",
                    osmUrl,
                    "https://wiki.openstreetmap.org/wiki/Main_Page",
                    "Edit the feature in OpenStreetMap (iD or JOSM). Overture conflation can pick up OSM changes in a future monthly release.");
            }

            if (lowerDataset.Contains("esri", StringComparison.Ordinal))
            {
                return (
                    "Esri Community Maps",
                    null,
                    "https://livingatlas.arcgis.com/community-maps/",
                    "Corrections to Esri Community Maps flow through Esri's community contribution programs, then into Overture via bridge files.");
            }

            if (lowerDataset.Contains("meta", StringComparison.Ordinal))
            {
                return (
                    "Meta",
                    null,
                    "https://mapwith.ai/",
                    "Meta place data is maintained through Meta's mapping programs. Report issues through Map With AI or Overture contribution channels.");
            }

            if (lowerDataset.Contains("microsoft", StringComparison.Ordinal))
            {
                return (
                    "Microsoft",
                    null,
                    "https://www.microsoft.com/en-us/maps",
                    "Microsoft-sourced features are conflated into Overture. Use Overture contribution workflows for persistent corrections.");
            }

            if (lowerDataset.Contains("pinmeto", StringComparison.Ordinal))
            {
                return (
                    "PinMeTo",
                    null,
                    "https://pinme.to/",
                    "PinMeTo place records are maintained by the provider. Contact PinMeTo or contribute through Overture for cross-release fixes.");
            }

            if (lowerDataset.Contains("geoboundaries", StringComparison.Ordinal))
            {
                return (
                    "geoBoundaries",
                    null,
                    "https://www.geoboundaries.org/",
                    "Administrative boundary updates are managed by the geoBoundaries project.");
            }

            return (
                string.IsNullOrWhiteSpace(normalizedDataset) ? "Unknown source" : normalizedDataset,
                null,
                "https://docs.overturemaps.org/gers/bridge-files/",
                "Use Overture bridge files to identify the upstream provider, then contribute corrections through that provider or Overture Maps Foundation.");
        }

        public static string BuildOpenStreetMapUrl(string recordId)
        {
            if (string.IsNullOrWhiteSpace(recordId))
                return null;

            string trimmed = recordId.Trim();
            Match nodeMatch = OsmNodePattern.Match(trimmed);
            if (nodeMatch.Success)
                return $"https://www.openstreetmap.org/node/{nodeMatch.Groups[1].Value}";

            Match wayMatch = OsmWayPattern.Match(trimmed);
            if (wayMatch.Success)
                return $"https://www.openstreetmap.org/way/{wayMatch.Groups[1].Value}";

            Match relationMatch = OsmRelationPattern.Match(trimmed);
            if (relationMatch.Success)
                return $"https://www.openstreetmap.org/relation/{relationMatch.Groups[1].Value}";

            return null;
        }
    }
}
