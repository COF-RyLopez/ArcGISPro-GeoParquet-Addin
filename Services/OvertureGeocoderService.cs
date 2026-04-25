using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public class OvertureGeocoderService : IDisposable
    {
        private const string AddinDataSubfolder = "OvertureProAddinData";
        private readonly DataProcessor _dataProcessor;
        private readonly OvertureLocatorBuildService _locatorBuildService;
        private bool _isInitialized;
        public bool PreferLocatorSearch { get; set; }

        public OvertureGeocoderService()
        {
            _dataProcessor = new DataProcessor();
            _locatorBuildService = new OvertureLocatorBuildService();
        }

        public async Task InitializeAsync()
        {
            if (_isInitialized)
            {
                return;
            }

            await _dataProcessor.InitializeDuckDBAsync();
            _isInitialized = true;
        }

        public async Task<List<GeocodeCandidate>> SearchAsync(string query, Envelope roiExtent = null, int maxResults = 25)
        {
            await InitializeAsync();

            string normalizedQuery = NormalizeQuery(query);
            if (string.IsNullOrWhiteSpace(normalizedQuery))
            {
                return [];
            }

            string addressParquetGlob = ResolveParquetGlobPath("address");
            string placeParquetGlob = ResolveParquetGlobPath("place");
            if (string.IsNullOrWhiteSpace(addressParquetGlob) && string.IsNullOrWhiteSpace(placeParquetGlob))
            {
                return [];
            }

            int perSourceLimit = Math.Max(maxResults, 25);
            var merged = new List<GeocodeCandidate>();

            if (!string.IsNullOrWhiteSpace(addressParquetGlob))
            {
                var addressCandidates = await _dataProcessor.SearchAddressCandidatesAsync(addressParquetGlob, normalizedQuery, roiExtent, perSourceLimit);
                merged.AddRange(addressCandidates);
            }

            if (!string.IsNullOrWhiteSpace(placeParquetGlob))
            {
                var placeCandidates = await _dataProcessor.SearchPlaceCandidatesAsync(placeParquetGlob, normalizedQuery, roiExtent, perSourceLimit);
                merged.AddRange(placeCandidates);
            }

            return merged
                .OrderByDescending(c => c.Score)
                .ThenBy(c => c.DisplayLabel, StringComparer.OrdinalIgnoreCase)
                .Take(Math.Clamp(maxResults, 1, 100))
                .ToList();
        }

        public bool IsLocatorReady()
        {
            return _locatorBuildService.IsLocatorReady();
        }

        public LocatorBuildMetadata GetLocatorMetadata()
        {
            return _locatorBuildService.ReadBuildMetadata();
        }

        public async Task<LocatorBuildResult> BuildLocatorAsync(bool forceRebuild)
        {
            return await _locatorBuildService.BuildOrRebuildLocatorAsync(forceRebuild);
        }

        private static string NormalizeQuery(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            return string.Join(" ", value
                .Trim()
                .ToLowerInvariant()
                .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
        }

        private static string ResolveParquetGlobPath(string dataType)
        {
            string dataRoot = ResolveDataRootPath();
            if (string.IsNullOrWhiteSpace(dataRoot) || !Directory.Exists(dataRoot))
            {
                return null;
            }

            var releaseDirectories = new DirectoryInfo(dataRoot)
                .GetDirectories()
                .OrderByDescending(d => d.LastWriteTimeUtc)
                .ToList();

            foreach (var releaseDir in releaseDirectories)
            {
                string typeDir = Path.Combine(releaseDir.FullName, dataType);
                if (!Directory.Exists(typeDir))
                {
                    continue;
                }

                if (Directory.EnumerateFiles(typeDir, "*.parquet", SearchOption.TopDirectoryOnly).Any())
                {
                    return Path.Combine(typeDir, "*.parquet");
                }
            }

            return null;
        }

        private static string ResolveDataRootPath()
        {
            try
            {
                var project = Project.Current;
                if (project != null && !string.IsNullOrWhiteSpace(project.HomeFolderPath))
                {
                    return Path.Combine(project.HomeFolderPath, AddinDataSubfolder, "Data");
                }

                if (project != null && !string.IsNullOrWhiteSpace(project.Path))
                {
                    string projectDir = Path.GetDirectoryName(project.Path);
                    if (!string.IsNullOrWhiteSpace(projectDir))
                    {
                        return Path.Combine(projectDir, AddinDataSubfolder, "Data");
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"ResolveDataRootPath failed: {ex.Message}");
            }

            return null;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            _dataProcessor.Dispose();
        }
    }
}
