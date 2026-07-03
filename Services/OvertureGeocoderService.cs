using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Models;
using static DuckDBGeoparquet.Models.AddinConstants;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public class OvertureGeocoderService : IDisposable
    {
        // Data subfolder constant is now in AddinConstants.DataSubfolder
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

        public async Task<List<GeocodeCandidate>> SearchAsync(string query, Envelope searchExtent = null, int maxResults = 25, CancellationToken cancellationToken = default)
        {
            await InitializeAsync();

            string normalizedQuery = GeocodeTextNormalizer.NormalizeForSearch(query);
            if (string.IsNullOrWhiteSpace(normalizedQuery))
            {
                return [];
            }

            if (PreferLocatorSearch && IsLocatorReady())
            {
                var locatorCandidates = await TryLocatorSearchAsync(query, searchExtent, maxResults);
                if (locatorCandidates.Count > 0)
                {
                    return locatorCandidates;
                }
                // No locator hits — fall through to the local DuckDB search.
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
                var addressCandidates = await _dataProcessor.SearchAddressCandidatesAsync(addressParquetGlob, normalizedQuery, searchExtent == null ? null : new ExtentBounds(searchExtent.XMin, searchExtent.YMin, searchExtent.XMax, searchExtent.YMax), perSourceLimit, cancellationToken);
                merged.AddRange(addressCandidates);
            }

            if (!string.IsNullOrWhiteSpace(placeParquetGlob))
            {
                var placeCandidates = await _dataProcessor.SearchPlaceCandidatesAsync(placeParquetGlob, normalizedQuery, searchExtent == null ? null : new ExtentBounds(searchExtent.XMin, searchExtent.YMin, searchExtent.XMax, searchExtent.YMax), perSourceLimit, cancellationToken);
                merged.AddRange(placeCandidates);
            }

            return merged
                .OrderByDescending(c => c.Score)
                .ThenBy(c => c.DisplayLabel, StringComparer.OrdinalIgnoreCase)
                .Take(Math.Clamp(maxResults, 1, 100))
                .ToList();
        }

        /// <summary>
        /// Geocodes a single query and returns the best candidate (or null).
        /// Used for file geocoding; goes through the same pipeline as
        /// interactive search, so it honors PreferLocatorSearch.
        /// </summary>
        public async Task<GeocodeCandidate> GeocodeBestAsync(string query, CancellationToken cancellationToken = default)
        {
            var results = await SearchAsync(query, null, 1, cancellationToken);
            return results.FirstOrDefault();
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

        /// <summary>
        /// Searches the map's registered locator providers (including the
        /// built hybrid locator) via LocatorManager.GeocodeAsync. Returns an
        /// empty list on any failure so callers fall back to the local
        /// DuckDB search.
        /// </summary>
        private static async Task<List<GeocodeCandidate>> TryLocatorSearchAsync(string query, Envelope searchExtent, int maxResults)
        {
            var candidates = new List<GeocodeCandidate>();
            try
            {
                var mapView = MapView.Active;
                if (mapView == null)
                {
                    return candidates;
                }

                var results = await mapView.LocatorManager.GeocodeAsync(query.Trim(), false, false);
                if (results == null)
                {
                    return candidates;
                }

                foreach (var result in results)
                {
                    var location = result.DisplayLocation;
                    if (location == null)
                    {
                        continue;
                    }

                    MapPoint wgsPoint = location;
                    if (location.SpatialReference != null && location.SpatialReference.Wkid != 4326)
                    {
                        wgsPoint = GeometryEngine.Instance.Project(location, SpatialReferences.WGS84) as MapPoint ?? location;
                    }

                    if (searchExtent != null &&
                        (wgsPoint.X < searchExtent.XMin || wgsPoint.X > searchExtent.XMax ||
                         wgsPoint.Y < searchExtent.YMin || wgsPoint.Y > searchExtent.YMax))
                    {
                        continue;
                    }

                    int score = (int)Math.Round(result.Score);
                    candidates.Add(new GeocodeCandidate
                    {
                        DisplayLabel = result.Label,
                        Longitude = wgsPoint.X,
                        Latitude = wgsPoint.Y,
                        SourceType = "Locator",
                        MatchTier = "locator",
                        Score = score,
                        ConfidenceTier = score >= 90 ? "High" : score >= 75 ? "Medium" : "Low"
                    });
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Locator search failed; falling back to local search: {ex.Message}");
            }

            return candidates
                .OrderByDescending(c => c.Score)
                .Take(Math.Clamp(maxResults, 1, 100))
                .ToList();
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
