using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDB.NET.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Service for matching user transportation data to Overture Maps data and assigning GERS IDs
    /// </summary>
    public class OvertureGERSMatcher : IDisposable
    {
        public class MatchConfiguration
        {
            /// <summary>
            /// Maximum distance in meters for spatial matching
            /// </summary>
            public double SpatialToleranceMeters { get; set; } = 50.0;
            
            /// <summary>
            /// Minimum confidence score (0.0 to 1.0) to consider a match valid
            /// </summary>
            public double MinimumConfidenceScore { get; set; } = 0.7;
            
            /// <summary>
            /// Weight for spatial similarity in overall confidence score
            /// </summary>
            public double SpatialWeight { get; set; } = 0.6;
            
            /// <summary>
            /// Weight for attribute similarity in overall confidence score
            /// </summary>
            public double AttributeWeight { get; set; } = 0.4;
            
            /// <summary>
            /// Include manual review candidates (matches below minimum confidence)
            /// </summary>
            public bool IncludeManualReviewCandidates { get; set; } = true;
            
            /// <summary>
            /// Attribute fields to compare between datasets
            /// </summary>
            public List<string> ComparisonAttributes { get; set; } = new List<string>
            {
                "class", "name", "speed_limit", "surface", "lanes"
            };
        }

        public class MatchResult
        {
            public bool Success { get; set; }
            public string Error { get; set; }
            public int TotalUserFeatures { get; set; }
            public int TotalOvertureFeatures { get; set; }
            public int ExactMatches { get; set; }
            public int PartialMatches { get; set; }
            public int ConflatedMatches { get; set; }
            public int SplitMatches { get; set; }
            public int UnmatchedUser { get; set; }
            public int UnmatchedOverture { get; set; }
            public List<FeatureMatch> Matches { get; set; } = new List<FeatureMatch>();
            public List<string> ProcessingLog { get; set; } = new List<string>();
            public string OutputPath { get; set; }
        }

        public class FeatureMatch
        {
            public long UserFeatureId { get; set; }
            public string UserFeatureName { get; set; }
            public string OvertureGERSId { get; set; }
            public string OvertureFeatureId { get; set; }
            public MatchType Type { get; set; }
            public double ConfidenceScore { get; set; }
            public double SpatialSimilarity { get; set; }
            public double AttributeSimilarity { get; set; }
            public Dictionary<string, object> UserAttributes { get; set; } = new Dictionary<string, object>();
            public Dictionary<string, object> OvertureAttributes { get; set; } = new Dictionary<string, object>();
            public string MatchReason { get; set; }
            public bool RequiresManualReview { get; set; }
        }

        public enum MatchType
        {
            Exact,      // 1:1 perfect match
            Partial,    // 1:1 with attribute differences
            Conflated,  // Many user features → 1 Overture feature
            Split,      // 1 user feature → many Overture features
            Unmatched   // No suitable match found
        }

        public class UserFeature
        {
            public long Id { get; set; }
            public string Name { get; set; }
            public Polyline Geometry { get; set; }
            public Dictionary<string, object> Attributes { get; set; } = new Dictionary<string, object>();
        }

        public class OvertureFeature
        {
            public string Id { get; set; }
            public string GERSId { get; set; }
            public Polyline Geometry { get; set; }
            public Dictionary<string, object> Attributes { get; set; } = new Dictionary<string, object>();
        }

        private readonly List<string> _processingLog = new List<string>();
        private DuckDBConnection _duckDBConnection;

        /// <summary>
        /// Main entry point for GERS ID matching process
        /// </summary>
        public async Task<MatchResult> MatchToOvertureAsync(
            string userDataPath, 
            string overtureDataPath, 
            string outputPath, 
            MatchConfiguration config = null, 
            IProgress<string> progress = null)
        {
            var result = new MatchResult();
            _processingLog.Clear();

            try
            {
                config ??= new MatchConfiguration();
                LogMessage("🚀 Starting GERS ID matching process...");
                progress?.Report("🚀 Starting GERS ID matching process...");

                await QueuedTask.Run(async () =>
                {
                    try
                    {
                        // Step 1: Load user data
                        LogMessage("📊 Loading user transportation data...");
                        progress?.Report("📊 Loading user transportation data...");
                        var userFeatures = await LoadUserDataAsync(userDataPath, progress);
                        result.TotalUserFeatures = userFeatures.Count;
                        LogMessage($"✅ Loaded {userFeatures.Count:N0} user features");

                        // Step 2: Load Overture reference data
                        LogMessage("🗺️ Loading Overture Maps reference data...");
                        progress?.Report("🗺️ Loading Overture Maps reference data...");
                        var overtureFeatures = await LoadOvertureReferenceDataAsync(overtureDataPath, progress);
                        result.TotalOvertureFeatures = overtureFeatures.Count;
                        LogMessage($"✅ Loaded {overtureFeatures.Count:N0} Overture features");

                        // Step 3: Create spatial index for efficient matching
                        LogMessage("🔍 Building spatial index for matching...");
                        progress?.Report("🔍 Building spatial index for matching...");
                        var spatialIndex = BuildSpatialIndex(overtureFeatures);
                        
                        // Step 4: Perform spatial and attribute matching
                        LogMessage("🎯 Performing spatial and attribute matching...");
                        progress?.Report("🎯 Performing spatial and attribute matching...");
                        result.Matches = await PerformMatchingAsync(userFeatures, overtureFeatures, spatialIndex, config, progress);

                        // Step 5: Analyze and categorize results
                        LogMessage("📈 Analyzing match results...");
                        progress?.Report("📈 Analyzing match results...");
                        AnalyzeMatchResults(result);

                        // Step 6: Save enhanced data with GERS IDs
                        LogMessage("💾 Saving enhanced data with GERS IDs...");
                        progress?.Report("💾 Saving enhanced data with GERS IDs...");
                        await SaveEnhancedDataAsync(userFeatures, result.Matches, outputPath);
                        result.OutputPath = outputPath;

                        result.Success = true;
                        LogMessage("🎉 GERS ID matching completed successfully!");
                        progress?.Report("🎉 GERS ID matching completed successfully!");
                    }
                    catch (Exception ex)
                    {
                        result.Error = ex.Message;
                        LogMessage($"❌ Error during matching: {ex.Message}");
                        progress?.Report($"❌ Error: {ex.Message}");
                    }
                });

                result.ProcessingLog = new List<string>(_processingLog);
                return result;
            }
            catch (Exception ex)
            {
                result.Error = $"GERS matching failed: {ex.Message}";
                result.ProcessingLog = new List<string>(_processingLog);
                progress?.Report($"❌ GERS matching failed: {ex.Message}");
                return result;
            }
        }

        // TODO: Implement core matching algorithms
        private async Task<List<UserFeature>> LoadUserDataAsync(string userDataPath, IProgress<string> progress = null)
        {
            // Implementation coming next
            return new List<UserFeature>();
        }

        private async Task<List<OvertureFeature>> LoadOvertureReferenceDataAsync(string overtureDataPath, IProgress<string> progress = null)
        {
            // Implementation coming next
            return new List<OvertureFeature>();
        }

        private Dictionary<string, List<OvertureFeature>> BuildSpatialIndex(List<OvertureFeature> overtureFeatures)
        {
            // Implementation coming next
            return new Dictionary<string, List<OvertureFeature>>();
        }

        private async Task<List<FeatureMatch>> PerformMatchingAsync(
            List<UserFeature> userFeatures, 
            List<OvertureFeature> overtureFeatures,
            Dictionary<string, List<OvertureFeature>> spatialIndex,
            MatchConfiguration config, 
            IProgress<string> progress = null)
        {
            // Implementation coming next
            return new List<FeatureMatch>();
        }

        private void AnalyzeMatchResults(MatchResult result)
        {
            // Implementation coming next
        }

        private async Task SaveEnhancedDataAsync(List<UserFeature> userFeatures, List<FeatureMatch> matches, string outputPath)
        {
            // Implementation coming next
        }

        private void LogMessage(string message)
        {
            var timestamp = DateTime.Now.ToString("HH:mm:ss");
            var logEntry = $"[{timestamp}] {message}";
            _processingLog.Add(logEntry);
            System.Diagnostics.Debug.WriteLine(logEntry);
        }

        public void Dispose()
        {
            _duckDBConnection?.Dispose();
        }
    }
} 