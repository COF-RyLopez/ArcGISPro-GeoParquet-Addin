using System;
using System.IO;
using System.Threading.Tasks;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using System.Collections.Generic;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Utility class for creating and managing Multifile Feature Connections (MFC)
    /// for Overture Maps data
    /// </summary>
    public class MfcUtility
    {
        /// <summary>
        /// Default base folder for Overture Maps MFC data
        /// Uses the current ArcGIS Pro project folder if available, or falls back to Documents
        /// </summary>
        public static string DefaultMfcBasePath
        {
            get
            {
                // Try to get the current project path first
                try
                {
                    var project = Project.Current;
                    if (project != null && !string.IsNullOrEmpty(project.Path))
                    {
                        string projectFolder = Path.GetDirectoryName(project.Path);
                        return Path.Combine(projectFolder, "OvertureMapsMFC");
                    }
                }
                catch (Exception ex)
                {
                    // Log error and fall back to Documents folder
                    System.Diagnostics.Debug.WriteLine($"Error getting project path: {ex.Message}");
                }

                // Fall back to Documents folder if no project is open
                return Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
                    "OvertureMapsMFC"
                );
            }
        }

        /// <summary>
        /// Creates a properly structured folder hierarchy for Overture Maps data
        /// based on the MFC requirements
        /// </summary>
        /// <param name="basePath">Base folder path (optional)</param>
        /// <param name="releaseDate">Release date of Overture Maps data</param>
        /// <returns>The root path of the created MFC structure</returns>
        public static string CreateMfcFolderStructure(string basePath, string releaseDate)
        {
            // Use default path if none specified
            if (string.IsNullOrEmpty(basePath))
            {
                basePath = Path.Combine(DefaultMfcBasePath, "Data");
            }

            // Create the release folder path
            string releasePath = Path.Combine(basePath, releaseDate);

            // Create base directory if it doesn't exist
            if (!Directory.Exists(basePath))
            {
                Directory.CreateDirectory(basePath);
            }

            // Create release directory if it doesn't exist
            if (!Directory.Exists(releasePath))
            {
                Directory.CreateDirectory(releasePath);
            }

            return releasePath;
        }

        /// <summary>
        /// Creates theme subfolders within the release folder
        /// </summary>
        /// <param name="releasePath">Path to the release folder</param>
        /// <param name="themes">List of themes to create folders for</param>
        public static void CreateThemeFolders(string releasePath, IEnumerable<string> themes)
        {
            foreach (var theme in themes)
            {
                string themePath = Path.Combine(releasePath, theme);
                if (!Directory.Exists(themePath))
                {
                    Directory.CreateDirectory(themePath);
                }
            }
        }

        /// <summary>
        /// Creates an MFC file for the specified release folder
        /// </summary>
        /// <param name="sourceFolderPath">Path to the folder containing theme subfolders</param>
        /// <param name="outputMfcPath">Path where the MFC file should be created</param>
        /// <param name="isShared">Whether the connection should be shared (true) or standalone (false)</param>
        /// <returns>True if the MFC was created successfully</returns>
        public static async Task<bool> CreateMfcAsync(string sourceFolderPath, string outputMfcPath, bool isShared = true)
        {
            try
            {
                // Set connection type
                string connectionType = isShared ? "SHARED" : "STAND_ALONE";

                // Create parameters for the GP tool
                var parameters = Geoprocessing.MakeValueArray(
                    sourceFolderPath,          // Input folder containing theme subfolders
                    outputMfcPath,             // Output .mfc file location
                    connectionType,            // Connection type
                    "YES_REMOVE_INVALID"       // Remove invalid schemas option
                );

                // Execute the GP tool
                var result = await Geoprocessing.ExecuteToolAsync(
                    "GeoAnalyticsBigData.CreateMultifileFeatureConnection",
                    parameters
                );

                return result.IsFailed != true;
            }
            catch (Exception ex)
            {
                // Log the error
                System.Diagnostics.Debug.WriteLine($"Error creating MFC: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Refreshes an existing MFC to incorporate new or updated data
        /// </summary>
        /// <param name="mfcPath">Path to the existing MFC file</param>
        /// <returns>True if the refresh was successful</returns>
        public static async Task<bool> RefreshMfcAsync(string mfcPath)
        {
            try
            {
                // Create parameters for the GP tool
                var parameters = Geoprocessing.MakeValueArray(mfcPath);

                // Execute the GP tool
                var result = await Geoprocessing.ExecuteToolAsync(
                    "GeoAnalyticsBigData.RefreshMultifileFeatureConnection",
                    parameters
                );

                return result.IsFailed != true;
            }
            catch (Exception ex)
            {
                // Log the error
                System.Diagnostics.Debug.WriteLine($"Error refreshing MFC: {ex.Message}");
                return false;
            }
        }
    }
}