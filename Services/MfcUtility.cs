using System;
using System.IO;
using System.Threading.Tasks;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

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
        /// <param name="dataSourceFolder">Path to the folder containing the data files</param>
        /// <param name="outputMfcPath">Path where the MFC file should be created</param>
        /// <param name="isShared">Whether the connection should be shared (true) or standalone (false)</param>
        /// <param name="geometryVisible">Whether geometry fields should be visible in analysis (default true)</param>
        /// <param name="timeVisible">Whether time fields should be visible in analysis (default true)</param>
        /// <returns>True if the MFC was created successfully</returns>
        public static async Task<bool> CreateMfcAsync(
            string dataSourceFolder,
            string outputMfcPath,
            bool isShared = true,
            bool geometryVisible = true,
            bool timeVisible = true)
        {
            try
            {
                // Debug the data source folder structure first
                System.Diagnostics.Debug.WriteLine($"Checking structure of data source folder: {dataSourceFolder}");
                if (!Directory.Exists(dataSourceFolder))
                {
                    System.Diagnostics.Debug.WriteLine($"ERROR: Data source folder does not exist: {dataSourceFolder}");
                    throw new DirectoryNotFoundException($"Data source folder does not exist: {dataSourceFolder}");
                }

                // Log the contents of the data source folder to help debug
                var files = Directory.GetFiles(dataSourceFolder, "*.parquet");
                System.Diagnostics.Debug.WriteLine($"Found {files.Length} parquet files in data source folder");
                foreach (var file in files.Take(5))  // Just log the first 5 to avoid too much output
                {
                    System.Diagnostics.Debug.WriteLine($"Found file: {Path.GetFileName(file)}");
                }
                if (files.Length > 5)
                {
                    System.Diagnostics.Debug.WriteLine($"... and {files.Length - 5} more files");
                }

                // Get output folder and output name
                string bdcLocation = Path.GetDirectoryName(outputMfcPath);
                string bdcName = Path.GetFileNameWithoutExtension(outputMfcPath);

                // Check and create output folder if needed
                if (!Directory.Exists(bdcLocation))
                {
                    System.Diagnostics.Debug.WriteLine($"Creating output folder: {bdcLocation}");
                    Directory.CreateDirectory(bdcLocation);
                }

                // Set geometry and time visibility strings
                string visibleGeometry = geometryVisible ? "GEOMETRY_VISIBLE" : "GEOMETRY_NOT_VISIBLE";
                string visibleTime = timeVisible ? "TIME_VISIBLE" : "TIME_NOT_VISIBLE";

                // Connection type is always FOLDER for file-based connections
                string connectionType = "FOLDER";

                // Set shared mode
                string sharedMode = isShared ? "SHARED" : "STANDALONE";

                // Create parameters for the GP tool in exact order matching ArcGIS Pro 3.5:
                // arcpy.gapro.CreateBDC(
                //     bdc_location=r"C:\...\Connection",
                //     bdc_name="OvertureMapsMFC",
                //     connection_type="FOLDER",
                //     data_source_folder=r"C:\...\Data\2025-04-23.0",
                //     visible_geometry="GEOMETRY_VISIBLE",
                //     visible_time="TIME_VISIBLE",
                //     sharing_mode="SHARED"
                // )
                var parameters = Geoprocessing.MakeValueArray(
                    bdcLocation,         // bdc_location
                    bdcName,             // bdc_name
                    connectionType,      // connection_type
                    dataSourceFolder,    // data_source_folder
                    visibleGeometry,     // visible_geometry
                    visibleTime,         // visible_time
                    sharedMode           // sharing_mode
                );

                // Log what we're doing
                System.Diagnostics.Debug.WriteLine($"Creating MFC at {outputMfcPath}");
                System.Diagnostics.Debug.WriteLine($"Source folder: {dataSourceFolder}");
                System.Diagnostics.Debug.WriteLine($"Parameters: {bdcLocation}, {bdcName}, {connectionType}, {dataSourceFolder}, {visibleGeometry}, {visibleTime}, {sharedMode}");

                // Try different geoprocessing tool names based on ArcGIS Pro version compatibility
                var result = await QueuedTask.Run(() =>
                {
                    // Use only the official gapro.CreateBDC tool as documented in ArcGIS Pro 3.5
                    System.Diagnostics.Debug.WriteLine("Executing gapro.CreateBDC");
                    return Geoprocessing.ExecuteToolAsync("gapro.CreateBDC", parameters);
                });

                if (result.IsFailed)
                {
                    // Log the error messages from the result's Messages collection
                    System.Diagnostics.Debug.WriteLine($"MFC creation failed");
                    foreach (var msg in result.Messages)
                    {
                        System.Diagnostics.Debug.WriteLine($"GP message: {msg.Text}");
                    }
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("MFC creation succeeded!");
                }

                return !result.IsFailed;
            }
            catch (Exception ex)
            {
                // Log the error
                System.Diagnostics.Debug.WriteLine($"Error creating MFC: {ex.Message}");
                System.Diagnostics.Debug.WriteLine($"Stack trace: {ex.StackTrace}");
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

                // Log what we're doing
                System.Diagnostics.Debug.WriteLine($"Refreshing MFC at {mfcPath}");

                // Execute the GP tool
                var result = await QueuedTask.Run(() =>
                    Geoprocessing.ExecuteToolAsync("RefreshMultifileFeatureConnection", parameters)
                );

                if (result.IsFailed)
                {
                    // Log the error messages from the result's Messages collection
                    System.Diagnostics.Debug.WriteLine($"MFC refresh failed");
                    foreach (var msg in result.Messages)
                    {
                        System.Diagnostics.Debug.WriteLine($"GP message: {msg.Text}");
                    }
                    return false;
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("MFC refresh succeeded!");
                    return true;
                }
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