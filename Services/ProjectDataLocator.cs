using ArcGIS.Desktop.Core;
using DuckDBGeoparquet.Models;
using System;
using System.IO;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Single source of truth for where the add-in stores loaded data, replacing
    /// three drifted copies (the wizard's default MFC base path, the geocoder's
    /// data-root lookup, and the MFC dockpane's source path). The shared piece is
    /// the ArcGIS project-root lookup; each caller keeps its own fallback:
    /// the wizard/MFC fall back to MyDocuments, the geocoder treats "no project"
    /// as null.
    /// </summary>
    public static class ProjectDataLocator
    {
        /// <summary>
        /// The current project's root — its Home Folder, or the folder holding
        /// the .aprx if no Home Folder is set. Null when no project is open (or
        /// the lookup throws), so callers can choose their own fallback.
        /// </summary>
        public static string GetProjectRoot()
        {
            try
            {
                var project = Project.Current;
                if (project != null && !string.IsNullOrWhiteSpace(project.HomeFolderPath))
                {
                    return project.HomeFolderPath;
                }
                if (project != null && !string.IsNullOrWhiteSpace(project.Path))
                {
                    string projectDir = Path.GetDirectoryName(project.Path);
                    if (!string.IsNullOrWhiteSpace(projectDir))
                        return projectDir;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"ProjectDataLocator.GetProjectRoot failed: {ex.Message}");
            }
            return null;
        }

        /// <summary>
        /// The add-in's data base folder (<c>&lt;root&gt;\OvertureProAddinData</c>),
        /// falling back to MyDocuments when no project is open.
        /// </summary>
        public static string GetAddinDataBase()
        {
            string root = GetProjectRoot()
                ?? Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
            return Path.Combine(root, AddinConstants.DataSubfolder);
        }

        /// <summary>
        /// The directory holding per-release data folders
        /// (<c>&lt;root&gt;\OvertureProAddinData\Data</c>).
        /// </summary>
        public static string GetDataDirectory() => Path.Combine(GetAddinDataBase(), "Data");

        /// <summary>
        /// The most recently loaded release folder under <see cref="GetDataDirectory"/>,
        /// or null if nothing has been loaded. This is the folder CreateBdc needs
        /// (it directly contains the per-type subfolders).
        /// </summary>
        public static string GetNewestLoadedReleaseFolder() =>
            OvertureDataPaths.ResolveNewestReleaseFolder(GetDataDirectory());
    }
}
