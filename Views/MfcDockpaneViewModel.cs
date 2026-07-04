using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows.Forms;
using DuckDBGeoparquet.Services;

namespace DuckDBGeoparquet.Views
{
    internal class MfcDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_MfcDockpane";
        private const string DefaultMfcFileName = "OvertureData.mfc";

        protected MfcDockpaneViewModel() {
            BrowseCustomDataFolderCommand = new RelayCommand(() => BrowseCustomDataFolder());
            BrowseMfcLocationCommand = new RelayCommand(() => BrowseMfcLocation());
            CreateMfcCommand = new RelayCommand(async () => await CreateMfcAsync(), () => CanCreateMfc());

            // Default output under the add-in's data base, matching the wizard
            // (<project>\OvertureProAddinData\Connections, MyDocuments fallback).
            MfcOutputPath = Path.Combine(ProjectDataLocator.GetAddinDataBase(), "Connections");
        }

        protected override Task InitializeAsync()
        {
            return base.InitializeAsync();
        }

        internal static void Show()
        {
            DockPane pane = FrameworkApplication.DockPaneManager.Find(_dockPaneID);
            if (pane == null)
                return;

            pane.Activate();
        }

        private bool _usePreviouslyLoadedData = true;
        public bool UsePreviouslyLoadedData
        {
            get => _usePreviouslyLoadedData;
            set
            {
                SetProperty(ref _usePreviouslyLoadedData, value);
                NotifyPropertyChanged(nameof(UseCustomDataFolder));
                ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
            }
        }

        public bool UseCustomDataFolder
        {
            get => !_usePreviouslyLoadedData;
            set
            {
                if (_usePreviouslyLoadedData == value)
                {
                    _usePreviouslyLoadedData = !value;
                    NotifyPropertyChanged(nameof(UsePreviouslyLoadedData));
                    NotifyPropertyChanged(nameof(UseCustomDataFolder));
                    ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
                }
            }
        }

        private string _customDataFolderPath;
        public string CustomDataFolderPath
        {
            get => _customDataFolderPath;
            set
            {
                SetProperty(ref _customDataFolderPath, value);
                ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
            }
        }

        private string _mfcOutputPath;
        public string MfcOutputPath
        {
            get => _mfcOutputPath;
            set
            {
                SetProperty(ref _mfcOutputPath, value);
                ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
            }
        }

        private bool _isCreatingMfc;
        public bool IsCreatingMfc
        {
            get => _isCreatingMfc;
            set => SetProperty(ref _isCreatingMfc, value);
        }

        private string _statusText = "Ready";
        public string StatusText
        {
            get => _statusText;
            set => SetProperty(ref _statusText, value);
        }

        private string _mfcLogText = string.Empty;
        public string MfcLogText
        {
            get => _mfcLogText;
            set => SetProperty(ref _mfcLogText, value);
        }

        public ICommand BrowseCustomDataFolderCommand { get; }
        public ICommand BrowseMfcLocationCommand { get; }
        public ICommand CreateMfcCommand { get; }

        private bool CanCreateMfc()
        {
            if (IsCreatingMfc) return false;
            if (UseCustomDataFolder && string.IsNullOrWhiteSpace(CustomDataFolderPath)) return false;
            if (string.IsNullOrWhiteSpace(MfcOutputPath)) return false;
            return true;
        }

        private void BrowseCustomDataFolder()
        {
            using (var dialog = new FolderBrowserDialog())
            {
                dialog.Description = "Select Folder Containing GeoParquet Files";
                dialog.UseDescriptionForTitle = true;
                if (!string.IsNullOrEmpty(CustomDataFolderPath) && Directory.Exists(CustomDataFolderPath))
                {
                    dialog.SelectedPath = CustomDataFolderPath;
                }
                
                if (dialog.ShowDialog() == DialogResult.OK)
                {
                    CustomDataFolderPath = dialog.SelectedPath;
                }
            }
        }

        private void BrowseMfcLocation()
        {
            using (var dialog = new FolderBrowserDialog())
            {
                dialog.Description = "Select Output Folder for MFC";
                dialog.UseDescriptionForTitle = true;
                if (!string.IsNullOrEmpty(MfcOutputPath) && Directory.Exists(MfcOutputPath))
                {
                    dialog.SelectedPath = MfcOutputPath;
                }
                
                if (dialog.ShowDialog() == DialogResult.OK)
                {
                    MfcOutputPath = dialog.SelectedPath;
                }
            }
        }

        private async Task CreateMfcAsync()
        {
            var logBuilder = new StringBuilder();

            void LogMessage(string message)
            {
                if (string.IsNullOrWhiteSpace(message))
                    return;

                System.Diagnostics.Debug.WriteLine(message);
                if (logBuilder.Length > 0)
                    logBuilder.AppendLine();

                logBuilder.Append(message);
                MfcLogText = logBuilder.ToString();
            }

            try
            {
                IsCreatingMfc = true;
                ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
                StatusText = "Creating Multifile Feature Connection (MFC)...";
                MfcLogText = string.Empty;

                string sourceDataPath;
                if (UsePreviouslyLoadedData)
                {
                    // Newest release folder under <project>\OvertureProAddinData\Data —
                    // the folder that directly holds the per-type subfolders the MFC needs.
                    sourceDataPath = ProjectDataLocator.GetNewestLoadedReleaseFolder();
                }
                else
                {
                    sourceDataPath = CustomDataFolderPath;
                }

                if (string.IsNullOrWhiteSpace(sourceDataPath) || !Directory.Exists(sourceDataPath))
                {
                    throw new DirectoryNotFoundException($"Source data directory not found: {sourceDataPath}");
                }

                string outputMfcFilePath = ResolveMfcFilePath(MfcOutputPath);
                string outputDirectory = Path.GetDirectoryName(outputMfcFilePath);
                if (string.IsNullOrWhiteSpace(outputDirectory))
                {
                    throw new DirectoryNotFoundException($"MFC output directory could not be resolved from: {MfcOutputPath}");
                }

                if (!Directory.Exists(outputDirectory))
                {
                    Directory.CreateDirectory(outputDirectory);
                }

                string addinExecutingPath = GetAddinExecutingPath();
                LogMessage($"Source data folder: {sourceDataPath}");
                LogMessage($"Output MFC file: {outputMfcFilePath}");

                bool created = await MfcUtility.GenerateMfcFileAsync(
                    sourceDataPath,
                    outputMfcFilePath,
                    addinExecutingPath,
                    LogMessage);

                if (!created)
                {
                    StatusText = "MFC creation failed. Review the log details below.";
                }
                else
                {
                    StatusText = $"MFC created successfully: {outputMfcFilePath}";
                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                        $"Multifile Feature Connection created successfully:\n{outputMfcFilePath}",
                        "Success");
                }
            }
            catch (Exception ex)
            {
                StatusText = $"Error creating MFC: {ex.Message}";
                LogMessage(ex.ToString());
                System.Diagnostics.Debug.WriteLine(ex);
            }
            finally
            {
                IsCreatingMfc = false;
                ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
            }
        }

        private static string ResolveMfcFilePath(string outputPath)
        {
            string trimmedPath = outputPath?.Trim();
            if (string.IsNullOrWhiteSpace(trimmedPath))
                return trimmedPath;

            return string.Equals(Path.GetExtension(trimmedPath), ".mfc", StringComparison.OrdinalIgnoreCase)
                ? trimmedPath
                : Path.Combine(trimmedPath, DefaultMfcFileName);
        }

        private static string GetAddinExecutingPath()
        {
            string assemblyLocation = Assembly.GetExecutingAssembly().Location;
            string assemblyDirectory = string.IsNullOrWhiteSpace(assemblyLocation)
                ? null
                : Path.GetDirectoryName(assemblyLocation);

            return string.IsNullOrWhiteSpace(assemblyDirectory)
                ? AppDomain.CurrentDomain.BaseDirectory
                : assemblyDirectory;
        }
    }

    /// <summary>
    /// Button implementation to show the DockPane.
    /// </summary>
    internal class MfcDockpane_ShowButton : ArcGIS.Desktop.Framework.Contracts.Button
    {
        protected override void OnClick()
        {
            MfcDockpaneViewModel.Show();
        }
    }
}
