using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows.Forms;
using DuckDBGeoparquet.Services;
using ArcGIS.Desktop.Core;

namespace DuckDBGeoparquet.Views
{
    internal class MfcDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_MfcDockpane";

        protected MfcDockpaneViewModel() {
            BrowseCustomDataFolderCommand = new RelayCommand(() => BrowseCustomDataFolder());
            BrowseMfcLocationCommand = new RelayCommand(() => BrowseMfcLocation());
            CreateMfcCommand = new RelayCommand(async () => await CreateMfcAsync(), () => CanCreateMfc());

            // Initialize default output
            var defaultBasePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "ArcGIS", "Projects", "OvertureProAddinData");
            MfcOutputPath = Path.Combine(defaultBasePath, "Connections");
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
            try
            {
                IsCreatingMfc = true;
                ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
                StatusText = "Creating Multifile Feature Connection (MFC)...";

                string sourceDataPath = string.Empty;
                if (UsePreviouslyLoadedData)
                {
                    var defaultBasePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "ArcGIS", "Projects", "OvertureProAddinData");
                    sourceDataPath = Path.Combine(defaultBasePath, "Data", "latest");
                    if (!Directory.Exists(sourceDataPath))
                    {
                        sourceDataPath = Path.Combine(defaultBasePath, "Data");
                    }
                }
                else
                {
                    sourceDataPath = CustomDataFolderPath;
                }

                if (string.IsNullOrWhiteSpace(sourceDataPath) || !Directory.Exists(sourceDataPath))
                {
                    throw new DirectoryNotFoundException($"Source data directory not found: {sourceDataPath}");
                }

                if (!Directory.Exists(MfcOutputPath))
                {
                    Directory.CreateDirectory(MfcOutputPath);
                }

                // Call CreateBdc gp tool
                var parameters = Geoprocessing.MakeValueArray(sourceDataPath, MfcOutputPath, "OvertureData");
                var result = await Geoprocessing.ExecuteToolAsync("management.CreateBdc", parameters);
                
                if (result.IsFailed)
                {
                    StatusText = $"MFC Creation Failed: {string.Join(", ", result.Messages)}";
                }
                else
                {
                    StatusText = "MFC created successfully in the output folder.";
                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show("Multifile Feature Connection created successfully.", "Success");
                }
            }
            catch (Exception ex)
            {
                StatusText = $"Error creating MFC: {ex.Message}";
                System.Diagnostics.Debug.WriteLine(ex);
            }
            finally
            {
                IsCreatingMfc = false;
                ((RelayCommand)CreateMfcCommand).RaiseCanExecuteChanged();
            }
        }
    }

    /// <summary>
    /// Button implementation to show the DockPane.
    /// </summary>
    internal class MfcDockpane_ShowButton : Button
    {
        protected override void OnClick()
        {
            MfcDockpaneViewModel.Show();
        }
    }
}
