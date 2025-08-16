using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using Microsoft.Win32;
using DuckDBGeoparquet.Services;

namespace DuckDBGeoparquet.Views
{
    internal class Ng911DockpaneViewModel : DockPane, INotifyPropertyChanged
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_Ng911Dockpane";

        private string _statusText = string.Empty;
        private double _progressValue;
        private string _logOutputText = string.Empty;
        private string _outputFolder = string.Empty;
        private bool _isTargetAddressPoints = true;
        private bool _isTargetRoadCenterlines = true;

        private RelayCommand _browseOutputFolderCommand;
        private RelayCommand _runMappingCommand;

        private DataProcessor _dataProcessor;

        protected override void OnShow(bool isVisible)
        {
            base.OnShow(isVisible);
        }

        protected override Task InitializeAsync()
        {
            _dataProcessor = new DataProcessor();
            return base.InitializeAsync();
        }

        public static void Show()
        {
            var pane = FrameworkApplication.FindDockPane(_dockPaneID) as Ng911DockpaneViewModel;
            pane?.Activate();
        }

        public string StatusText
        {
            get => _statusText;
            set { _statusText = value; NotifyPropertyChanged(); }
        }

        public double ProgressValue
        {
            get => _progressValue;
            set { _progressValue = value; NotifyPropertyChanged(); }
        }

        public string LogOutputText
        {
            get => _logOutputText;
            set { _logOutputText = value; NotifyPropertyChanged(); }
        }

        public string OutputFolder
        {
            get => _outputFolder;
            set { _outputFolder = value; NotifyPropertyChanged(); }
        }

        public bool IsTargetAddressPoints
        {
            get => _isTargetAddressPoints;
            set { _isTargetAddressPoints = value; NotifyPropertyChanged(); }
        }

        public bool IsTargetRoadCenterlines
        {
            get => _isTargetRoadCenterlines;
            set { _isTargetRoadCenterlines = value; NotifyPropertyChanged(); }
        }

        public RelayCommand BrowseOutputFolderCommand => _browseOutputFolderCommand ??= new RelayCommand(() =>
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog();
            var result = dialog.ShowDialog();
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                OutputFolder = dialog.SelectedPath;
            }
        });

        public RelayCommand RunMappingCommand => _runMappingCommand ??= new RelayCommand(async () =>
        {
            if (string.IsNullOrWhiteSpace(OutputFolder))
            {
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show("Please select an NG911 output folder.");
                return;
            }

            var mapper = new Ng911Mapper(_dataProcessor);
            var progress = new Progress<string>(msg => AddToLog(msg));

            if (IsTargetAddressPoints)
                await mapper.MapCurrentToNg911Async("SiteStructureAddressPoint", OutputFolder, progress);
            if (IsTargetRoadCenterlines)
                await mapper.MapCurrentToNg911Async("RoadCenterline", OutputFolder, progress);

            AddToLog("NG911 mapping completed.");
        });

        private void AddToLog(string message)
        {
            LogOutputText += (string.IsNullOrEmpty(LogOutputText) ? "" : Environment.NewLine) + message;
        }

        public event PropertyChangedEventHandler PropertyChanged;
        private void NotifyPropertyChanged([CallerMemberName] string propertyName = "")
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}


