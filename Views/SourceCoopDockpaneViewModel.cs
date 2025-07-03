using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace DuckDBGeoparquet.Views
{
    internal class SourceCoopDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_SourceCoopDockpane";

        private string _statusText = "Source Cooperative integration coming soon...";
        public string StatusText
        {
            get => _statusText;
            set => SetProperty(ref _statusText, value);
        }

        protected bool SetProperty<T>(ref T field, T value, [CallerMemberName] string propertyName = null)
        {
            if (Equals(field, value)) return false;
            field = value;
            OnPropertyChanged(propertyName);
            return true;
        }

        internal static void Show()
        {
            var pane = FrameworkApplication.DockPaneManager.Find(_dockPaneID);
            pane?.Activate();
        }
    }
} 