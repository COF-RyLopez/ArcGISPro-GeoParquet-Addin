using ArcGIS.Core.CIM;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using ArcGIS.Core.Geometry;
using System;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Linq;
using System.Data;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using Microsoft.Win32;
using ArcGIS.Desktop.Catalog;
using System.Net.Http;
using System.Text.Json;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Threading;
using ArcGIS.Desktop.Core;
using System.Net;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;


namespace DuckDBGeoparquet.Views
{
    internal partial class WizardDockpaneViewModel : DockPane
    {
        #region Commands
        public ICommand LoadDataCommand { get; private set; }
        public ICommand ShowThemeInfoCommand { get; private set; }
        public ICommand SetCustomExtentCommand { get; private set; }
        public ICommand BrowseMfcLocationCommand { get; private set; }
        public ICommand BrowseDataLocationCommand { get; private set; }
        public ICommand BrowseCustomDataFolderCommand { get; private set; }
        public ICommand CreateMfcCommand { get; private set; }
        public ICommand GoToCreateMfcTabCommand { get; private set; }
        public ICommand ApplyManualReleaseCommand { get; private set; }
        public ICommand CancelCommand { get; private set; }
        public ICommand SelectAllCommand { get; private set; }
        #endregion
    }
}
