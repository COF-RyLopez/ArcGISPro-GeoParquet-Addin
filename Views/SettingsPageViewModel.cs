using ArcGIS.Desktop.Framework.Contracts;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// GeoParquet Settings property page (proxy and DuckDB memory limits).
    /// Values load from AddinSettingsStore when the sheet opens and save once,
    /// on OK — replacing the wizard's old per-keystroke persistence.
    /// </summary>
    internal class SettingsPageViewModel : Page
    {
        private string _httpProxy;
        private string _httpProxyUsername;
        private string _httpProxyPassword;
        private string _memoryLimit;
        private string _maxMemory;
        private string _detectedProxyDisplay;

        public SettingsPageViewModel()
        {
            var settings = AddinSettingsStore.Load();
            _httpProxy = settings.HttpProxy ?? string.Empty;
            _httpProxyUsername = settings.HttpProxyUsername ?? string.Empty;
            _httpProxyPassword = settings.HttpProxyPassword ?? string.Empty;
            _memoryLimit = settings.EffectiveMemoryLimit;
            _maxMemory = settings.EffectiveMaxMemory;
            RefreshDetectedProxyDisplay(settings);
        }

        public string HttpProxy { get => _httpProxy; set => SetModified(ref _httpProxy, value); }
        public string HttpProxyUsername { get => _httpProxyUsername; set => SetModified(ref _httpProxyUsername, value); }
        /// <summary>Never log this value.</summary>
        public string HttpProxyPassword { get => _httpProxyPassword; set => SetModified(ref _httpProxyPassword, value); }
        public string MemoryLimit { get => _memoryLimit; set => SetModified(ref _memoryLimit, value); }
        public string MaxMemory { get => _maxMemory; set => SetModified(ref _maxMemory, value); }

        public string DetectedProxyDisplay
        {
            get => _detectedProxyDisplay;
            private set => SetProperty(ref _detectedProxyDisplay, value);
        }

        private void SetModified(ref string field, string value, [CallerMemberName] string propertyName = null)
        {
            value ??= string.Empty;
            if (field == value) return;
            field = value;
            IsModified = true;
            NotifyPropertyChanged(propertyName);
        }

        /// <summary>OK/Apply: save once, refresh the HTTP client and any live engine.</summary>
        protected override Task CommitAsync()
        {
            var settings = new AddinSettings
            {
                HttpProxy = HttpProxy,
                HttpProxyUsername = HttpProxyUsername,
                HttpProxyPassword = HttpProxyPassword,
                MemoryLimit = MemoryLimit,
                MaxMemory = MaxMemory
            };
            AddinSettingsStore.Save(settings);
            ReleaseValidator.RefreshHttpClient();
            RefreshDetectedProxyDisplay(settings);
            IsModified = false;
            return WizardDockpaneViewModel.ReapplyEngineSettingsAsync();
        }

        private void RefreshDetectedProxyDisplay(AddinSettings settings)
        {
            try
            {
                var resolved = DuckDBManager.ResolveProxy(settings);
                DetectedProxyDisplay = resolved.IsConfigured
                    ? resolved.ToLogSafeDescription()
                    : "No proxy (direct connection, or leave blank to auto-detect the Windows system proxy)";
            }
            catch (Exception ex)
            {
                DetectedProxyDisplay = "Windows system proxy (auto-detect)";
                System.Diagnostics.Debug.WriteLine($"Proxy display refresh failed: {ex.Message}");
            }
        }
    }
}
