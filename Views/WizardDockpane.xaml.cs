using Microsoft.Web.WebView2.Core;
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;

namespace DuckDBGeoparquet.Views
{
    public partial class WizardDockpaneView : UserControl
    {
        private bool _previewInitStarted;

        public WizardDockpaneView()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Event handler for the log text box to scroll to the end whenever text changes
        /// </summary>
        private void LogTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            // Automatically scroll to the end when text changes
            if (sender is TextBox textBox)
            {
                textBox.ScrollToEnd();
            }
        }

        private void Hyperlink_RequestNavigate(object sender, RequestNavigateEventArgs e)
        {
            Process.Start(new ProcessStartInfo(e.Uri.AbsoluteUri) { UseShellExecute = true });
            e.Handled = true;
        }

        /// <summary>
        /// Initializes the WebView2 control hosting the ArcGIS JS preview map.
        /// Runs once, the first time the Preview tab's WebView2 is loaded.
        /// </summary>
        private async void PreviewWebView_Loaded(object sender, RoutedEventArgs e)
        {
            if (_previewInitStarted)
                return;
            _previewInitStarted = true;

            var viewModel = DataContext as WizardDockpaneViewModel;
            try
            {
                // The add-in runs from the read-only assembly cache, so WebView2
                // needs an explicit writable user-data folder.
                string userDataFolder = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "DuckDBGeoparquet", "WebView2");
                var environment = await CoreWebView2Environment.CreateAsync(null, userDataFolder);
                await PreviewWebView.EnsureCoreWebView2Async(environment);

                var core = PreviewWebView.CoreWebView2;

                // Serve the bundled preview page over a virtual host so it has a
                // proper https origin (file:// origins break web messaging and fetches).
                string assetFolder = Path.Combine(
                    Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                    "Views", "PreviewMap");
                core.SetVirtualHostNameToFolderMapping(
                    "appassets", assetFolder, CoreWebView2HostResourceAccessKind.Allow);

                core.WebMessageReceived += (s, args) =>
                {
                    if (DataContext is WizardDockpaneViewModel vm)
                        vm.PreviewBridge?.HandleWebMessage(args.TryGetWebMessageAsString());
                };

                viewModel?.AttachPreview(
                    postMessage: json => PreviewWebView.CoreWebView2?.PostWebMessageAsString(json));

                core.Navigate("https://appassets/preview.html");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Preview WebView2 initialization failed: {ex}");
                viewModel?.NotifyPreviewInitFailed(ex.Message);
            }
        }
    }
}
