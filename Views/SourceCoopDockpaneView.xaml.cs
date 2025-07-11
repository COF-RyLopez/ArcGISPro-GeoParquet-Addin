using System;
using System.Diagnostics;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using ArcGIS.Desktop.Framework;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Interaction logic for SourceCoopDockpaneView.xaml
    /// </summary>
    public partial class SourceCoopDockpaneView : UserControl
    {
        public SourceCoopDockpaneView()
        {
            try
            {
                Debug.WriteLine("🎯 SourceCoopDockpaneView constructor starting...");
                InitializeComponent();
                
                // Set the DataContext to the DockPane ViewModel
                // In ArcGIS Pro, the parent DockPane should be our ViewModel
                Loaded += SourceCoopDockpaneView_Loaded;
                
                Debug.WriteLine("✅ SourceCoopDockpaneView constructor completed");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"❌ SourceCoopDockpaneView constructor failed: {ex.Message}");
                Debug.WriteLine($"❌ Stack trace: {ex.StackTrace}");
            }
        }

        private void SourceCoopDockpaneView_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                Debug.WriteLine("🔍 SourceCoopDockpaneView_Loaded: Attempting to set DataContext...");
                
                // Try to find the DockPane ViewModel
                var dockPane = FrameworkApplication.DockPaneManager.Find("DuckDBGeoparquet_Views_SourceCoopDockpane");
                if (dockPane != null)
                {
                    Debug.WriteLine($"✅ Found DockPane: {dockPane.GetType().Name}");
                    this.DataContext = dockPane;
                    Debug.WriteLine("✅ DataContext set successfully");
                }
                else
                {
                    Debug.WriteLine("❌ Could not find DockPane with ID 'DuckDBGeoparquet_Views_SourceCoopDockpane'");
                    
                    // Alternative: try to get from parent - but SourceCoopDockpaneViewModel is not in the visual tree
                    // SourceCoopDockpaneViewModel inherits from DockPane, not a visual element
                    Debug.WriteLine("⚠️ Cannot find ViewModel in visual tree hierarchy (SourceCoopDockpaneViewModel is not a visual element)");
                }
                
                Debug.WriteLine($"🎯 Final DataContext: {(DataContext?.GetType().Name ?? "null")}");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"❌ Error setting DataContext: {ex.Message}");
            }
        }

        private void Hyperlink_RequestNavigate(object sender, RequestNavigateEventArgs e)
        {
            // Open the URL in the default browser
            try
            {
                Process.Start(new ProcessStartInfo(e.Uri.AbsoluteUri) { UseShellExecute = true });
                e.Handled = true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error opening URL: {ex.Message}");
            }
        }
    }
} 