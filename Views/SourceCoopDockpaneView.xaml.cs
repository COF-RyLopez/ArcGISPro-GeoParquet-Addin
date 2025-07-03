using System;
using System.Diagnostics;
using System.Windows.Controls;
using System.Windows.Navigation;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Interaction logic for SourceCoopDockpaneView.xaml
    /// </summary>
    public partial class SourceCoopDockpaneView : UserControl
    {
        public SourceCoopDockpaneView()
        {
            InitializeComponent();
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