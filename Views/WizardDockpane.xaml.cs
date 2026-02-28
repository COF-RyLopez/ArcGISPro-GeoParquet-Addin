using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using System.Diagnostics;

namespace DuckDBGeoparquet.Views
{
    public partial class WizardDockpaneView : UserControl
    {
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
    }
}
