using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using System.Diagnostics;
using System.Windows.Media;

namespace DuckDBGeoparquet.Views
{
    public partial class WizardDockpaneView : UserControl
    {
        public WizardDockpaneView()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Handles the TreeViewItem's Selected event to update the ViewModel's preview item.
        /// </summary>
        private void OnTreeViewItemSelected(object sender, RoutedEventArgs e)
        {
            if (DataContext is WizardDockpaneViewModel viewModel && e.OriginalSource is TreeViewItem treeViewItem)
            {
                if (treeViewItem.DataContext is SelectableThemeItem selectedThemeItem)
                {
                    viewModel.SelectedItemForPreview = selectedThemeItem;
                }
            }
        }

        private void BrowseButton_Click(object sender, RoutedEventArgs e)
        {
            // Add your browse button click logic here
        }

        private void IngestButton_Click(object sender, RoutedEventArgs e)
        {
            // Add your ingest button click logic here
        }

        private void TransformButton_Click(object sender, RoutedEventArgs e)
        {
            // Add your transform button click logic here
        }

        private void ExportButton_Click(object sender, RoutedEventArgs e)
        {
            // Add your export button click logic here
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
                
                // Also find parent ScrollViewer and scroll it to the bottom
                // This handles cases where the TextBox is inside a ScrollViewer
                var scrollViewer = FindParent<ScrollViewer>(textBox);
                if (scrollViewer != null)
                {
                    scrollViewer.ScrollToEnd();
                }
            }
        }
        
        /// <summary>
        /// Helper method to find parent control of specific type
        /// </summary>
        private static T FindParent<T>(DependencyObject child) where T : DependencyObject
        {
            DependencyObject parentObject = VisualTreeHelper.GetParent(child);
            
            if (parentObject == null) return null;
            
            if (parentObject is T parent)
                return parent;
            else
                return FindParent<T>(parentObject);
        }

        private void Hyperlink_RequestNavigate(object sender, RequestNavigateEventArgs e)
        {
            // Navigate to the requested URL
            Process.Start(new ProcessStartInfo(e.Uri.AbsoluteUri) { UseShellExecute = true });
            e.Handled = true;
        }
    }
} 