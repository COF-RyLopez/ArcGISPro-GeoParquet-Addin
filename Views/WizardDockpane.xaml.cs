using System.Windows;
using System.Windows.Controls;

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
            }
        }
    }
}
