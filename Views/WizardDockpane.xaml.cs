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
        /// Event handler for theme checkboxes to toggle theme selection
        /// </summary>
        private void ThemeCheckBox_CheckedChanged(object sender, RoutedEventArgs e)
        {
            if (DataContext is WizardDockpaneViewModel viewModel)
            {
                var checkBox = sender as CheckBox;
                if (checkBox != null && checkBox.Tag != null)
                {
                    string theme = checkBox.Tag.ToString();
                    viewModel.ToggleThemeSelection(theme);

                    // Also update the preview by setting SelectedTheme to the most recently selected theme
                    if (checkBox.IsChecked == true)
                    {
                        viewModel.SelectedTheme = theme;
                    }
                    else if (viewModel.SelectedThemes.Count > 0)
                    {
                        viewModel.SelectedTheme = viewModel.SelectedThemes[0];
                    }
                    else
                    {
                        viewModel.SelectedTheme = null;
                    }
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
    }
}
