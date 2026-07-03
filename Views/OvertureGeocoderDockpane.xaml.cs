using System.Linq;
using System.Windows.Controls;
using System.Windows.Input;
using DuckDBGeoparquet.Models;

namespace DuckDBGeoparquet.Views
{
    public partial class OvertureGeocoderDockpaneView : UserControl
    {
        public OvertureGeocoderDockpaneView()
        {
            InitializeComponent();
        }

        private void ResultsListBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (DataContext is OvertureGeocoderDockpaneViewModel viewModel &&
                sender is ListBox listBox)
            {
                viewModel.SetSelectedCandidates(listBox.SelectedItems.Cast<GeocodeCandidate>());
            }
        }

        private void SearchTextBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Return &&
                DataContext is OvertureGeocoderDockpaneViewModel viewModel &&
                viewModel.SearchAddressCommand.CanExecute(null))
            {
                viewModel.SearchAddressCommand.Execute(null);
                e.Handled = true;
            }
        }
    }
}
