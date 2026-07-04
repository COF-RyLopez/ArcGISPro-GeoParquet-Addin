using System.Windows.Controls;

namespace DuckDBGeoparquet.Views
{
    public partial class MfcDockpaneView : UserControl
    {
        public MfcDockpaneView()
        {
            InitializeComponent();
        }

        private void MfcLogTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (sender is TextBox textBox)
            {
                textBox.ScrollToEnd();
            }
        }
    }
}
