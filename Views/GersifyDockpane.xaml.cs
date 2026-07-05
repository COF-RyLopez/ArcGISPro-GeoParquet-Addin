using System.Windows.Controls;

namespace DuckDBGeoparquet.Views
{
    public partial class GersifyDockpaneView : UserControl
    {
        public GersifyDockpaneView()
        {
            InitializeComponent();
        }

        private void LogTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (sender is TextBox textBox)
            {
                textBox.ScrollToEnd();
            }
        }
    }
}
