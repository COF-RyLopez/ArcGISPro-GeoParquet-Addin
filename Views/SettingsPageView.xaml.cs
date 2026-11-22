using System.Windows;
using System.Windows.Controls;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Code-behind for the GeoParquet Settings page. PasswordBox is not
    /// bindable, so the password syncs here (same approach the wizard used).
    /// </summary>
    public partial class SettingsPageView : UserControl
    {
        public SettingsPageView()
        {
            InitializeComponent();
            DataContextChanged += (_, _) => ApplyPasswordToBox();
            Loaded += (_, _) => ApplyPasswordToBox();
        }

        private void ApplyPasswordToBox()
        {
            if (ProxyPasswordBox != null && DataContext is SettingsPageViewModel vm &&
                ProxyPasswordBox.Password != (vm.HttpProxyPassword ?? string.Empty))
                ProxyPasswordBox.Password = vm.HttpProxyPassword ?? string.Empty;
        }

        private void ProxyPasswordBox_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (DataContext is SettingsPageViewModel vm && sender is PasswordBox box &&
                vm.HttpProxyPassword != box.Password)
                vm.HttpProxyPassword = box.Password;
        }
    }
}
