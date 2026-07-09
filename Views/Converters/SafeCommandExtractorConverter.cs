using System;
using System.Globalization;
using System.Reflection;
using System.Windows.Input;
using System.Windows.Data;

namespace DuckDBGeoparquet.Views.Converters
{
    public class SafeCommandExtractorConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null || parameter == null)
                return null;

            string propName = parameter.ToString();
            try
            {
                var prop = value.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
                if (prop == null)
                    return null;
                var cmd = prop.GetValue(value) as ICommand;
                return cmd;
            }
            catch
            {
                return null;
            }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
