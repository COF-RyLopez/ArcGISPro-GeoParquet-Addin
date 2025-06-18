using System;
using System.Globalization;
using System.Windows.Data;
using DuckDBGeoparquet.Services;

namespace DuckDBGeoparquet.Views
{
    public class CloudProviderConverter : IValueConverter
    {
        public static CloudProviderConverter Instance { get; } = new CloudProviderConverter();

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is CloudProvider provider && parameter is string parameterString)
            {
                if (Enum.TryParse<CloudProvider>(parameterString, out CloudProvider parameterProvider))
                {
                    return provider == parameterProvider;
                }
            }
            return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is bool isChecked && isChecked && parameter is string parameterString)
            {
                if (Enum.TryParse<CloudProvider>(parameterString, out CloudProvider parameterProvider))
                {
                    return parameterProvider;
                }
            }
            return CloudProvider.AwsS3; // Default fallback
        }
    }
} 