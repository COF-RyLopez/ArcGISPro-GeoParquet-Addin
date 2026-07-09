using System;
using System.IO;
using System.Windows;

namespace DuckDBGeoparquet.Views.Diagnostics
{
    public static class DataContextLogger
    {
        private static readonly string LogDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory ?? ".", "docs", "diagnostics");
        private static readonly string LogFile = Path.Combine(LogDir, "datacontext-log.txt");

        public static readonly DependencyProperty EnableDataContextLoggingProperty =
            DependencyProperty.RegisterAttached("EnableDataContextLogging", typeof(bool), typeof(DataContextLogger), new PropertyMetadata(false, OnEnableChanged));

        public static void SetEnableDataContextLogging(DependencyObject element, bool value)
        {
            element.SetValue(EnableDataContextLoggingProperty, value);
        }

        public static bool GetEnableDataContextLogging(DependencyObject element)
        {
            return (bool)element.GetValue(EnableDataContextLoggingProperty);
        }

        private static void OnEnableChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            if (d is FrameworkElement fe)
            {
                if ((bool)e.NewValue)
                {
                    fe.DataContextChanged += Fe_DataContextChanged;
                    // Log initial value as well
                    LogDataContext(fe, fe.DataContext);
                }
                else
                {
                    fe.DataContextChanged -= Fe_DataContextChanged;
                }
            }
        }

        private static void Fe_DataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (sender is FrameworkElement fe)
            {
                LogDataContext(fe, e.NewValue);
            }
        }

        private static void LogDataContext(FrameworkElement fe, object dc)
        {
            try
            {
                Directory.CreateDirectory(LogDir);
                var ts = DateTime.UtcNow.ToString("o");
                var controlType = fe.GetType().FullName;
                var controlName = (fe.Name ?? "(unnamed)");
                var dcType = dc?.GetType().FullName ?? "(null)";
                var dcSample = dc?.ToString() ?? "(null)";
                if (dcSample.Length > 500) dcSample = dcSample.Substring(0, 500) + "...";
                var line = $"[{ts}] Control={controlType} Name={controlName} DataContextType={dcType} DataContextSample={dcSample}" + Environment.NewLine;
                File.AppendAllText(LogFile, line);
            }
            catch
            {
                // Swallow exceptions to avoid affecting UI
            }
        }
    }
}
