using System;
using System.Threading.Tasks;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using System.Diagnostics;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Manages the DuckDB Feature Service Bridge lifecycle within the ArcGIS Pro Add-in
    /// Provides integration between the HTTP server and the existing DataProcessor
    /// </summary>
    public class FeatureServiceManager : IDisposable
    {
        private readonly DataProcessor _dataProcessor;
        private FeatureServiceBridge _featureServiceBridge;
        private bool _isRunning;

        public FeatureServiceManager(DataProcessor dataProcessor)
        {
            _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
        }

        /// <summary>
        /// Gets the feature service URL if running
        /// </summary>
        public string ServiceUrl => _featureServiceBridge?.ServiceUrl;

        /// <summary>
        /// Gets whether the service is currently running
        /// </summary>
        public bool IsRunning => _isRunning;

        /// <summary>
        /// Starts the DuckDB Feature Service Bridge
        /// </summary>
        public async Task<bool> StartServiceAsync(int port = 8080)
        {
            try
            {
                if (_isRunning)
                {
                    Debug.WriteLine("Feature Service Bridge is already running");
                    return true;
                }

                Debug.WriteLine("Starting DuckDB Feature Service Bridge...");

                _featureServiceBridge = new FeatureServiceBridge(_dataProcessor, port);
                await _featureServiceBridge.StartAsync();

                _isRunning = true;

                Debug.WriteLine($"✅ DuckDB Feature Service Bridge started successfully at {ServiceUrl}");
                
                // Show notification in ArcGIS Pro
                ShowProNotification("DuckDB Feature Service Started", 
                    $"Service available at: {ServiceUrl}", NotificationType.Information);

                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to start DuckDB Feature Service Bridge: {ex.Message}");
                
                ShowProNotification("Failed to Start Feature Service", 
                    $"Error: {ex.Message}", NotificationType.Error);
                
                return false;
            }
        }

        /// <summary>
        /// Stops the DuckDB Feature Service Bridge
        /// </summary>
        public async Task<bool> StopServiceAsync()
        {
            try
            {
                if (!_isRunning)
                {
                    Debug.WriteLine("Feature Service Bridge is not running");
                    return true;
                }

                Debug.WriteLine("Stopping DuckDB Feature Service Bridge...");

                if (_featureServiceBridge != null)
                {
                    await _featureServiceBridge.StopAsync();
                    _featureServiceBridge.Dispose();
                    _featureServiceBridge = null;
                }

                _isRunning = false;

                Debug.WriteLine("✅ DuckDB Feature Service Bridge stopped successfully");
                
                ShowProNotification("DuckDB Feature Service Stopped", 
                    "Service is no longer available", NotificationType.Information);

                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error stopping DuckDB Feature Service Bridge: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Toggles the service on/off
        /// </summary>
        public async Task<bool> ToggleServiceAsync(int port = 8080)
        {
            if (_isRunning)
            {
                return await StopServiceAsync();
            }
            else
            {
                return await StartServiceAsync(port);
            }
        }

        /// <summary>
        /// Gets service status information
        /// </summary>
        public ServiceStatus GetServiceStatus()
        {
            return new ServiceStatus
            {
                IsRunning = _isRunning,
                ServiceUrl = ServiceUrl,
                Message = _isRunning ? "Service is running" : "Service is stopped"
            };
        }

        /// <summary>
        /// Shows a notification in ArcGIS Pro
        /// </summary>
        private void ShowProNotification(string title, string message, NotificationType type)
        {
            try
            {
                // Simple logging approach since Notification class availability varies by ArcGIS Pro version
                var logMessage = $"{title}: {message}";
                if (type == NotificationType.Error)
                {
                    Debug.WriteLine($"ERROR: {logMessage}");
                }
                else
                {
                    Debug.WriteLine($"INFO: {logMessage}");
                }
                
                // Also output to Debug console for development
                System.Diagnostics.Debug.WriteLine($"[{type}] {logMessage}");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to show notification in ArcGIS Pro: {ex.Message}");
            }
        }

        public void Dispose()
        {
            try
            {
                if (_isRunning)
                {
                    // Fire and forget - don't await in Dispose
                    _ = Task.Run(async () => await StopServiceAsync());
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during FeatureServiceManager disposal: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Service status information
    /// </summary>
    public class ServiceStatus
    {
        public bool IsRunning { get; set; }
        public string ServiceUrl { get; set; }
        public string Message { get; set; }
    }

    /// <summary>
    /// Notification types for ArcGIS Pro
    /// </summary>
    public enum NotificationType
    {
        Information,
        Warning,
        Error
    }
} 