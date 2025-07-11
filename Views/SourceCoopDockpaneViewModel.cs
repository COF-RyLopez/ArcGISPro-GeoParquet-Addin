using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using ArcGIS.Core.Geometry;
using ArcGIS.Core.Data;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows;
using DuckDBGeoparquet.Services;
using System.IO;
using ArcGIS.Desktop.Core;

namespace DuckDBGeoparquet.Views
{
    // RelayCommand implementation for WPF commanding
    public class RelayCommand : ICommand
    {
        private readonly Action _execute;
        private readonly Func<bool> _canExecute;
        private readonly Func<Task> _executeAsync;

        public RelayCommand(Action execute, Func<bool> canExecute = null)
        {
            _execute = execute ?? throw new ArgumentNullException(nameof(execute));
            _canExecute = canExecute;
        }

        public RelayCommand(Func<Task> executeAsync, Func<bool> canExecute = null)
        {
            _executeAsync = executeAsync ?? throw new ArgumentNullException(nameof(executeAsync));
            _canExecute = canExecute;
        }

        public event EventHandler CanExecuteChanged;

        public bool CanExecute(object parameter)
        {
            return _canExecute?.Invoke() ?? true;
        }

        public async void Execute(object parameter)
        {
            if (_executeAsync != null)
            {
                await _executeAsync();
            }
            else
            {
                _execute?.Invoke();
            }
        }

        public void RaiseCanExecuteChanged()
        {
            CanExecuteChanged?.Invoke(this, EventArgs.Empty);
        }
    }

    // Model classes for Source Cooperative API responses
    public class SourceCoopDataset : INotifyPropertyChanged
    {
        private bool _isSelected;
        
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public string Organization { get; set; } = string.Empty;
        public string Format { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
        public long? SizeBytes { get; set; }
        public DateTime? LastModified { get; set; }
        public string[] Tags { get; set; } = Array.Empty<string>();
        public SourceCoopSpatialExtent SpatialExtent { get; set; }
        
        public bool IsSelected
        {
            get => _isSelected;
            set => SetProperty(ref _isSelected, value);
        }
        
        public string DisplayName => $"{Name} ({Organization})";
        public string SizeDisplay => SizeBytes.HasValue ? FormatBytes(SizeBytes.Value) : "Unknown";
        public string FormatDisplay => Format?.ToUpper() ?? "Unknown";
        
        private static string FormatBytes(long bytes)
        {
            string[] suffixes = { "B", "KB", "MB", "GB", "TB" };
            int counter = 0;
            decimal number = bytes;
            while (Math.Round(number / 1024) >= 1)
            {
                number /= 1024;
                counter++;
            }
            return $"{number:n1} {suffixes[counter]}";
        }
        
        public event PropertyChangedEventHandler PropertyChanged;
        protected virtual bool SetProperty<T>(ref T field, T value, [CallerMemberName] string propertyName = null)
        {
            if (!EqualityComparer<T>.Default.Equals(field, value))
            {
                field = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
                return true;
            }
            return false;
        }
    }
    
    public class SourceCoopSpatialExtent
    {
        public double MinX { get; set; }
        public double MinY { get; set; }
        public double MaxX { get; set; }
        public double MaxY { get; set; }
    }
    
    public class SourceCoopOrganization
    {
        public string Name { get; set; } = string.Empty;
        public string DisplayName { get; set; } = string.Empty;
        public int DatasetCount { get; set; }
    }

    internal class SourceCoopDockpaneViewModel : DockPane
    {
        private const string _dockPaneID = "DuckDBGeoparquet_Views_SourceCoopDockpane";
        private const string SOURCE_COOP_API_BASE = "https://api.source.coop/v1"; // Hypothetical API endpoint
        private const string ADDIN_DATA_SUBFOLDER = "SourceCoopData";
        
        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        
        private DataProcessor _dataProcessor;
        private CancellationTokenSource _cts;
        
        // Collections
        private ObservableCollection<SourceCoopDataset> _datasets = new();
        private ObservableCollection<SourceCoopDataset> _filteredDatasets = new();
        private ObservableCollection<SourceCoopOrganization> _organizations = new();
        
        // Properties
        private bool _isLoading = true;
        private string _statusText = "Initializing Source Cooperative browser...";
        private double _progressValue = 0;
        private string _searchText = string.Empty;
        private string _selectedOrganization = "All Organizations";
        private string _selectedFormat = "All Formats";
        private SourceCoopDataset _selectedDataset;
        private StringBuilder _logOutput = new();
        private string _logOutputText = string.Empty;
        private readonly object _logLock = new object(); // Add thread safety
        private bool _isInitialized = false; // Prevent duplicate initialization
        private bool _useCurrentMapExtent = true;
        private bool _useCustomExtent = false;
        private Envelope _customExtent;
        private string _customExtentDisplay = "No custom extent set";
        private string _outputPath = string.Empty;
        private bool _filterByMapExtent = false;
        
        // Public Properties for Data Binding
        public ObservableCollection<SourceCoopDataset> Datasets
        {
            get => _datasets;
            set => SetProperty(ref _datasets, value);
        }
        
        public ObservableCollection<SourceCoopDataset> FilteredDatasets
        {
            get => _filteredDatasets;
            set => SetProperty(ref _filteredDatasets, value);
        }
        
        public ObservableCollection<SourceCoopOrganization> Organizations
        {
            get => _organizations;
            set => SetProperty(ref _organizations, value);
        }
        
        public bool IsLoading
        {
            get => _isLoading;
            set => SetProperty(ref _isLoading, value);
        }
        
        public string StatusText
        {
            get => _statusText;
            set => SetProperty(ref _statusText, value);
        }
        
        public double ProgressValue
        {
            get => _progressValue;
            set => SetProperty(ref _progressValue, value);
        }
        
        public string SearchText
        {
            get => _searchText;
            set 
            {
                if (SetProperty(ref _searchText, value))
                    ApplyFilters();
            }
        }
        
        public string SelectedOrganization
        {
            get => _selectedOrganization;
            set
            {
                if (SetProperty(ref _selectedOrganization, value))
                    ApplyFilters();
            }
        }
        
        public string SelectedFormat
        {
            get => _selectedFormat;
            set
            {
                if (SetProperty(ref _selectedFormat, value))
                    ApplyFilters();
            }
        }
        
        public SourceCoopDataset SelectedDataset
        {
            get => _selectedDataset;
            set => SetProperty(ref _selectedDataset, value);
        }
        
        public string LogOutputText
        {
            get => _logOutputText;
            set => SetProperty(ref _logOutputText, value);
        }
        
        public bool UseCurrentMapExtent
        {
            get => _useCurrentMapExtent;
            set
            {
                if (SetProperty(ref _useCurrentMapExtent, value) && value)
                {
                    UseCustomExtent = false;
                }
            }
        }
        
        public bool UseCustomExtent
        {
            get => _useCustomExtent;
            set
            {
                if (SetProperty(ref _useCustomExtent, value) && value)
                {
                    UseCurrentMapExtent = false;
                }
            }
        }
        
        public string CustomExtentDisplay
        {
            get => _customExtentDisplay;
            set => SetProperty(ref _customExtentDisplay, value);
        }
        
        public string OutputPath
        {
            get => _outputPath;
            set => SetProperty(ref _outputPath, value);
        }
        
        public bool FilterByMapExtent
        {
            get => _filterByMapExtent;
            set
            {
                if (SetProperty(ref _filterByMapExtent, value))
                    ApplyFilters();
            }
        }
        
        // Commands
        public ICommand RefreshDatasetsCommand { get; private set; }
        public ICommand DownloadSelectedCommand { get; private set; }
        public ICommand SetCustomExtentCommand { get; private set; }
        public ICommand BrowseOutputLocationCommand { get; private set; }
        public ICommand ClearFiltersCommand { get; private set; }
        public ICommand DebugCheckStateCommand { get; private set; }
        
        // Available filter options
        public List<string> AvailableOrganizations { get; private set; } = new() { "All Organizations" };
        public List<string> AvailableFormats { get; private set; } = new() { "All Formats" };
        
        public SourceCoopDockpaneViewModel()
        {
            try
            {
                // Prevent duplicate initialization
                if (_isInitialized)
                {
                    AddToLog("⚠️ Warning: Constructor called on already initialized ViewModel - skipping duplicate initialization");
                    return;
                }
                _isInitialized = true;
                
                AddToLog("🎯 Starting SourceCoopDockpaneViewModel constructor...");
                
                // Initialize collections first - these should never fail
                Datasets = new ObservableCollection<SourceCoopDataset>();
                FilteredDatasets = new ObservableCollection<SourceCoopDataset>();
                Organizations = new ObservableCollection<SourceCoopOrganization>();
                
                // Add immediate test data to verify the constructor is working
                var constructorTestDataset = new SourceCoopDataset
                {
                    Id = "constructor-test",
                    Name = "CONSTRUCTOR: Source Cooperative UI is Loading",
                    Description = "This dataset appears if the ViewModel constructor succeeded. Real datasets should load shortly.",
                    Organization = "Constructor Test",
                    Format = "GeoParquet",
                    SizeBytes = 1000000,
                    IsSelected = false
                };
                
                // Subscribe to property changes to update download button state
                constructorTestDataset.PropertyChanged += OnDatasetPropertyChanged;
                
                Datasets.Add(constructorTestDataset);
                FilteredDatasets.Add(constructorTestDataset);
                
                InitializeCommands();
                InitializeOutputPath();
                
                // Try to subscribe to CustomExtentTool events with error handling
                try
                {
                    CustomExtentTool.ExtentCreatedStatic += OnExtentCreated;
                    AddToLog("✅ Successfully subscribed to CustomExtentTool events");
                }
                catch (Exception extentEx)
                {
                    AddToLog($"⚠️ Warning: Could not subscribe to CustomExtentTool events: {extentEx.Message}");
                    // Continue without CustomExtentTool integration - not critical for basic functionality
                }
                
                AddToLog("🚀 SourceCoopDockpaneViewModel constructor completed successfully");
                
                // Start async initialization - don't await in constructor
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await InitializeAsync();
                    }
                    catch (Exception initEx)
                    {
                        AddToLog($"❌ Async initialization failed: {initEx.Message}");
                    }
                });
            }
            catch (Exception ex)
            {
                // Constructor-level failure - this is serious
                try
                {
                    AddToLog($"❌ CONSTRUCTOR FAILED: {ex.Message}");
                    AddToLog($"❌ Exception type: {ex.GetType().Name}");
                    AddToLog($"❌ Stack trace: {ex.StackTrace}");
                    
                    // Try to create minimal working state
                    if (Datasets == null) Datasets = new ObservableCollection<SourceCoopDataset>();
                    if (FilteredDatasets == null) FilteredDatasets = new ObservableCollection<SourceCoopDataset>();
                    
                    var errorDataset = new SourceCoopDataset
                    {
                        Id = "constructor-error",
                        Name = "ERROR: Constructor Failed",
                        Description = $"ViewModel constructor failed: {ex.Message}",
                        Organization = "Error Handler",
                        Format = "Error",
                        SizeBytes = 0
                    };
                    Datasets.Add(errorDataset);
                    FilteredDatasets.Add(errorDataset);
                    
                    StatusText = "Constructor failed - check Activity Log";
                    IsLoading = false;
                }
                catch (Exception logEx)
                {
                    // If even our error handling fails, we're in big trouble
                    System.Diagnostics.Debug.WriteLine($"Complete failure in SourceCoopDockpaneViewModel constructor: {ex.Message}");
                    System.Diagnostics.Debug.WriteLine($"Even error logging failed: {logEx.Message}");
                }
            }
        }
        
        private void InitializeCommands()
        {
            RefreshDatasetsCommand = new RelayCommand(async () => await RefreshDatasets(), () => !IsLoading);
            DownloadSelectedCommand = new RelayCommand(async () => await DownloadSelected(), () => CanDownload());
            SetCustomExtentCommand = new RelayCommand(() => SetCustomExtent());
            BrowseOutputLocationCommand = new RelayCommand(() => BrowseOutputLocation());
            ClearFiltersCommand = new RelayCommand(() => ClearFilters());
            DebugCheckStateCommand = new RelayCommand(() => DebugCheckState());
        }
        
        private void InitializeOutputPath()
        {
            try
            {
                var project = Project.Current;
                string basePath = project?.HomeFolderPath ?? Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
                OutputPath = Path.Combine(basePath, ADDIN_DATA_SUBFOLDER);
            }
            catch (Exception ex)
            {
                AddToLog($"Warning: Could not determine project path: {ex.Message}");
                OutputPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), ADDIN_DATA_SUBFOLDER);
            }
        }
        
        protected override async Task InitializeAsync()
        {
            try
            {
                AddToLog("🚀 Initializing Source Cooperative browser...");
                AddToLog($"🔍 Debug: ViewModel type = {this.GetType().Name}");
                AddToLog($"🔍 Debug: Collections initialized - Datasets: {Datasets.Count}, FilteredDatasets: {FilteredDatasets.Count}");
                
                // Ensure we have immediate test data that's always visible
                if (Datasets.Count == 0)
                {
                    AddToLog("🧪 Adding immediate test datasets for UI verification...");
                    
                    var testDatasets = new[]
                    {
                        new SourceCoopDataset
                        {
                            Id = "test-dataset-1",
                            Name = "TEST: Source Cooperative UI Check",
                            Description = "If you see this, the UI binding is working correctly! Real datasets should load after this.",
                            Organization = "UI Test",
                            Format = "GeoParquet",
                            Url = "test://test",
                            SizeBytes = 1000000,
                            IsSelected = false
                        },
                        new SourceCoopDataset
                        {
                            Id = "test-dataset-2", 
                            Name = "TEST: Planet EU Field Boundaries (Preview)",
                            Description = "Preview of real Planet Labs agricultural boundaries dataset from Source Cooperative",
                            Organization = "Planet Labs (Test)",
                            Format = "GeoParquet",
                            Url = "https://data.source.coop/planet/eu-field-boundaries/field_boundaries.parquet",
                            SizeBytes = 2500000000,
                            IsSelected = false
                        },
                        new SourceCoopDataset
                        {
                            Id = "test-dataset-3",
                            Name = "TEST: VIDA Buildings (Preview)", 
                            Description = "Preview of VIDA combined building footprints from Google, Microsoft, and OSM",
                            Organization = "VIDA (Test)",
                            Format = "GeoParquet",
                            Url = "s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/",
                            SizeBytes = 45000000000,
                            IsSelected = false
                        }
                    };
                    
                    foreach (var dataset in testDatasets)
                    {
                        // Subscribe to property changes to update download button state
                        dataset.PropertyChanged += OnDatasetPropertyChanged;
                        
                        Datasets.Add(dataset);
                        FilteredDatasets.Add(dataset);
                    }
                    
                    AddToLog($"✅ Added {testDatasets.Length} test datasets. Total count: {Datasets.Count}");
                    
                    // Update filter options with test data
                    UpdateFilterOptions();
                    AddToLog($"🔍 Filter options updated - Organizations: {AvailableOrganizations.Count}, Formats: {AvailableFormats.Count}");
                }
                
                if (_dataProcessor == null)
                    _dataProcessor = new DataProcessor();
                
                // Now try to load real datasets (but don't let failure block the test data)
                try
                {
                    AddToLog("📊 Attempting to load real Source Cooperative datasets...");
                    await RefreshDatasets();
                }
                catch (Exception dataEx)
                {
                    AddToLog($"⚠️ Real dataset loading failed, but test data is still available: {dataEx.Message}");
                }
                
                StatusText = $"Ready - {Datasets.Count} datasets available";
                IsLoading = false;
                AddToLog("✅ Initialization completed successfully");
                AddToLog("💡 To test the download functionality, manually check a dataset checkbox and click download, or trigger the debug command from code.");
            }
            catch (Exception ex)
            {
                AddToLog($"❌ Initialization failed: {ex.Message}");
                AddToLog($"❌ Stack trace: {ex.StackTrace}");
                StatusText = "Initialization failed - check Activity Log";
                IsLoading = false;
                
                // Add emergency fallback data even if everything fails
                try
                {
                    AddToLog("🚨 Adding emergency fallback data...");
                    if (Datasets.Count == 0)
                    {
                        var emergencyDataset = new SourceCoopDataset
                        {
                            Id = "emergency-fallback",
                            Name = "EMERGENCY: Fallback Dataset",
                            Description = "This appears because all initialization failed. Check the Activity Log for details.",
                            Organization = "Emergency Fallback",
                            Format = "Error",
                            SizeBytes = 0,
                            IsSelected = false
                        };
                        Datasets.Add(emergencyDataset);
                        FilteredDatasets.Add(emergencyDataset);
                        AddToLog("🚨 Emergency fallback dataset added");
                    }
                }
                catch (Exception fallbackEx)
                {
                    AddToLog($"❌ Even emergency fallback failed: {fallbackEx.Message}");
                }
            }
        }
        
        private async Task RefreshDatasets()
        {
            if (_cts != null)
            {
                _cts.Cancel();
                _cts.Dispose();
                _cts = null;
            }
            _cts = new CancellationTokenSource();
            
            try
            {
                IsLoading = true;
                StatusText = "Loading available datasets from Source Cooperative...";
                ProgressValue = 0;
                
                AddToLog("📡 Starting dataset refresh...");
                AddToLog($"🔍 Debug: Current Datasets count: {Datasets.Count}");
                AddToLog($"🔍 Debug: Current FilteredDatasets count: {FilteredDatasets.Count}");
                
                AddToLog("📊 Fetching dataset catalog from Source Cooperative API...");
                
                // Get datasets from our Source Cooperative client
                var datasets = await FetchDatasetsFromApi(_cts.Token);
                AddToLog($"📥 API returned {datasets.Count} datasets");
                
                // Update UI on main thread
                AddToLog("🔄 Updating datasets collection...");
                var initialCount = Datasets.Count;
                Datasets.Clear();
                AddToLog($"🗑️ Cleared existing datasets (was {initialCount})");
                
                foreach (var dataset in datasets)
                {
                    // Subscribe to property changes to update download button state
                    dataset.PropertyChanged += OnDatasetPropertyChanged;
                    
                    Datasets.Add(dataset);
                    AddToLog($"➕ Added dataset: {dataset.Name} ({dataset.Organization})");
                    
                    // Debug: Log dataset bounding boxes
                    if (dataset.SpatialExtent != null)
                    {
                        System.Diagnostics.Debug.WriteLine($"🔍 Dataset '{dataset.Name}' extent: {dataset.SpatialExtent.MinX:F2}, {dataset.SpatialExtent.MinY:F2}, {dataset.SpatialExtent.MaxX:F2}, {dataset.SpatialExtent.MaxY:F2}");
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine($"🔍 Dataset '{dataset.Name}' has no spatial extent");
                    }
                }
                AddToLog($"✅ Added {datasets.Count} datasets to collection");
                
                AddToLog("🔍 Updating filter options...");
                UpdateFilterOptions();
                AddToLog($"🔍 Available organizations: {AvailableOrganizations.Count}");
                AddToLog($"🔍 Available formats: {AvailableFormats.Count}");
                
                AddToLog("🔍 Applying filters...");
                ApplyFilters();
                AddToLog($"🔍 FilteredDatasets count after filtering: {FilteredDatasets.Count}");
                
                AddToLog($"🎉 Successfully loaded {datasets.Count} datasets");
                StatusText = $"Loaded {datasets.Count} datasets from Source Cooperative";
                ProgressValue = 100;
            }
            catch (OperationCanceledException)
            {
                AddToLog("⏹️ Dataset loading was cancelled");
                StatusText = "Loading cancelled";
            }
            catch (Exception ex)
            {
                AddToLog($"❌ Error loading datasets: {ex.Message}");
                AddToLog($"❌ Exception type: {ex.GetType().Name}");
                AddToLog($"❌ Stack trace: {ex.StackTrace}");
                StatusText = "Failed to load datasets. Check connection and try again.";
                
                // Ensure we have some test data even if loading fails
                if (Datasets.Count == 0)
                {
                    AddToLog("🔄 Adding emergency test data since no datasets loaded...");
                    var emergencyDataset = new SourceCoopDataset
                    {
                        Id = "emergency-test",
                        Name = "EMERGENCY: Test Dataset Placeholder",
                        Description = "This appears because the real dataset loading failed. Check the Activity Log for details.",
                        Organization = "Error Fallback",
                        Format = "GeoParquet",
                        SizeBytes = 1000000
                    };
                    
                    // Subscribe to property changes to update download button state
                    emergencyDataset.PropertyChanged += OnDatasetPropertyChanged;
                    
                    Datasets.Add(emergencyDataset);
                    FilteredDatasets.Add(emergencyDataset);
                    AddToLog("🚨 Emergency test dataset added");
                }
            }
            finally
            {
                IsLoading = false;
                AddToLog($"🏁 RefreshDatasets completed. Final counts - Datasets: {Datasets.Count}, FilteredDatasets: {FilteredDatasets.Count}");
                (RefreshDatasetsCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (DownloadSelectedCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }
        
        private async Task<List<SourceCoopDataset>> FetchDatasetsFromApi(CancellationToken cancellationToken)
        {
            try
            {
                AddToLog("Connecting to Source Cooperative...");
                
                using var sourceCoopClient = new SourceCooperativeClient();
                
                // Get available repositories (these now include embedded datasets from Chris Holmes' presets)
                AddToLog("Loading Source Cooperative dataset catalog...");
                var repositories = await sourceCoopClient.GetRepositoriesAsync(cancellationToken);
                AddToLog($"Found {repositories.Count} data providers");
                
                var allDatasets = new List<SourceCoopDataset>();
                
                // Process each repository and its embedded datasets
                foreach (var repo in repositories)
                {
                    try
                    {
                        AddToLog($"Loading datasets from {repo.Name}...");
                        
                        // Use embedded datasets from repository (Chris Holmes' presets structure)
                        if (repo.Datasets != null && repo.Datasets.Any())
                        {
                            foreach (var dataset in repo.Datasets)
                            {
                                allDatasets.Add(new SourceCoopDataset
                                {
                                    Id = dataset.Id,
                                    Name = dataset.Name,
                                    Description = dataset.Description,
                                    Organization = dataset.Organization,
                                    Format = dataset.Format,
                                    Url = dataset.Url,
                                    SizeBytes = dataset.SizeBytes,
                                    LastModified = dataset.UpdatedDate,
                                    Tags = new string[] { repo.Name.ToLower(), dataset.Format.ToLower() },
                                    SpatialExtent = new SourceCoopSpatialExtent
                                    {
                                        MinX = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[0] : -180,
                                        MinY = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[1] : -90,
                                        MaxX = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[2] : 180,
                                        MaxY = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[3] : 90
                                    }
                                });
                                
                                // Debug: Log what we got from the API
                                System.Diagnostics.Debug.WriteLine($"🔍 API Dataset '{dataset.Name}':");
                                System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox null: {dataset.BoundingBox == null}");
                                if (dataset.BoundingBox != null)
                                {
                                    System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox length: {dataset.BoundingBox.Length}");
                                    if (dataset.BoundingBox.Length > 0)
                                    {
                                        System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox values: [{string.Join(", ", dataset.BoundingBox)}]");
                                    }
                                }
                                else
                                {
                                    System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox is null - using world extent fallback");
                                }
                            }
                            AddToLog($"✅ Added {repo.Datasets.Count} datasets from {repo.Name}");
                        }
                        else
                        {
                            // Fallback: try to fetch datasets via API if no embedded datasets
                            var repoDatasets = await sourceCoopClient.GetRepositoryDatasetsAsync(repo.Id, cancellationToken);
                            
                            foreach (var dataset in repoDatasets)
                            {
                                allDatasets.Add(new SourceCoopDataset
                                {
                                    Id = dataset.Id,
                                    Name = dataset.Name,
                                    Description = dataset.Description,
                                    Organization = dataset.Organization,
                                    Format = dataset.Format,
                                    Url = dataset.DownloadUrl ?? dataset.Url,
                                    SizeBytes = dataset.SizeBytes,
                                    LastModified = dataset.UpdatedDate,
                                    Tags = dataset.Tags ?? new string[] { repo.Name.ToLower() },
                                    SpatialExtent = new SourceCoopSpatialExtent
                                    {
                                        MinX = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[0] : -180,
                                        MinY = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[1] : -90,
                                        MaxX = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[2] : 180,
                                        MaxY = dataset.BoundingBox?.Length >= 4 ? dataset.BoundingBox[3] : 90
                                    }
                                });
                                
                                // Debug: Log what we got from the API
                                System.Diagnostics.Debug.WriteLine($"🔍 API Dataset '{dataset.Name}':");
                                System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox null: {dataset.BoundingBox == null}");
                                if (dataset.BoundingBox != null)
                                {
                                    System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox length: {dataset.BoundingBox.Length}");
                                    if (dataset.BoundingBox.Length > 0)
                                    {
                                        System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox values: [{string.Join(", ", dataset.BoundingBox)}]");
                                    }
                                }
                                else
                                {
                                    System.Diagnostics.Debug.WriteLine($"🔍   BoundingBox is null - using world extent fallback");
                                }
                            }
                            AddToLog($"✅ Added {repoDatasets.Count} datasets from {repo.Name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"⚠️  Warning: Failed to fetch datasets from {repo.Name}: {ex.Message}");
                        // Continue with other repositories
                    }
                }
                
                AddToLog($"🎉 Successfully loaded {allDatasets.Count} real GeoParquet datasets");
                AddToLog("📊 Datasets include: Agricultural boundaries, Building footprints, Infrastructure data, Places/POIs, and Hydrology");
                return allDatasets;
            }
            catch (OperationCanceledException)
            {
                AddToLog("Dataset fetch operation was cancelled");
                throw;
            }
            catch (Exception ex)
            {
                AddToLog($"❌ Error connecting to Source Cooperative: {ex.Message}");
                throw;
            }
        }
        
        private void UpdateFilterOptions()
        {
            var organizations = Datasets.Select(d => d.Organization).Distinct().OrderBy(o => o).ToList();
            AvailableOrganizations = new List<string> { "All Organizations" }.Concat(organizations).ToList();
            
            var formats = Datasets.Select(d => d.Format).Distinct().OrderBy(f => f).ToList();
            AvailableFormats = new List<string> { "All Formats" }.Concat(formats).ToList();
            
            NotifyPropertyChanged(nameof(AvailableOrganizations));
            NotifyPropertyChanged(nameof(AvailableFormats));
        }
        
        private void ApplyFilters()
        {
            var filtered = Datasets.AsEnumerable();
            
            System.Diagnostics.Debug.WriteLine($"🔍 ApplyFilters called - Total datasets: {Datasets.Count}, FilterByMapExtent: {FilterByMapExtent}");
            
            if (!string.IsNullOrWhiteSpace(SearchText))
            {
                filtered = filtered.Where(d => 
                    d.Name.Contains(SearchText, StringComparison.OrdinalIgnoreCase) ||
                    d.Description.Contains(SearchText, StringComparison.OrdinalIgnoreCase) ||
                    d.Tags.Any(tag => tag.Contains(SearchText, StringComparison.OrdinalIgnoreCase)));
                System.Diagnostics.Debug.WriteLine($"🔍 After search filter: {filtered.Count()} datasets");
            }
            
            if (SelectedOrganization != "All Organizations")
            {
                filtered = filtered.Where(d => d.Organization == SelectedOrganization);
                System.Diagnostics.Debug.WriteLine($"🔍 After organization filter: {filtered.Count()} datasets");
            }
            
            if (SelectedFormat != "All Formats")
            {
                filtered = filtered.Where(d => d.Format == SelectedFormat);
                System.Diagnostics.Debug.WriteLine($"🔍 After format filter: {filtered.Count()} datasets");
            }
            
            // Spatial extent filtering
            if (FilterByMapExtent)
            {
                System.Diagnostics.Debug.WriteLine("🔍 Spatial filtering enabled - getting map extent...");
                var mapExtent = GetCurrentMapExtentSync();
                if (mapExtent != null)
                {
                    System.Diagnostics.Debug.WriteLine($"🔍 Map extent retrieved successfully - applying spatial filter...");
                    var beforeCount = filtered.Count();
                    filtered = filtered.Where(d => DatasetIntersectsExtent(d.SpatialExtent, mapExtent));
                    var afterCount = filtered.Count();
                    System.Diagnostics.Debug.WriteLine($"🔍 Spatial filter applied: {beforeCount} -> {afterCount} datasets");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("🔍 Could not get map extent - spatial filtering skipped");
                }
            }
            
            // Ensure UI updates happen on the UI thread
            System.Windows.Application.Current.Dispatcher.Invoke(() =>
            {
                FilteredDatasets.Clear();
                foreach (var dataset in filtered)
                {
                    FilteredDatasets.Add(dataset);
                }
                
                var statusMessage = $"Showing {FilteredDatasets.Count} of {Datasets.Count} datasets";
                if (FilterByMapExtent)
                {
                    statusMessage += " (filtered by map extent)";
                }
                StatusText = statusMessage;
                System.Diagnostics.Debug.WriteLine($"🔍 Final result: {statusMessage}");
            });
        }
        
        /// <summary>
        /// Synchronous version of GetCurrentMapExtent for use in filters
        /// </summary>
        private Envelope GetCurrentMapExtentSync()
        {
            try
            {
                // This needs to be called on the UI thread, but we're already on it during filtering
                var mapView = MapView.Active;
                if (mapView?.Extent != null)
                {
                    var extent = mapView.Extent;
                    System.Diagnostics.Debug.WriteLine($"🔍 Original map extent: {extent.XMin:F2}, {extent.YMin:F2}, {extent.XMax:F2}, {extent.YMax:F2}");
                    System.Diagnostics.Debug.WriteLine($"🔍 Map extent SR: {extent.SpatialReference?.Wkid}");
                    
                    // Project to WGS84 if needed for comparison with dataset extents
                    if (extent.SpatialReference?.Wkid != 4326)
                    {
                        var wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                        try
                        {
                            var projectedExtent = GeometryEngine.Instance.Project(extent, wgs84) as Envelope;
                            if (projectedExtent != null)
                            {
                                System.Diagnostics.Debug.WriteLine($"🔍 Projected to WGS84: {projectedExtent.XMin:F2}, {projectedExtent.YMin:F2}, {projectedExtent.XMax:F2}, {projectedExtent.YMax:F2}");
                                return projectedExtent;
                            }
                            else
                            {
                                System.Diagnostics.Debug.WriteLine("🔍 Projection failed, using original extent");
                                return extent;
                            }
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine($"🔍 Projection error: {ex.Message}, using original extent");
                            // If projection fails, return original extent
                            return extent;
                        }
                    }
                    System.Diagnostics.Debug.WriteLine("🔍 Map extent already in WGS84");
                    return extent;
                }
                System.Diagnostics.Debug.WriteLine("🔍 No active map view or extent found");
                return null;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"🔍 Error getting map extent for filtering: {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// Checks if a dataset's spatial extent intersects with the map extent
        /// </summary>
        private bool DatasetIntersectsExtent(SourceCoopSpatialExtent datasetExtent, Envelope mapExtent)
        {
            if (datasetExtent == null || mapExtent == null) return true;
            
            // Debug logging
            System.Diagnostics.Debug.WriteLine($"🔍 Checking intersection for dataset extent: {datasetExtent.MinX:F2}, {datasetExtent.MinY:F2}, {datasetExtent.MaxX:F2}, {datasetExtent.MaxY:F2}");
            System.Diagnostics.Debug.WriteLine($"🔍 Map extent: {mapExtent.XMin:F2}, {mapExtent.YMin:F2}, {mapExtent.XMax:F2}, {mapExtent.YMax:F2}");
            
            // Check for bounding box intersection
            // Two rectangles intersect if they overlap in both X and Y dimensions
            bool intersects = !(datasetExtent.MaxX < mapExtent.XMin || 
                               datasetExtent.MinX > mapExtent.XMax ||
                               datasetExtent.MaxY < mapExtent.YMin || 
                               datasetExtent.MinY > mapExtent.YMax);
            
            System.Diagnostics.Debug.WriteLine($"🔍 Intersection result: {intersects}");
            return intersects;
        }
        
        private bool CanDownload()
        {
            return !IsLoading && FilteredDatasets.Any(d => d.IsSelected);
        }
        
        private async Task DownloadSelected()
        {
            AddToLog("🚀 Download button clicked - checking selected datasets...");
            AddToLog($"🔍 Total datasets in FilteredDatasets: {FilteredDatasets.Count}");
            
            var selectedDatasets = FilteredDatasets.Where(d => d.IsSelected).ToList();
            AddToLog($"🔍 Selected datasets count: {selectedDatasets.Count}");
            
            if (!selectedDatasets.Any()) 
            {
                AddToLog("❌ No datasets selected - please check the checkboxes in the Browse Datasets tab");
                AddToLog("💡 Debug: Listing all datasets and their selection status:");
                foreach (var dataset in FilteredDatasets)
                {
                    AddToLog($"   📋 {dataset.Name}: IsSelected = {dataset.IsSelected}");
                }
                return;
            }
            
            if (_cts != null)
            {
                _cts.Cancel();
                _cts.Dispose();
                _cts = null;
            }
            _cts = new CancellationTokenSource();
            
            try
            {
                IsLoading = true;
                
                foreach (var dataset in selectedDatasets)
                {
                    StatusText = $"Downloading {dataset.Name}...";
                    AddToLog($"Starting download of {dataset.Name} from {dataset.Organization}");
                    
                    // Get spatial extent for filtering
                    Envelope extent = null;
                    if (UseCurrentMapExtent)
                    {
                        extent = await GetCurrentMapExtent();
                    }
                    else if (UseCustomExtent && _customExtent != null)
                    {
                        extent = _customExtent;
                    }
                    
                    // Download and process the dataset
                    if (extent != null)
                    {
                        await ProcessDataset(dataset, extent, _cts.Token);
                    }
                    else
                    {
                        // Process without spatial filtering
                        await ProcessDataset(dataset, null, _cts.Token);
                    }
                    
                    AddToLog($"Successfully processed {dataset.Name}");
                }
                
                StatusText = "Download completed successfully";
                AddToLog($"All selected datasets downloaded to: {OutputPath}");
            }
            catch (OperationCanceledException)
            {
                StatusText = "Download cancelled";
                AddToLog("Download operation was cancelled");
            }
            catch (Exception ex)
            {
                StatusText = $"Download failed: {ex.Message}";
                AddToLog($"Error during download: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
                (DownloadSelectedCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }
        
        private async Task<Envelope> GetCurrentMapExtent()
        {
            return await QueuedTask.Run(() =>
            {
                var mapView = MapView.Active;
                return mapView?.Extent;
            });
        }
        
        private async Task ProcessDataset(SourceCoopDataset dataset, Envelope extent, CancellationToken cancellationToken)
        {
            AddToLog($"Processing dataset: {dataset.Name}");
            
            try
            {
                // Initialize DataProcessor if not already done
                if (_dataProcessor == null)
                {
                    _dataProcessor = new DataProcessor();
                    AddToLog("DataProcessor initialized");
                }
                
                // Create output file path
                var sanitizedName = dataset.Id.Replace("/", "_").Replace("\\", "_");
                var outputFile = Path.Combine(OutputPath, $"{sanitizedName}.parquet");
                AddToLog($"Output file: {outputFile}");
                
                // Download and process the actual data
                await DownloadAndProcessDataset(dataset, outputFile, extent, cancellationToken);
                
                // Add the layer to the active map
                await QueuedTask.Run(() =>
                {
                    try
                    {
                        var mapView = MapView.Active;
                        if (mapView?.Map != null)
                        {
                            AddToLog($"Adding layer to map: {dataset.Name}");
                            
                            // Check if the output file exists
                            if (!File.Exists(outputFile))
                            {
                                AddToLog($"❌ Output file does not exist: {outputFile}");
                                return;
                            }
                            
                            AddToLog($"✅ Output file exists: {outputFile}");
                            
                            // Create a layer from the downloaded GeoParquet
                            var layerUri = new Uri(outputFile);
                            
                            try
                            {
                                // Try using the generic layer creation method for GeoParquet
                                var layer = LayerFactory.Instance.CreateLayer(layerUri, mapView.Map, layerName: dataset.Name);
                                if (layer != null)
                                {
                                    AddToLog($"✅ Layer '{dataset.Name}' added to map successfully");
                                }
                                else
                                {
                                    AddToLog($"⚠️ Layer creation returned null for '{dataset.Name}'");
                                }
                            }
                            catch (Exception layerEx)
                            {
                                AddToLog($"❌ Error creating layer with LayerFactory: {layerEx.Message}");
                                
                                // Try alternative approach using LayerCreationParams
                                try
                                {
                                    AddToLog("Trying alternative layer creation method...");
                                    var layerParams = new LayerCreationParams(layerUri)
                                    {
                                        Name = dataset.Name
                                    };
                                    var featureLayer = LayerFactory.Instance.CreateLayer<FeatureLayer>(layerParams, mapView.Map);
                                    if (featureLayer != null)
                                    {
                                        AddToLog($"✅ Layer '{dataset.Name}' added to map using alternative method");
                                    }
                                    else
                                    {
                                        AddToLog($"❌ Alternative layer creation also failed for '{dataset.Name}'");
                                    }
                                }
                                catch (Exception altEx)
                                {
                                    AddToLog($"❌ Alternative layer creation failed: {altEx.Message}");
                                    AddToLog($"💡 You can manually add the file to ArcGIS Pro: {outputFile}");
                                }
                            }
                        }
                        else
                        {
                            AddToLog("⚠️ No active map view to add layer");
                        }
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"❌ Error adding layer to map: {ex.Message}");
                    }
                });
                
                AddToLog($"✅ Successfully processed {dataset.Name}");
            }
            catch (Exception ex)
            {
                AddToLog($"❌ Error processing {dataset.Name}: {ex.Message}");
                throw;
            }
        }
        
        private async Task DownloadAndProcessDataset(SourceCoopDataset dataset, string outputFile, Envelope extent, CancellationToken cancellationToken)
        {
            // Create directory if it doesn't exist
            var directory = Path.GetDirectoryName(outputFile);
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
                AddToLog($"Created output directory: {directory}");
            }
            
            AddToLog($"Downloading data from: {dataset.Url}");
            
            // Use DataProcessor to download and process the data with DuckDB
            var success = await _dataProcessor.ProcessSourceCooperativeDataset(
                dataset.Url, 
                outputFile, 
                extent, 
                progress => {
                    // Update progress on UI thread
                    FrameworkApplication.Current.Dispatcher.Invoke(() => {
                        ProgressValue = progress;
                        StatusText = $"Processing {dataset.Name}: {progress:F1}%";
                    });
                },
                message => AddToLog(message),
                cancellationToken
            );
            
            if (!success)
            {
                throw new InvalidOperationException($"Failed to download and process dataset: {dataset.Name}");
            }
            
            AddToLog($"✅ Successfully downloaded and processed: {dataset.Name}");
        }
        
        private void SetCustomExtent()
        {
            try
            {
                AddToLog("Activating custom extent drawing tool...");
                
                // Ensure the custom extent radio button is selected
                UseCustomExtent = true;
                
                // Use ArcGIS Pro's drawing tool to select an extent
                QueuedTask.Run(async () =>
                {
                    AddToLog("Starting custom extent drawing operation...");
                    
                    // Get a reference to the active map and make sure one exists
                    var mapView = MapView.Active;
                    if (mapView == null)
                    {
                        AddToLog("Unable to set custom extent: No active map view");
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                            "Please open a map before setting a custom extent.",
                            "No Active Map");
                        return;
                    }
                    
                    AddToLog($"Active map view found: {mapView.Map.Name}");
                    
                    try
                    {
                        // Activate our custom tool using the ID defined in Config.daml
                        await FrameworkApplication.SetCurrentToolAsync("DuckDBGeoparquet_CustomExtentTool");
                        AddToLog("Draw a rectangle on the map to set the custom extent...");
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"Error activating drawing tool: {ex.Message}");
                    }
                });
            }
            catch (Exception ex)
            {
                AddToLog($"Error setting custom extent: {ex.Message}");
            }
        }
        
        // Handler for when the custom tool creates an extent
        private void OnExtentCreated(Envelope extent)
        {
            try
            {
                AddToLog($"Custom extent created: {extent.XMin:F6}, {extent.YMin:F6}, {extent.XMax:F6}, {extent.YMax:F6}");
                
                // Project extent to WGS84 if needed
                Envelope extentInWGS84 = extent;
                if (extent.SpatialReference == null || extent.SpatialReference.Wkid != 4326)
                {
                    AddToLog("Projecting custom extent to WGS84...");
                    SpatialReference wgs84 = SpatialReferenceBuilder.CreateSpatialReference(4326);
                    try
                    {
                        extentInWGS84 = GeometryEngine.Instance.Project(extent, wgs84) as Envelope;
                        if (extentInWGS84 != null)
                        {
                            AddToLog($"Successfully projected to WGS84: {extentInWGS84.XMin:F6}, {extentInWGS84.YMin:F6}, {extentInWGS84.XMax:F6}, {extentInWGS84.YMax:F6}");
                        }
                        else
                        {
                            AddToLog("ERROR: Projection to WGS84 resulted in null envelope. Using original extent.");
                            extentInWGS84 = extent;
                        }
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"ERROR: Failed to project to WGS84: {ex.Message}. Using original extent.");
                        extentInWGS84 = extent;
                    }
                }
                else
                {
                    AddToLog("Custom extent is already in WGS84.");
                }
                
                // Store the extent and update UI
                _customExtent = extentInWGS84;
                UpdateCustomExtentDisplay();
                
                // Ensure custom extent option is selected
                UseCustomExtent = true;
                
                // Deactivate the drawing tool and provide feedback
                QueuedTask.Run(async () => {
                    try
                    {
                        // Return to default explore tool
                        await FrameworkApplication.SetCurrentToolAsync("esri_mapping_exploreTool");
                        
                        AddToLog("Custom extent successfully set. You can now download datasets using this extent.");
                        
                        // Show confirmation dialog
                        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                            $"Custom extent set successfully:\nMin X,Y: {extentInWGS84.XMin:F4}, {extentInWGS84.YMin:F4}\nMax X,Y: {extentInWGS84.XMax:F4}, {extentInWGS84.YMax:F4}",
                            "Custom Extent Set",
                            System.Windows.MessageBoxButton.OK,
                            System.Windows.MessageBoxImage.Information);
                    }
                    catch (Exception ex)
                    {
                        AddToLog($"Error after setting extent: {ex.Message}");
                    }
                });
            }
            catch (Exception ex)
            {
                AddToLog($"Error processing custom extent: {ex.Message}");
            }
        }
        
        private void UpdateCustomExtentDisplay()
        {
            if (_customExtent != null)
            {
                CustomExtentDisplay = $"Min X: {_customExtent.XMin:F4}, Min Y: {_customExtent.YMin:F4}\nMax X: {_customExtent.XMax:F4}, Max Y: {_customExtent.YMax:F4}";
            }
            else
            {
                CustomExtentDisplay = "No custom extent set";
            }
        }
        
        private void BrowseOutputLocation()
        {
            var dialog = new System.Windows.Forms.FolderBrowserDialog
            {
                Description = "Select output location for downloaded datasets",
                SelectedPath = OutputPath
            };
            
            if (dialog.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                OutputPath = dialog.SelectedPath;
                AddToLog($"Output location set to: {OutputPath}");
            }
        }
        
        private void ClearFilters()
        {
            SearchText = string.Empty;
            SelectedOrganization = "All Organizations";
            SelectedFormat = "All Formats";
            FilterByMapExtent = false;
            AddToLog("Filters cleared");
        }
        
        private void DebugCheckState()
        {
            // Ensure this runs on the UI thread to avoid threading issues
            if (!Application.Current.Dispatcher.CheckAccess())
            {
                Application.Current.Dispatcher.Invoke(() => DebugCheckState());
                return;
            }
            
            AddToLog("🔍 DEBUG: Checking current state...");
            AddToLog($"🔍 IsLoading: {IsLoading}");
            AddToLog($"🔍 Total Datasets: {Datasets.Count}");
            AddToLog($"🔍 FilteredDatasets: {FilteredDatasets.Count}");
            
            AddToLog("🔍 Dataset details:");
            foreach (var dataset in FilteredDatasets)
            {
                AddToLog($"   📋 '{dataset.Name}' - IsSelected: {dataset.IsSelected}");
            }
            
            var canDownload = CanDownload();
            var selectedCount = FilteredDatasets.Count(d => d.IsSelected);
            AddToLog($"🔍 Selected count: {selectedCount}");
            AddToLog($"🔍 CanDownload(): {canDownload}");
            AddToLog($"🔍 Download button should be: {(canDownload ? "ENABLED" : "DISABLED")}");
            
            // Manual test: Select first dataset and try download
            if (FilteredDatasets.Any())
            {
                AddToLog("🧪 MANUAL TEST: Selecting first dataset...");
                var firstDataset = FilteredDatasets.First();
                
                AddToLog($"🧪 Before: {firstDataset.Name} IsSelected = {firstDataset.IsSelected}");
                firstDataset.IsSelected = true;
                AddToLog($"🧪 After: {firstDataset.Name} IsSelected = {firstDataset.IsSelected}");
                
                AddToLog("🧪 Manually triggering download command...");
                if (DownloadSelectedCommand.CanExecute(null))
                {
                    AddToLog("🧪 Command can execute - calling it now...");
                    DownloadSelectedCommand.Execute(null);
                }
                else
                {
                    AddToLog("🧪 Command CANNOT execute!");
                }
            }
        }
        
        private void AddToLog(string message)
        {
            try
            {
                var timestamp = DateTime.Now.ToString("HH:mm:ss");
                lock (_logLock)
                {
                    // Ensure _logOutput is not null
                    if (_logOutput == null)
                        _logOutput = new StringBuilder();
                        
                    _logOutput.AppendLine($"[{timestamp}] {message}");
                    LogOutputText = _logOutput.ToString();
                }
            }
            catch (Exception ex)
            {
                // If logging fails, write to debug output as fallback
                System.Diagnostics.Debug.WriteLine($"AddToLog failed: {ex.Message} | Original message: {message}");
            }
        }
        
        internal static void Show()
        {
            var pane = FrameworkApplication.DockPaneManager.Find(_dockPaneID);
            pane?.Activate();
        }
        
        protected new bool SetProperty<T>(ref T field, T value, [CallerMemberName] string propertyName = null)
        {
            if (!EqualityComparer<T>.Default.Equals(field, value))
            {
                field = value;
                NotifyPropertyChanged(propertyName);
                return true;
            }
            return false;
        }
        
        // Handle property changes in datasets to update command states
        private void OnDatasetPropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            if (e.PropertyName == nameof(SourceCoopDataset.IsSelected))
            {
                var dataset = sender as SourceCoopDataset;
                AddToLog($"🔄 Dataset selection changed: {dataset?.Name} = {dataset?.IsSelected}");
                
                // Update the download command's CanExecute state when dataset selection changes
                (DownloadSelectedCommand as RelayCommand)?.RaiseCanExecuteChanged();
                AddToLog($"🔄 Download command CanExecute state updated");
                
                // Log current selection state of all datasets
                var selectedCount = FilteredDatasets.Count(d => d.IsSelected);
                AddToLog($"🔍 Total selected datasets: {selectedCount} out of {FilteredDatasets.Count}");
            }
        }
    }
} 