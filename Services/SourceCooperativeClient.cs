using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ArcGIS.Core.Geometry;
using System.Text.Json.Serialization;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Client for accessing Source Cooperative data repositories
    /// Source Cooperative is a data publishing utility by Radiant Earth
    /// </summary>
    public class SourceCooperativeClient : IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly JsonSerializerOptions _jsonOptions;
        private const string BASE_URL = "https://source.coop"; // Update when actual API endpoints are available
        
        // Configuration option to skip API calls and use direct S3 access
        private static readonly bool USE_API_DISCOVERY = false; // Set to false for reliable operation

        public SourceCooperativeClient()
        {
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Add("User-Agent", "ArcGIS-Pro-GeoParquet-Addin/1.0");
            _httpClient.Timeout = TimeSpan.FromSeconds(30); // Increase timeout for API calls
            
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        /// <summary>
        /// Get available repositories/datasets from Source Cooperative
        /// </summary>
        public async Task<List<SourceCooperativeRepository>> GetRepositoriesAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                // Check if we should skip API discovery and use reliable sample data
                if (!USE_API_DISCOVERY)
                {
                    System.Diagnostics.Debug.WriteLine("API discovery disabled - using reliable sample data with real S3 paths");
                    await Task.Delay(100, cancellationToken); // Simulate brief loading
                    return GetSampleRepositories();
                }
                
                // Try to fetch from real API first (only if enabled)
                try
                {
                    System.Diagnostics.Debug.WriteLine("Attempting to call Source Cooperative API...");
                    var response = await _httpClient.GetStringAsync("https://source.coop/api/v1/repositories?limit=100", cancellationToken);
                    System.Diagnostics.Debug.WriteLine($"API response received, length: {response?.Length ?? 0} characters");
                    
                    var apiResponse = JsonSerializer.Deserialize<SourceCooperativeApiResponse>(response, _jsonOptions);
                    
                    if (apiResponse?.Repositories != null)
                    {
                        System.Diagnostics.Debug.WriteLine($"Successfully parsed {apiResponse.Repositories.Count} repositories from API");
                        return ConvertApiRepositoriesToViewModel(apiResponse.Repositories);
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("API response parsed but no repositories found");
                    }
                }
                catch (TaskCanceledException ex)
                {
                    System.Diagnostics.Debug.WriteLine($"API call timed out: {ex.Message}. Falling back to sample data...");
                }
                catch (HttpRequestException ex)
                {
                    System.Diagnostics.Debug.WriteLine($"HTTP error calling API: {ex.Message}. Falling back to sample data...");
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Unexpected error calling API: {ex.Message}. Falling back to sample data...");
                }
                
                // Final fallback to sample data
                await Task.Delay(100, cancellationToken);
                System.Diagnostics.Debug.WriteLine("Using sample data as fallback");
                return GetSampleRepositories();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to fetch Source Cooperative repositories: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Discover datasets by directly listing S3 bucket contents
        /// This bypasses the API and discovers what's actually available
        /// </summary>
        public async Task<List<SourceCooperativeRepository>> GetRepositoriesFromS3Async(CancellationToken cancellationToken = default)
        {
            try
            {
                System.Diagnostics.Debug.WriteLine("Attempting S3 direct discovery...");
                
                // We can use DuckDB to list S3 bucket contents
                // This would require DuckDB with httpfs extension
                using var tempConnection = new DuckDB.NET.Data.DuckDBConnection("DataSource=:memory:");
                await tempConnection.OpenAsync(cancellationToken);
                
                using var command = tempConnection.CreateCommand();
                
                // Install extensions if needed
                try
                {
                    command.CommandText = "INSTALL httpfs; LOAD httpfs;";
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
                catch
                {
                    // Extensions might already be loaded
                }
                
                // List top-level directories in the Source Cooperative bucket
                command.CommandText = @"
                    SELECT DISTINCT split_part(key, '/', 1) as account,
                           split_part(key, '/', 2) as repository
                    FROM read_parquet('s3://us-west-2.opendata.source.coop/*/*/manifest.parquet')
                    WHERE key LIKE '%/%/%'
                    LIMIT 50
                ";
                
                var repositories = new List<SourceCooperativeRepository>();
                
                try
                {
                    using var reader = await command.ExecuteReaderAsync(cancellationToken);
                    var accountGroups = new Dictionary<string, List<(string account, string repository)>>();
                    
                    while (await reader.ReadAsync())
                    {
                        var account = reader.GetString("account");
                        var repository = reader.GetString("repository");
                        
                        if (!accountGroups.ContainsKey(account))
                            accountGroups[account] = new List<(string, string)>();
                        
                        accountGroups[account].Add((account, repository));
                    }
                    
                    // Convert to our repository structure
                    foreach (var group in accountGroups)
                    {
                        var repo = new SourceCooperativeRepository
                        {
                            Id = group.Key,
                            Name = FormatAccountName(group.Key),
                            Description = $"Datasets from {FormatAccountName(group.Key)}",
                            Organization = FormatAccountName(group.Key),
                            DatasetCount = group.Value.Count,
                            Url = $"https://source.coop/repositories/{group.Key}",
                            UpdatedDate = DateTime.Now,
                            Datasets = new List<SourceCooperativeDataset>()
                        };
                        
                        // Add datasets for this account
                        foreach (var (account, repoName) in group.Value)
                        {
                            var dataset = new SourceCooperativeDataset
                            {
                                Id = repoName,
                                Name = FormatRepositoryName(repoName),
                                Description = $"Dataset: {FormatRepositoryName(repoName)}",
                                Organization = FormatAccountName(account),
                                Format = "GeoParquet",
                                Url = $"s3://us-west-2.opendata.source.coop/{account}/{repoName}/*.parquet",
                                InfoUrl = $"https://source.coop/repositories/{account}/{repoName}",
                                Tags = InferTags(repoName),
                                UpdatedDate = DateTime.Now,
                                BoundingBox = InferBoundingBox(repoName)
                            };
                            
                            repo.Datasets.Add(dataset);
                        }
                        
                        repositories.Add(repo);
                    }
                    
                    System.Diagnostics.Debug.WriteLine($"S3 discovery found {repositories.Count} account groups with {repositories.Sum(r => r.DatasetCount)} total datasets");
                    return repositories;
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"S3 listing failed: {ex.Message}");
                    return new List<SourceCooperativeRepository>();
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"S3 discovery failed: {ex.Message}");
                return new List<SourceCooperativeRepository>();
            }
        }
        
        /// <summary>
        /// Get datasets within a specific repository
        /// </summary>
        public async Task<List<SourceCooperativeDataset>> GetRepositoryDatasetsAsync(string repositoryId, CancellationToken cancellationToken = default)
        {
            try
            {
                // For now, return sample data
                // Simulate async operation
                await Task.Delay(50, cancellationToken);
                return GetSampleDatasets(repositoryId);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to fetch datasets for repository {repositoryId}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Get download URL for a specific dataset
        /// </summary>
        public async Task<string> GetDatasetDownloadUrlAsync(string repositoryId, string datasetId, CancellationToken cancellationToken = default)
        {
            try
            {
                // In a real implementation, this would fetch the actual download URL
                // For now, return a placeholder that represents the expected structure
                
                // Simulate async operation
                await Task.Delay(25, cancellationToken);
                return $"{BASE_URL}/repositories/{repositoryId}/datasets/{datasetId}/download";
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to get download URL for dataset {datasetId}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Convert API repositories to our ViewModel format
        /// </summary>
        private List<SourceCooperativeRepository> ConvertApiRepositoriesToViewModel(List<SourceCooperativeApiRepository> apiRepositories)
        {
            var repositories = new List<SourceCooperativeRepository>();
            
            // Group by account for organization
            var groupedByAccount = apiRepositories.GroupBy(r => r.AccountId);
            
            foreach (var group in groupedByAccount)
            {
                var firstRepo = group.First();
                var organizationName = firstRepo.Meta?.Tags?.FirstOrDefault() ?? "Unknown Organization";
                
                var repository = new SourceCooperativeRepository
                {
                    Id = group.Key,
                    Name = organizationName,
                    Description = $"Data repository from {organizationName}",
                    Organization = organizationName,
                    DatasetCount = group.Count(),
                    Url = $"https://source.coop/repositories/{group.Key}",
                    UpdatedDate = DateTime.Now,
                    Datasets = new List<SourceCooperativeDataset>()
                };
                
                // Convert each API repository to a dataset
                foreach (var apiRepo in group)
                {
                    var dataset = new SourceCooperativeDataset
                    {
                        Id = apiRepo.RepositoryId,
                        Name = apiRepo.Meta?.Title ?? "Unknown Dataset",
                        Description = apiRepo.Meta?.Description ?? "No description available",
                        Organization = organizationName,
                        Format = "GeoParquet", // Assume GeoParquet format
                        Url = GetDatasetUrl(apiRepo),
                        InfoUrl = $"https://source.coop/repositories/{apiRepo.RepositoryId}",
                        Tags = apiRepo.Meta?.Tags ?? new string[0],
                        UpdatedDate = DateTime.Now,
                        // Use fallback bounding box since spatial metadata isn't in list API
                        BoundingBox = new double[] { -180.0, -90.0, 180.0, 90.0 }
                    };
                    
                    repository.Datasets.Add(dataset);
                }
                
                repositories.Add(repository);
            }
            
            return repositories;
        }
        
        /// <summary>
        /// Extract dataset URL from API repository data
        /// </summary>
        private string GetDatasetUrl(SourceCooperativeApiRepository apiRepo)
        {
            if (apiRepo.Data?.Mirrors?.Any() == true)
            {
                var firstMirror = apiRepo.Data.Mirrors.First();
                if (firstMirror.ContainsKey("aws"))
                {
                    var awsPrefix = firstMirror["aws"];
                    return $"s3://us-west-2.opendata.source.coop/{awsPrefix}";
                }
            }
            
            return $"https://data.source.coop/{apiRepo.RepositoryId}";
        }

        /// <summary>
        /// Get sample repositories/datasets based on Chris Holmes' QGIS GeoParquet downloader presets
        /// These are real, accessible Source Cooperative datasets
        /// </summary>
        private List<SourceCooperativeRepository> GetSampleRepositories()
        {
            return new List<SourceCooperativeRepository>
            {
                new SourceCooperativeRepository
                {
                    Id = "planet",
                    Name = "Planet Labs",
                    Description = "Satellite imagery and derived agricultural datasets",
                    Organization = "Planet Labs",
                    Url = "https://source.coop/repositories/planet",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "planet_eu_boundaries",
                            Name = "Planet EU Field Boundaries (2022)",
                            Description = "Agricultural field boundaries across the European Union derived from satellite imagery",
                            Organization = "Planet Labs",
                            Format = "GeoParquet",
                            Url = "https://data.source.coop/planet/eu-field-boundaries/field_boundaries.parquet",
                            InfoUrl = "https://source.coop/repositories/planet/eu-field-boundaries/description",
                            SizeBytes = 2_500_000_000, // ~2.5GB estimated
                            NeedsValidation = false,
                            BoundingBox = new double[] { -10.0, 35.0, 32.0, 72.0 } // European Union extent
                        }
                    }
                },
                new SourceCooperativeRepository
                {
                    Id = "fiboa",
                    Name = "FIBOA (Field Boundaries for Agriculture)",
                    Description = "Standardized agricultural field boundary datasets",
                    Organization = "FIBOA Initiative",
                    Url = "https://source.coop/repositories/fiboa",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "usda_crop",
                            Name = "USDA Crop Sequence Boundaries",
                            Description = "United States Department of Agriculture crop sequence boundaries with detailed agricultural data",
                            Organization = "USDA / FIBOA",
                            Format = "GeoParquet",
                            Url = "https://data.source.coop/fiboa/us-usda-cropland/us_usda_cropland.parquet",
                            InfoUrl = "https://source.coop/fiboa/us-usda-cropland/description",
                            SizeBytes = 8_500_000_000, // ~8.5GB estimated
                            NeedsValidation = false,
                            BoundingBox = new double[] { -125.0, 25.0, -66.0, 49.0 } // Continental United States
                        },
                        new SourceCooperativeDataset
                        {
                            Id = "ca_crop",
                            Name = "California Crop Mapping",
                            Description = "Detailed crop mapping data for California with seasonal agricultural information",
                            Organization = "California / FIBOA",
                            Format = "GeoParquet",
                            Url = "https://data.source.coop/fiboa/us-ca-scm/us_ca_scm.parquet",
                            InfoUrl = "https://source.coop/repositories/fiboa/us-ca-scm/description",
                            SizeBytes = 1_200_000_000, // ~1.2GB estimated
                            NeedsValidation = false,
                            BoundingBox = new double[] { -124.4, 32.5, -114.1, 42.0 } // California state extent
                        }
                    }
                },
                new SourceCooperativeRepository
                {
                    Id = "vida",
                    Name = "VIDA (Versatile Infrastructure Data Analytics)",
                    Description = "Combined building footprints from Google, Microsoft, and OpenStreetMap",
                    Organization = "VIDA",
                    Url = "https://source.coop/repositories/vida",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "vida_buildings",
                            Name = "VIDA Google/Microsoft/OSM Buildings",
                            Description = "Comprehensive building footprints combining data from Google, Microsoft, and OpenStreetMap by country",
                            Organization = "VIDA",
                            Format = "GeoParquet",
                            Url = "s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country/*/*.parquet",
                            InfoUrl = "https://source.coop/repositories/vida/google-microsoft-osm-open-buildings/description",
                            SizeBytes = 45_000_000_000, // ~45GB estimated
                            NeedsValidation = false,
                            BoundingBox = new double[] { -180.0, -85.0, 180.0, 85.0 } // Global coverage (excluding polar regions)
                        }
                    }
                },
                new SourceCooperativeRepository
                {
                    Id = "wherobots",
                    Name = "Wherobots",
                    Description = "Geospatial data processing and infrastructure datasets",
                    Organization = "Wherobots",
                    Url = "https://source.coop/repositories/wherobots",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "us_structures",
                            Name = "US Structures from ORNL by Wherobots",
                            Description = "Comprehensive structural data across the United States from Oak Ridge National Laboratory",
                            Organization = "ORNL / Wherobots",
                            Format = "GeoParquet",
                            Url = "s3://us-west-2.opendata.source.coop/wherobots/usa-structures/geoparquet/*.parquet",
                            InfoUrl = "https://source.coop/wherobots/usa-structures/geoparquet",
                            SizeBytes = 12_000_000_000, // ~12GB estimated
                            NeedsValidation = false,
                            BoundingBox = new double[] { -158.0, 18.0, -66.0, 72.0 } // United States including Alaska and Hawaii
                        }
                    }
                },
                new SourceCooperativeRepository
                {
                    Id = "fused",
                    Name = "Fused",
                    Description = "Fused geospatial data processing and partitioned datasets",
                    Organization = "Fused",
                    Url = "https://source.coop/repositories/fused",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "fsq_places_fused",
                            Name = "Foursquare Open Source Places - Fused-partitioned",
                            Description = "Foursquare's open source places database, partitioned and optimized by Fused for better performance",
                            Organization = "Foursquare / Fused",
                            Format = "GeoParquet",
                            Url = "s3://us-west-2.opendata.source.coop/fused/fsq-os-places/2025-01-10/places/*.parquet",
                            InfoUrl = "https://source.coop/repositories/fused/fsq-os-places/description",
                            SizeBytes = 18_000_000_000, // ~18GB estimated
                            NeedsValidation = false,
                            BoundingBox = new double[] { -180.0, -85.0, 180.0, 85.0 } // Global places coverage
                        }
                    }
                },
                new SourceCooperativeRepository
                {
                    Id = "cholmes",
                    Name = "Chris Holmes (Experimental)",
                    Description = "Experimental datasets and prototypes by Chris Holmes",
                    Organization = "Individual Contributor",
                    Url = "https://source.coop/repositories/cholmes",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "nhd_flowlines",
                            Name = "NHD Flowlines (experimental)",
                            Description = "National Hydrography Dataset flowlines in experimental GeoParquet format",
                            Organization = "USGS / Chris Holmes",
                            Format = "GeoParquet",
                            Url = "https://data.source.coop/cholmes/nhd/NHDFlowline.parquet",
                            InfoUrl = "https://source.coop/repositories/cholmes/nhd/description",
                            SizeBytes = 3_800_000_000, // ~3.8GB estimated
                            NeedsValidation = true,
                            BoundingBox = new double[] { -158.0, 18.0, -66.0, 72.0 } // United States water features
                        }
                    }
                },
                new SourceCooperativeRepository
                {
                    Id = "kerner-lab",
                    Name = "Kerner Lab",
                    Description = "Agricultural field boundary datasets from Kerner Lab research",
                    Organization = "Kerner Lab",
                    Url = "https://source.coop/repositories/kerner-lab",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "fields-of-the-world-france",
                            Name = "Fields of the World: France",
                            Description = "Agricultural field boundaries for France from national agricultural agency",
                            Organization = "Kerner Lab",
                            Format = "GeoParquet",
                            Url = "s3://us-west-2.opendata.source.coop/kerner-lab/fields-of-the-world-france/*.parquet",
                            InfoUrl = "https://source.coop/repositories/kerner-lab/fields-of-the-world-france",
                            SizeBytes = 500_000_000,
                            BoundingBox = new double[] { -5.1, 41.3, 9.6, 51.1 } // France
                        },
                        new SourceCooperativeDataset
                        {
                            Id = "fields-of-the-world-germany",
                            Name = "Fields of the World: Germany",
                            Description = "Agricultural field boundaries for Germany from Fusion Competition dataset",
                            Organization = "Kerner Lab",
                            Format = "GeoParquet",
                            Url = "s3://us-west-2.opendata.source.coop/kerner-lab/fields-of-the-world-germany/*.parquet",
                            InfoUrl = "https://source.coop/repositories/kerner-lab/fields-of-the-world-germany",
                            SizeBytes = 400_000_000,
                            BoundingBox = new double[] { 5.9, 47.3, 15.0, 55.1 } // Germany
                        },
                        new SourceCooperativeDataset
                        {
                            Id = "fields-of-the-world-kenya",
                            Name = "Fields of the World: Kenya",
                            Description = "Agricultural field boundaries for Kenya from ECAAS Project",
                            Organization = "Kerner Lab",
                            Format = "GeoParquet",
                            Url = "s3://us-west-2.opendata.source.coop/kerner-lab/fields-of-the-world-kenya/*.parquet",
                            InfoUrl = "https://source.coop/repositories/kerner-lab/fields-of-the-world-kenya",
                            SizeBytes = 200_000_000,
                            BoundingBox = new double[] { 33.9, -4.7, 41.9, 5.5 } // Kenya
                        }
                    }
                },
                new SourceCooperativeRepository
                {
                    Id = "bkr",
                    Name = "Brookhaven National Lab",
                    Description = "Climate and weather datasets from Brookhaven National Laboratory",
                    Organization = "Brookhaven National Lab",
                    Url = "https://source.coop/repositories/bkr",
                    Datasets = new List<SourceCooperativeDataset>
                    {
                        new SourceCooperativeDataset
                        {
                            Id = "geos",
                            Name = "GEOS Atmosphere Composition Analysis",
                            Description = "GEOS-CF High temporal resolution analysis in Icechunk format - 15-minute, 0.25 degree resolution",
                            Organization = "Brookhaven National Lab",
                            Format = "Zarr/Icechunk",
                            Url = "s3://us-west-2.opendata.source.coop/bkr/geos/*.zarr",
                            InfoUrl = "https://source.coop/repositories/bkr/geos",
                            SizeBytes = 10_000_000_000,
                            BoundingBox = new double[] { -180.0, -90.0, 180.0, 90.0 } // Global
                        },
                        new SourceCooperativeDataset
                        {
                            Id = "precipradar",
                            Name = "International Precipitation Radar",
                            Description = "Precipitation radar estimates from UK, Finland and other countries in optimized Icechunk format",
                            Organization = "Brookhaven National Lab",
                            Format = "Zarr/Icechunk",
                            Url = "s3://us-west-2.opendata.source.coop/bkr/precipradar/*.zarr",
                            InfoUrl = "https://source.coop/repositories/bkr/precipradar",
                            SizeBytes = 5_000_000_000,
                            BoundingBox = new double[] { -10.0, 50.0, 35.0, 70.0 } // Europe
                        }
                    }
                }
            };
        }

        /// <summary>
        /// Get additional high-quality datasets from the "other" category in Chris Holmes' presets
        /// These are direct datasets not hosted on Source Cooperative but are important GeoParquet sources
        /// </summary>
        private List<SourceCooperativeDataset> GetOtherDatasets()
        {
            return new List<SourceCooperativeDataset>
            {
                new SourceCooperativeDataset
                {
                    Id = "foursquare_places",
                    Name = "Foursquare Places",
                    Description = "Comprehensive global places database from Foursquare's open source dataset on HuggingFace",
                    Organization = "Foursquare",
                    Format = "GeoParquet",
                    Url = "hf://datasets/foursquare/fsq-os-places/release/dt=2025-02-06/places/parquet/*.parquet",
                    InfoUrl = "https://docs.foursquare.com/data-products/docs/places-overview",
                    SizeBytes = 35_000_000_000, // ~35GB estimated
                    NeedsValidation = false,
                    GeometryType = "Point",
                    Crs = "EPSG:4326",
                    BoundingBox = new double[] { -180.0, -90.0, 180.0, 90.0 },
                    DownloadUrl = "hf://datasets/foursquare/fsq-os-places/release/dt=2025-02-06/places/parquet/*.parquet",
                    CreatedDate = DateTime.Parse("2025-02-06"),
                    UpdatedDate = DateTime.Now.AddDays(-15)
                }
            };
        }

        /// <summary>
        /// Get sample datasets for a given repository
        /// </summary>
        private List<SourceCooperativeDataset> GetSampleDatasets(string repositoryId)
        {
            // Extract datasets from the specified repository
            var repositories = GetSampleRepositories();
            var repository = repositories.FirstOrDefault(r => r.Id == repositoryId);
            
            if (repository?.Datasets != null)
            {
                // Set additional properties for the datasets based on their type
                foreach (var dataset in repository.Datasets)
                {
                    // Set geometry types and coordinate systems based on dataset type
                    if (dataset.Id.Contains("buildings") || dataset.Id.Contains("crop") || dataset.Id.Contains("boundaries"))
                    {
                        dataset.GeometryType = "Polygon";
                    }
                    else if (dataset.Id.Contains("flowlines"))
                    {
                        dataset.GeometryType = "LineString";
                    }
                    else if (dataset.Id.Contains("places"))
                    {
                        dataset.GeometryType = "Point";
                    }
                    else
                    {
                        dataset.GeometryType = "Mixed";
                    }
                    
                    // Set coordinate system
                    dataset.Crs = "EPSG:4326";
                    
                    // Set bounding boxes based on dataset scope - only if not already set
                    if (dataset.BoundingBox == null || dataset.BoundingBox.Length == 0)
                    {
                        if (dataset.Id.Contains("us_") || dataset.Id.Contains("usda"))
                        {
                            dataset.BoundingBox = new double[] { -179.0, 18.0, -66.0, 71.0 }; // USA bounds
                        }
                        else if (dataset.Id.Contains("ca_") || dataset.Id.Contains("california"))
                        {
                            dataset.BoundingBox = new double[] { -124.4, 32.5, -114.1, 42.0 }; // California bounds
                        }
                        else if (dataset.Id.Contains("eu_"))
                        {
                            dataset.BoundingBox = new double[] { -31.3, 27.6, 69.1, 81.9 }; // Europe bounds
                        }
                        else
                        {
                            dataset.BoundingBox = new double[] { -180.0, -90.0, 180.0, 90.0 }; // Global bounds
                        }
                    }
                    
                    // Set download URL to the URL from the repository dataset
                    dataset.DownloadUrl = dataset.Url;
                    
                    // Set dates
                    dataset.CreatedDate = DateTime.Parse("2022-01-01");
                    dataset.UpdatedDate = DateTime.Now.AddDays(-30);
                }
                
                return repository.Datasets.ToList();
            }
            
            return new List<SourceCooperativeDataset>();
        }

        /// <summary>
        /// Format account names for display
        /// </summary>
        private string FormatAccountName(string account)
        {
            return account switch
            {
                "bkr" => "Brookhaven National Lab",
                "kerner-lab" => "Kerner Lab",
                "earthgenome" => "Earth Genome", 
                "source" => "Source Cooperative",
                "fused" => "Fused",
                "planet" => "Planet Labs",
                _ => account.Replace("-", " ").Replace("_", " ")
            };
        }
        
        /// <summary>
        /// Format repository names for display
        /// </summary>
        private string FormatRepositoryName(string repository)
        {
            return repository switch
            {
                "fields-of-the-world-kenya" => "Fields of the World: Kenya",
                "fields-of-the-world-france" => "Fields of the World: France",
                "fields-of-the-world-germany" => "Fields of the World: Germany",
                "aoml" => "AOML Ocean Observations",
                "geos" => "GEOS Atmosphere Analysis", 
                "nsrdb" => "NREL Solar Radiation Database",
                "precipradar" => "International Precipitation Radar",
                _ => repository.Replace("-", " ").Replace("_", " ")
            };
        }
        
        /// <summary>
        /// Infer tags based on repository name
        /// </summary>
        private string[] InferTags(string repository)
        {
            var tags = new List<string>();
            
            if (repository.Contains("field") || repository.Contains("crop") || repository.Contains("agriculture"))
                tags.AddRange(new[] { "agriculture", "field boundaries" });
                
            if (repository.Contains("weather") || repository.Contains("climate") || repository.Contains("precip"))
                tags.AddRange(new[] { "weather", "climate" });
                
            if (repository.Contains("ocean") || repository.Contains("marine"))
                tags.AddRange(new[] { "ocean", "marine" });
                
            if (repository.Contains("solar") || repository.Contains("radiation"))
                tags.AddRange(new[] { "solar", "energy" });
                
            return tags.ToArray();
        }
        
        /// <summary>
        /// Infer bounding box based on repository name
        /// </summary>
        private double[] InferBoundingBox(string repository)
        {
            // Geographic inference based on dataset names
            if (repository.Contains("kenya")) return new double[] { 33.9, -4.7, 41.9, 5.5 };
            if (repository.Contains("france")) return new double[] { -5.1, 41.3, 9.6, 51.1 };
            if (repository.Contains("germany")) return new double[] { 5.9, 47.3, 15.0, 55.1 };
            if (repository.Contains("india")) return new double[] { 68.2, 6.8, 97.4, 37.1 };
            if (repository.Contains("finland")) return new double[] { 20.5, 59.8, 31.6, 70.1 };
            if (repository.Contains("denmark")) return new double[] { 8.1, 54.6, 12.7, 57.8 };
            if (repository.Contains("estonia")) return new double[] { 21.8, 57.5, 28.2, 59.7 };
            if (repository.Contains("croatia")) return new double[] { 13.5, 42.4, 19.4, 46.5 };
            if (repository.Contains("cambodia")) return new double[] { 102.3, 10.4, 107.6, 14.7 };
            if (repository.Contains("corsica")) return new double[] { 8.5, 41.4, 9.6, 43.0 };
            
            // US datasets
            if (repository.Contains("aoml") || repository.Contains("nsrdb") || repository.Contains("precipradar"))
                return new double[] { -180.0, 18.0, -66.0, 72.0 };
                
            // Global datasets
            return new double[] { -180.0, -90.0, 180.0, 90.0 };
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }

    /// <summary>
    /// Represents a Source Cooperative repository
    /// </summary>
    public class SourceCooperativeRepository
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public string Organization { get; set; } = string.Empty;
        public int DatasetCount { get; set; }
        public string[] Topics { get; set; } = Array.Empty<string>();
        public string Url { get; set; } = string.Empty;
        public DateTime? CreatedDate { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public List<SourceCooperativeDataset> Datasets { get; set; } = new();
    }

    /// <summary>
    /// Represents a Source Cooperative dataset
    /// </summary>
    public class SourceCooperativeDataset
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public string Format { get; set; } = string.Empty;
        public long? SizeBytes { get; set; }
        public SourceCooperativeSpatialExtent SpatialExtent { get; set; } = new();
        public string[] Tags { get; set; } = Array.Empty<string>();
        public DateTime? LastModified { get; set; }
        public string License { get; set; } = string.Empty;
        public string GeometryType { get; set; } = string.Empty;
        public string Crs { get; set; } = string.Empty;
        public double[] BoundingBox { get; set; } = Array.Empty<double>();
        public string DownloadUrl { get; set; } = string.Empty;
        public DateTime? CreatedDate { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public string Organization { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
        public string InfoUrl { get; set; } = string.Empty;
        public bool NeedsValidation { get; set; }
        
        // Helper properties for display
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
    }

    /// <summary>
    /// Spatial extent for Source Cooperative datasets
    /// </summary>
    public class SourceCooperativeSpatialExtent
    {
        public double MinX { get; set; }
        public double MinY { get; set; }
        public double MaxX { get; set; }
        public double MaxY { get; set; }
    }

    /// <summary>
    /// API response models for Source Cooperative API
    /// </summary>
    public class SourceCooperativeApiResponse
    {
        [JsonPropertyName("repositories")]
        public List<SourceCooperativeApiRepository> Repositories { get; set; } = new();
        
        [JsonPropertyName("next")]
        public string Next { get; set; } = string.Empty;
        
        [JsonPropertyName("count")]
        public int Count { get; set; }
    }

    public class SourceCooperativeApiRepository
    {
        [JsonPropertyName("repository_id")]
        public string RepositoryId { get; set; } = string.Empty;
        
        [JsonPropertyName("account_id")]
        public string AccountId { get; set; } = string.Empty;
        
        [JsonPropertyName("published")]
        public bool Published { get; set; }
        
        [JsonPropertyName("disabled")]
        public bool Disabled { get; set; }
        
        [JsonPropertyName("data_mode")]
        public string DataMode { get; set; } = string.Empty;
        
        [JsonPropertyName("meta")]
        public SourceCooperativeApiMeta Meta { get; set; } = new();
        
        [JsonPropertyName("data")]
        public SourceCooperativeApiData Data { get; set; } = new();
    }

    public class SourceCooperativeApiMeta
    {
        [JsonPropertyName("title")]
        public string Title { get; set; } = string.Empty;
        
        [JsonPropertyName("description")]
        public string Description { get; set; } = string.Empty;
        
        [JsonPropertyName("tags")]
        public string[] Tags { get; set; } = Array.Empty<string>();
    }

    public class SourceCooperativeApiData
    {
        [JsonPropertyName("mirrors")]
        public List<Dictionary<string, string>> Mirrors { get; set; } = new();
    }
} 