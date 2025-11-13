using System;
using System.IO;
using System.Threading.Tasks;
using DuckDBGeoparquet.Services;
using Amazon.S3;
using Amazon.S3.Model;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Google.Cloud.Storage.V1;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Service for uploading files directly to cloud storage providers using their native SDKs
    /// </summary>
    public class CloudStorageService
    {
        private readonly CloudProvider _provider;
        private readonly string _bucketName;
        private readonly string _region;
        private readonly string _basePath;

        // AWS S3 client
        private AmazonS3Client _s3Client;

        // Azure Blob client
        private BlobServiceClient _azureClient;
        private BlobContainerClient _azureContainerClient;

        // Google Cloud Storage client
        private StorageClient _gcsClient;

        /// <summary>
        /// Creates a new CloudStorageService instance
        /// </summary>
        /// <param name="provider">The cloud storage provider</param>
        /// <param name="bucketName">The bucket/container name</param>
        /// <param name="region">The region (for AWS S3)</param>
        /// <param name="basePath">Optional base path/folder in the bucket</param>
        /// <param name="accessKey">Access key/account name (provider-specific)</param>
        /// <param name="secretKey">Secret key/account key (provider-specific)</param>
        public CloudStorageService(
            CloudProvider provider,
            string bucketName,
            string region = null,
            string basePath = null,
            string accessKey = null,
            string secretKey = null)
        {
            _provider = provider;
            _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
            _region = region ?? "us-east-1";
            _basePath = basePath?.Trim().TrimStart('/').TrimEnd('/') ?? "";

            InitializeClient(accessKey, secretKey);
        }

        private void InitializeClient(string accessKey, string secretKey)
        {
            switch (_provider)
            {
                case CloudProvider.AwsS3:
                    if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
                    {
                        var config = new AmazonS3Config
                        {
                            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(_region)
                        };
                        _s3Client = new AmazonS3Client(accessKey, secretKey, config);
                    }
                    else
                    {
                        // Use default credentials from environment/IAM role
                        var defaultConfig = new AmazonS3Config
                        {
                            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(_region)
                        };
                        _s3Client = new AmazonS3Client(defaultConfig);
                    }
                    break;

                case CloudProvider.AzureBlobStorage:
                    if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
                    {
                        // Azure: accessKey is account name, secretKey is account key
                        var connectionString = $"DefaultEndpointsProtocol=https;AccountName={accessKey};AccountKey={secretKey};EndpointSuffix=core.windows.net";
                        _azureClient = new BlobServiceClient(connectionString);
                    }
                    else if (!string.IsNullOrEmpty(accessKey))
                    {
                        // Try using account name only (for managed identity scenarios)
                        var connectionString = $"DefaultEndpointsProtocol=https;AccountName={accessKey};EndpointSuffix=core.windows.net";
                        _azureClient = new BlobServiceClient(connectionString);
                    }
                    else
                    {
                        throw new ArgumentException("Azure Blob Storage requires account name and key");
                    }
                    _azureContainerClient = _azureClient.GetBlobContainerClient(_bucketName);
                    break;

                case CloudProvider.GoogleCloudStorage:
                    if (!string.IsNullOrEmpty(secretKey))
                    {
                        // Google Cloud: secretKey is the path to JSON credentials file
                        if (File.Exists(secretKey))
                        {
                            _gcsClient = StorageClient.Create(Google.Cloud.Storage.V1.StorageClient.Create(secretKey));
                        }
                        else
                        {
                            throw new FileNotFoundException($"Google Cloud credentials file not found: {secretKey}");
                        }
                    }
                    else
                    {
                        // Use default credentials from environment
                        _gcsClient = StorageClient.Create();
                    }
                    break;

                default:
                    throw new NotSupportedException($"Cloud provider {_provider} is not supported");
            }
        }

        /// <summary>
        /// Uploads a file to cloud storage
        /// </summary>
        /// <param name="localFilePath">Path to the local file to upload</param>
        /// <param name="remotePath">Remote path/key in the bucket (relative to basePath if set)</param>
        /// <param name="progress">Optional progress reporter</param>
        /// <returns>The full remote path of the uploaded file</returns>
        public async Task<string> UploadFileAsync(string localFilePath, string remotePath, IProgress<string> progress = null)
        {
            if (!File.Exists(localFilePath))
            {
                throw new FileNotFoundException($"Local file not found: {localFilePath}");
            }

            // Build the full remote path
            var fullRemotePath = string.IsNullOrEmpty(_basePath)
                ? remotePath.TrimStart('/')
                : $"{_basePath}/{remotePath.TrimStart('/')}";

            progress?.Report($"Uploading to {_provider}...");

            switch (_provider)
            {
                case CloudProvider.AwsS3:
                    return await UploadToS3Async(localFilePath, fullRemotePath, progress);

                case CloudProvider.AzureBlobStorage:
                    return await UploadToAzureAsync(localFilePath, fullRemotePath, progress);

                case CloudProvider.GoogleCloudStorage:
                    return await UploadToGCSAsync(localFilePath, fullRemotePath, progress);

                default:
                    throw new NotSupportedException($"Cloud provider {_provider} is not supported");
            }
        }

        private async Task<string> UploadToS3Async(string localFilePath, string remotePath, IProgress<string> progress)
        {
            var fileInfo = new FileInfo(localFilePath);
            progress?.Report($"Uploading {fileInfo.Name} ({fileInfo.Length / 1024 / 1024} MB) to S3...");

            var putRequest = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = remotePath.Replace('\\', '/'),
                FilePath = localFilePath,
                ContentType = "application/octet-stream"
            };

            // For Parquet files, set appropriate content type
            if (localFilePath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            {
                putRequest.ContentType = "application/parquet";
            }

            var response = await _s3Client.PutObjectAsync(putRequest);

            var s3Path = $"s3://{_bucketName}/{remotePath.Replace('\\', '/')}";
            progress?.Report($"✅ Uploaded to S3: {s3Path}");
            return s3Path;
        }

        private async Task<string> UploadToAzureAsync(string localFilePath, string remotePath, IProgress<string> progress)
        {
            var fileInfo = new FileInfo(localFilePath);
            progress?.Report($"Uploading {fileInfo.Name} ({fileInfo.Length / 1024 / 1024} MB) to Azure Blob Storage...");

            // Ensure container exists
            await _azureContainerClient.CreateIfNotExistsAsync(PublicAccessType.None);

            var blobClient = _azureContainerClient.GetBlobClient(remotePath.Replace('\\', '/'));

            var uploadOptions = new BlobUploadOptions
            {
                TransferOptions = new Azure.Storage.StorageTransferOptions
                {
                    MaximumConcurrency = 4,
                    InitialTransferSize = 4 * 1024 * 1024, // 4 MB
                    MaximumTransferSize = 4 * 1024 * 1024 * 1024L // 4 GB
                }
            };

            // Set content type for Parquet files
            if (localFilePath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            {
                uploadOptions.HttpHeaders = new BlobHttpHeaders
                {
                    ContentType = "application/parquet"
                };
            }

            using (var fileStream = File.OpenRead(localFilePath))
            {
                await blobClient.UploadAsync(fileStream, uploadOptions);
            }

            var azurePath = $"https://{_azureClient.AccountName}.blob.core.windows.net/{_bucketName}/{remotePath.Replace('\\', '/')}";
            progress?.Report($"✅ Uploaded to Azure Blob Storage: {azurePath}");
            return azurePath;
        }

        private async Task<string> UploadToGCSAsync(string localFilePath, string remotePath, IProgress<string> progress)
        {
            var fileInfo = new FileInfo(localFilePath);
            progress?.Report($"Uploading {fileInfo.Name} ({fileInfo.Length / 1024 / 1024} MB) to Google Cloud Storage...");

            var contentType = localFilePath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase)
                ? "application/parquet"
                : "application/octet-stream";

            using (var fileStream = File.OpenRead(localFilePath))
            {
                var blob = await _gcsClient.UploadObjectAsync(
                    _bucketName,
                    remotePath.Replace('\\', '/'),
                    contentType,
                    fileStream);
            }

            var gcsPath = $"gs://{_bucketName}/{remotePath.Replace('\\', '/')}";
            progress?.Report($"✅ Uploaded to Google Cloud Storage: {gcsPath}");
            return gcsPath;
        }

        /// <summary>
        /// Tests the connection to the cloud storage provider
        /// </summary>
        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                switch (_provider)
                {
                    case CloudProvider.AwsS3:
                        var s3Request = new ListObjectsV2Request
                        {
                            BucketName = _bucketName,
                            MaxKeys = 1
                        };
                        await _s3Client.ListObjectsV2Async(s3Request);
                        return true;

                    case CloudProvider.AzureBlobStorage:
                        await _azureContainerClient.GetPropertiesAsync();
                        return true;

                    case CloudProvider.GoogleCloudStorage:
                        await _gcsClient.GetBucketAsync(_bucketName);
                        return true;

                    default:
                        return false;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Connection test failed: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Disposes of the service and cleans up resources
        /// </summary>
        public void Dispose()
        {
            _s3Client?.Dispose();
            _azureClient?.Dispose();
            _gcsClient?.Dispose();
        }
    }
}

