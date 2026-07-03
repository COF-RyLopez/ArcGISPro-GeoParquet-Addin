import re

with open('Services/DataProcessor.cs', 'r') as f:
    content = f.read()

# 1. Remove LayerCreationInfo class from DataProcessor.cs
content = re.sub(r'(\s*// Structure to hold layer information for bulk creation.*?public class LayerCreationInfo.*?\{.*?\n    \}\n)', '', content, flags=re.DOTALL)

# 2. Add LayerManager and ParquetExporter to DataProcessor
new_fields = """
        public readonly LayerManager LayerManager;
        public readonly ParquetExporter ParquetExporter;
"""
content = re.sub(r'(private readonly GeocoderEngine _geocoder;\n)', r'\1' + new_fields, content)

# 3. Update Constructor
old_ctor = """        public DataProcessor()
        {
            _duckDb = new DuckDBManager();
            _geocoder = new GeocoderEngine(_duckDb);
            _pendingLayers = new List<LayerCreationInfo>();
        }"""
new_ctor = """        public DataProcessor()
        {
            _duckDb = new DuckDBManager();
            _geocoder = new GeocoderEngine(_duckDb);
            LayerManager = new LayerManager();
            ParquetExporter = new ParquetExporter(_duckDb, LayerManager);
        }"""
content = content.replace(old_ctor, new_ctor)

# 4. Remove all the old fields
content = re.sub(r'        // Default draw order fallback.*?};', '', content, flags=re.DOTALL)
content = re.sub(r'        // Collection to store layer information for bulk creation.*?;', '', content, flags=re.DOTALL)
content = re.sub(r'        // Fields to store theme context.*?;', '', content, flags=re.DOTALL)
content = re.sub(r'        private string _currentParentS3Theme;\n        private string _currentActualS3Type;\n', '', content)
content = re.sub(r'        private static readonly bool VerbosePerLayerDebugLogging = false;\n', '', content)
content = re.sub(r'        private readonly List<LayerCreationInfo> _pendingLayers;\n', '', content)
content = re.sub(r'        public int LastAddedLayerCount \{ get; private set; \}\n', '        public int LastAddedLayerCount => LayerManager.LastAddedLayerCount;\n', content)
content = re.sub(r'        public string LastAppliedStyleName \{ get; private set; \} = "default";\n', '        public string LastAppliedStyleName => LayerManager.LastAppliedStyleName;\n', content)
content = re.sub(r'        private MapStyleDefinition GetEffectiveSelectedStyle\(\).*?\}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private bool HasSelectedCartography =>.*?;', '', content)

# Remove the usages of _currentActualS3Type and _currentParentS3Theme inside IngestFileAsync
content = re.sub(r'                // Store the actualS3Type for context.*?_currentParentS3Theme = null;\n\n', '', content, flags=re.DOTALL)

# 5. Remove ExportPreviewSampleAsync body and make it a wrapper
content = re.sub(r'        public async Task ExportPreviewSampleAsync\(.*?\).*?\n        \}', 
                 '        public Task ExportPreviewSampleAsync(string s3Path, string outputGeoJsonPath, ExtentBounds extent = null, int maxFeatures = 2000, CancellationToken cancellationToken = default)\n            => ParquetExporter.ExportPreviewSampleAsync(s3Path, outputGeoJsonPath, extent, maxFeatures, cancellationToken);', 
                 content, flags=re.DOTALL)

# 6. Remove RemoveLayersUsingFileAsync, DeleteParquetFileAsync, DeleteParquetFile entirely
content = re.sub(r'        private static async Task RemoveLayersUsingFileAsync.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static async Task DeleteParquetFileAsync.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        // Keep the non-async version.*?private static void DeleteParquetFile.*?\n        \}\n', '', content, flags=re.DOTALL)

# 7. Update CreateFeatureLayerAsync to wrapper
create_layer_old = r'        public async Task CreateFeatureLayerAsync\(.*?\n        \}'
create_layer_new = '''        public Task CreateFeatureLayerAsync(string layerNameBase, IProgress<string> progress, string parentS3Theme, string actualS3Type, string dataOutputPathRoot, string compression = "ZSTD", CancellationToken cancellationToken = default)
            => ParquetExporter.CreateFeatureLayersAsync(layerNameBase, progress, parentS3Theme, actualS3Type, dataOutputPathRoot, _outputSessionSuffix, compression, cancellationToken);'''
content = re.sub(create_layer_old, create_layer_new, content, flags=re.DOTALL)

# 8. Remove ExportByGeometryType, ExportToGeoParquet, GetDescriptiveGeometryType
content = re.sub(r'        private async Task ExportByGeometryType\(.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private async Task<string> ExportToGeoParquet\(.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        // Helper method to get a more descriptive.*?private static string GetDescriptiveGeometryType.*?\n        \};\n', '', content, flags=re.DOTALL)

# 9. Update AddAllLayersToMapAsync to wrapper
add_all_old = r'        public async Task AddAllLayersToMapAsync\(.*?\n        \}'
add_all_new = '''        public Task AddAllLayersToMapAsync(IProgress<string> progress = null)
            => LayerManager.AddAllLayersToMapAsync(SelectedMapStyle, SelectedCartographicProfile, progress);'''
content = re.sub(add_all_old, add_all_new, content, flags=re.DOTALL)

# 10. Remove SortLayersByGeometryPriority, GetLayerDrawingRank, ResolveLayerType, GetGeometryGroup, AddLayerBatch, EnforceLayerDrawOrder, RemoveExistingMembersForTargetParquetFiles, NormalizeToParquetFilePath, AddLayerWithFallback, AddLayersWithFallbackBatch, EnsureTargetFileAvailableAsync, FallbackToIndividualLayerCreation, ApplyLayerSettings
content = re.sub(r'        private static List<LayerCreationInfo> SortLayersByGeometryPriority.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static int GetLayerDrawingRank.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static string ResolveLayerType.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static string GetGeometryGroup.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private List<LayerCreationInfo> AddLayerBatch.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static void EnforceLayerDrawOrder.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static int RemoveExistingMembersForTargetParquetFiles.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static string NormalizeToParquetFilePath.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private async Task AddLayerWithFallback.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private async Task AddLayersWithFallbackBatch.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static async Task<string> EnsureTargetFileAvailableAsync.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private async Task FallbackToIndividualLayerCreation.*?\n        \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'        private static void ApplyLayerSettings.*?\n        \}\n', '', content, flags=re.DOTALL)

# 11. ClearPendingLayers to wrapper
content = re.sub(r'        public void ClearPendingLayers\(\)\s*\{\s*_pendingLayers\.Clear\(\);\s*\}', 
                 '        public void ClearPendingLayers()\n        {\n            LayerManager.ClearPendingLayers();\n        }', content)

# 12. Remove AddLayerToMapAsync
content = re.sub(r'        private static async Task AddLayerToMapAsync.*?\n        \}\n', '', content, flags=re.DOTALL)

# Also fix the Dispose to not clear _pendingLayers, but instead clear LayerManager's pending layers.
content = content.replace('_pendingLayers?.Clear();', 'LayerManager?.ClearPendingLayers();')

# Remove dangling summary comments left over from methods we removed above
content = re.sub(r'\s*/// <summary>\s*/// Enforces deterministic top-to-bottom draw order.*?/// Must be called on the MCT.\s*/// </summary>\s*', '\n\n', content, flags=re.DOTALL)
content = re.sub(r'\s*/// <summary>\s*/// Ensure the target file path is available for writing.*?/// If still locked, returns a unique alternate path.\s*/// </summary>\s*', '\n\n', content, flags=re.DOTALL)
content = re.sub(r'\s*/// <summary>\s*/// Applies consistent settings to feature layers.*?/// Note: In Pro 3.5, accessing layer definitions for Parquet files may trigger domain lookups that fail\s*/// </summary>\s*', '\n\n', content, flags=re.DOTALL)
content = re.sub(r'        // Add static readonly field for the theme-type separator\s*private static readonly string\[\] THEME_TYPE_SEPARATOR = \[" - "\];\s*// Column exclusions are now defined in OvertureSchema.ColumnExclusions \(single source of truth\).\s*', '', content, flags=re.DOTALL)

with open('Services/DataProcessor.cs', 'w') as f:
    f.write(content)
