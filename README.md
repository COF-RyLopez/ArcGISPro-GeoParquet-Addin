# ArcGIS Pro GeoParquet Add‑in

A powerful ArcGIS Pro add-in that simplifies working with cloud-native geospatial data formats, especially GeoParquet files. This tool uses [DuckDB](https://duckdb.org/) to deliver high-performance data processing directly within ArcGIS Pro, making it ideal for both GIS professionals and newcomers alike.

![ArcGIS Pro GeoParquet Add-in](https://raw.githubusercontent.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/main/Images/addin-icon.png)

## 🎥 See It In Action

Watch the add-in in action loading Overture Maps data with incredible performance:

[![Demo Video](https://img.shields.io/badge/▶️%20Demo%20Video-LinkedIn-blue?style=for-the-badge)](https://www.linkedin.com/posts/ryan-lopez-fresnocounty_now-the-arcgis-pro-35-brings-native-geoparquet-activity-7333914884641890307-sYOY)

📖 **Comprehensive Guide**: Mark Litwintschik wrote an excellent [step-by-step guide](https://tech.marksblogg.com/overture-maps-esri-arcgis-pro.html) covering installation and usage.

## Features

- **Native GeoParquet Support**: Fully leverages ArcGIS Pro 3.5's native GeoParquet capabilities for optimal performance
- **High-Performance Processing**: Optimized data pipeline with 5-15% performance improvements and smart empty dataset handling
- **Complex Data Preservation**: Maintains original data structure including nested types
- **Cloud-Native Integration**: Direct access to data in S3, Azure, and other cloud storage
- **Wizard-Driven Interface**: Simple step-by-step process with clean, focused progress reporting
- **Overture Maps Integration**: Specialized support for Overture Maps Foundation data
- **🔗 Bridge Files & Source Attribution**: NEW! Trace features back to their source datasets (OpenStreetMap, Microsoft, Meta, etc.)
- **✏️ Attribution Editing**: Edit and update source relationships for data quality assessment
- **In-Memory Processing**: Uses DuckDB for high-performance operations
- **Spatial Filtering**: Filter data by map extent before loading
- **Multi-File Feature Connections**: Automatically creates MFCs for efficient multi-dataset workflows
- **Incredible Performance**: Experience blazing-fast map redraws thanks to Parquet's optimized format

## 🔗 NEW: Bridge Files & Source Attribution

This add-in now includes comprehensive support for [Overture Maps Bridge Files](https://docs.overturemaps.org/gers/bridge-files/), enabling advanced source attribution workflows:

### What are Bridge Files?

Bridge files are part of Overture's Global Entity Reference System (GERS). They connect GERS IDs in Overture data to the IDs from underlying source datasets, providing transparency into data origins and conflation processes.

### Key Capabilities

- **🔍 Source Tracing**: Trace any Overture feature back to its original source datasets
- **📊 Attribution Analysis**: Understand which datasets contributed to each feature
- **🏢 Multi-Source Insights**: Identify features that combine data from multiple sources
- **✏️ Quality Assessment**: Evaluate data quality based on source attribution
- **💾 Enhanced Exports**: Export data with comprehensive source attribution metadata

### Supported Source Datasets

- OpenStreetMap
- Microsoft Places  
- Meta Places
- Esri Community Maps
- geoBoundaries
- Instituto Geográfico Nacional (España)
- PinMeTo

### Bridge Files Workflow

1. **Enable Bridge Files**: Turn on bridge files in the Configuration tab
2. **Load Overture Data**: Select and download your desired Overture Maps themes
3. **Load Bridge Files**: Use the "Bridge Files & Attribution" tab to download source attribution data
4. **Explore Attribution**: View detailed source breakdown and attribution statistics
5. **Edit Relationships**: Modify source attribution for custom analysis
6. **Export Enhanced Data**: Export datasets with complete source attribution information

## Installation

### Prerequisites

- **ArcGIS Pro 3.5 or later** (for optimal GeoParquet support)
- **Windows 10/11** (ArcGIS Pro requirement)
- **Internet connection** (for downloading Overture Maps data)

### Using the Installer (Recommended)

1. **Download** the latest `.esriAddinX` file from [Releases](https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/releases)
2. **Install** by double-clicking the `.esriAddinX` file
3. **Restart** ArcGIS Pro

### Using the Required Extensions

This add-in requires DuckDB extensions for S3 and spatial functionality:

1. **Download** `spatial.duckdb_extension` and `httpfs.duckdb_extension` from [DuckDB releases v1.2.0](https://github.com/duckdb/duckdb/releases/tag/v1.2.0)
2. **Place** these files in the `Extensions` folder within your add-in installation directory
3. See `Extensions/README.txt` for detailed instructions

## Quick Start

### Basic Overture Maps Data Loading

#### Step 1: Launch the Tool
- Open ArcGIS Pro and create or open a project
- In the **Add-In** tab, click **"Launch Overture"**

#### Step 2: Select Your Data
1. Choose themes (Buildings, Places, Transportation, etc.)
2. Select specific data types within each theme
3. Review your selections in the preview section

#### Step 3: Define Your Area of Interest
1. Select "Use Current Map Extent" to use your current view
2. Alternatively, use "Custom Extent" to define a specific area

#### Step 4: Load and Transform
1. Click "Load Data" to begin the download process
2. Monitor the progress through the clean, focused status reporting
3. The add-in handles all processing automatically with optimized performance:
   - Skips empty datasets for faster processing
   - Preserves complex data types
   - Maintains spatial relationships
   - Ensures proper georeferencing

#### Step 5: Visualization
1. Data is automatically added to your map once processed
2. Layers are named based on the Overture Maps theme and geometry type
3. All attributes are preserved in their original structure

### Bridge Files & Source Attribution Workflow

#### Step 1: Enable Bridge Files
1. Go to the **"Bridge Files & Attribution"** tab
2. Check **"Enable bridge files and source attribution"**
3. Note: This may increase download time but provides valuable source data

#### Step 2: Load Bridge Files
1. After loading Overture data, click **"Load Bridge Files for Current Data"**
2. Wait for the system to download and process attribution data
3. View the attribution summary showing:
   - Total features with attribution
   - Multi-source features
   - Source dataset breakdown

#### Step 3: Explore Source Attribution
1. **View Attribution Report**: Generate detailed reports showing source contribution statistics
2. **Dataset Breakdown**: See which source datasets contribute most to your data
3. **Multi-Source Analysis**: Identify features that combine multiple data sources

#### Step 4: Attribution Editing (Optional)
1. Select features in the map to view their source attribution
2. Use **"Get Selected Feature Attribution"** to see source details
3. Edit attribution relationships for custom analysis workflows

#### Step 5: Export Enhanced Data
1. Set your export path for attributed datasets
2. Choose options for including bridge timestamps and source record IDs
3. Click **"Export Attributed Dataset"** to save data with source attribution

### Advanced Features

- **Spatial Filtering**: Reduces data volume by filtering to your area of interest
- **Geometry-Based Layers**: Data is separated by geometry type for optimal display
- **Release Selection**: Choose specific Overture Maps data releases
- **Progress Tracking**: Detailed logging of each processing step
- **Source Attribution**: NEW! Understand data origins and quality through bridge files

#### Step 6: Multi-File Feature Connection (Optional)
1. After loading multiple datasets, you'll be prompted to create an MFC
2. Choose your preferred location for the MFC file
3. This enables more efficient workflows when working with multiple related datasets
4. The MFC appears in your Catalog for easy access and management

## Project Structure

- **Views/**  
  - `WizardDockpane.xaml` / `WizardDockpane.xaml.cs`: The main UI components
  - `WizardDockpaneViewModel.cs`: ViewModel controlling the UI logic and bridge files functionality
  - `SourceCoopDockpane.*`: Source Cooperative data browser components
- **Services/**  
  - `DataProcessor.cs`: Core data handling using DuckDB with bridge files support
  - `SourceCooperativeClient.cs`: Client for accessing Source Cooperative data
  - `MfcUtility.cs`: Multifile Feature Connection utilities
- **DuckDBGeoparquetModule.cs**: Main module class
- **Config.daml**: Add-in manifest defining components

## Bridge Files Technical Details

### Data Structure

Bridge files use the following schema:
- **id**: GERS ID (Global Entity Reference System identifier)
- **record_id**: Original source dataset record ID
- **update_time**: Timestamp of last attribution update
- **dataset**: Source dataset name (e.g., "OpenStreetMap", "Microsoft Places")
- **theme**: Overture theme (buildings, places, transportation, etc.)
- **type**: Specific data type within theme
- **between**: Array indicating portion of normalized length
- **dataset_between**: Array indicating dataset-specific portion

### S3 Path Structure

Bridge files are stored at:
```
s3://overturemaps-us-west-2/bridgefiles/{RELEASE}/dataset={DATASET}/theme={THEME}/type={TYPE}/*.parquet
```

### Attribution Export Schema

When exporting attributed datasets, the following additional columns are included:
- **source_record_id**: Original source data ID
- **source_dataset**: Primary contributing dataset
- **bridge_update_time**: Last attribution update timestamp
- **source_count**: Number of contributing source datasets
- **contributing_datasets**: Comma-separated list of all contributing datasets

## Troubleshooting

### Common Issues

#### Installation Problems
- **Missing SDK Templates**: Ensure you've installed the [ArcGIS Pro SDK for .NET](https://pro.arcgis.com/en/pro-app/latest/sdk/)

#### Bridge Files Issues
- **No Bridge Files Found**: Not all themes/types have bridge files available. Check the supported source datasets list.
- **Attribution Loading Errors**: Ensure you have loaded Overture data before attempting to load bridge files.
- **Export Failures**: Verify you have write permissions to the export directory.

### Performance Tips

- **Use Spatial Filtering**: Always apply spatial filtering to reduce data volume
- **Enable Bridge Files Selectively**: Only enable bridge files when you need source attribution analysis
- **Manage Export Size**: Bridge files can significantly increase export file sizes

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details.

## Support

- **Documentation**: [Overture Maps Documentation](https://docs.overturemaps.org/)
- **Bridge Files Guide**: [GERS Bridge Files Documentation](https://docs.overturemaps.org/gers/bridge-files/)
- **Issues**: [GitHub Issues](https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/issues)
- **Discussions**: [GitHub Discussions](https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/discussions)

## Acknowledgments

- **Overture Maps Foundation** for the amazing open geospatial data initiative and GERS system
- **DuckDB Team** for the high-performance analytical database
- **ArcGIS Pro Team** for native GeoParquet support
- **Community Contributors** who help improve this tool

---

*Experience the power of cloud-native geospatial data with full source attribution transparency!*
