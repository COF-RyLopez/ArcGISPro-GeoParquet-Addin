# ArcGIS Pro GeoParquet Add‑in

A powerful ArcGIS Pro add-in that simplifies working with cloud-native geospatial data formats, especially GeoParquet files. This tool uses [DuckDB](https://duckdb.org/) to deliver high-performance data processing directly within ArcGIS Pro, making it ideal for both GIS professionals and newcomers alike.

![ArcGIS Pro GeoParquet Add-in](https://cofgisonline.maps.arcgis.com/home/item.html?id=8293d1220b7848848ce316b4fa3263b5)

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
- **In-Memory Processing**: Uses DuckDB for high-performance operations
- **Spatial Filtering**: Filter data by map extent before loading
- **Multi-File Feature Connections**: Automatically creates MFCs for efficient multi-dataset workflows
- **Incredible Performance**: Experience blazing-fast map redraws thanks to Parquet's optimized format

## Requirements

- **ArcGIS Pro**: Version 3.5 or later
- **.NET SDK**: Version 8.0 or later
- **Storage**: Minimum 4GB free disk space for temporary data
- **Memory**: Minimum 8GB RAM (16GB recommended for large datasets)
- **For Developers**:
  - Visual Studio 2022 or newer
  - ArcGIS Pro SDK for .NET

## Installation

### Option 1: Quick Installation

1. Navigate to [ArcGIS Marketplace](https://cofgisonline.maps.arcgis.com/home/item.html?id=8293d1220b7848848ce316b4fa3263b5)
2. Click **Download** to get the add-in
3. Once downloaded, double-click the `ArcGISPro-GeoParquet-Addin.esriAddInX` file
4. Follow the installation prompts
5. Restart ArcGIS Pro if it's already running

### Option 2: Build from Source

1. **Clone the Repository**
   ```bash
   git clone https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin.git
   cd ArcGISPro-GeoParquet-Addin
   ```

2. **Open in Visual Studio**
   - Launch Visual Studio 2022
   - Go to **File → Open → Project/Solution**
   - Select `DuckDBGeoparquet.sln`

3. **Build the Add-in**
   - Set configuration to Debug or Release
   - Right-click the project in Solution Explorer
   - Select **Build**
   - Find the generated `.esriAddInX` file in the bin folder

4. **Install the Add-in**
   - Double-click the `.esriAddInX` file
   - Or copy it to: `C:\Users\<YourUserName>\Documents\ArcGIS\AddIns\ArcGISPro`
   - Restart ArcGIS Pro if it's already running

## Usage

### Basic Workflow

1. **Start ArcGIS Pro** and open a project
2. Navigate to the **Add-In** tab on the ribbon
3. Click the **Launch Overture** button to open the dockpane
4. Follow the wizard steps:
   - Select data source
   - Preview the data
   - Apply transformations (if needed)
   - Export to GeoParquet format

### Working with Overture Maps Data

#### Step 1: Select Overture Maps Theme
1. Open the Wizard Dockpane
2. Choose a theme from the dropdown (addresses, base, buildings, etc.)
3. Review the estimated data volume information

#### Step 2: Define Your Area of Interest
1. Select "Use Current Map Extent" to use your current view
2. Alternatively, use "Custom Extent" to define a specific area

#### Step 3: Load and Transform
1. Click "Load Data" to begin the download process
2. Monitor the progress through the clean, focused status reporting
3. The add-in handles all processing automatically with optimized performance:
   - Skips empty datasets for faster processing
   - Preserves complex data types
   - Maintains spatial relationships
   - Ensures proper georeferencing

#### Step 4: Visualization
1. Data is automatically added to your map once processed
2. Layers are named based on the Overture Maps theme and geometry type
3. All attributes are preserved in their original structure

#### Step 5: Multi-File Feature Connection (Optional)
1. After loading multiple datasets, you'll be prompted to create an MFC
2. Choose your preferred location for the MFC file
3. This enables more efficient workflows when working with multiple related datasets
4. The MFC appears in your Catalog for easy access and management

### Advanced Features

- **Spatial Filtering**: Reduces data volume by filtering to your area of interest
- **Geometry-Based Layers**: Data is separated by geometry type for optimal display
- **Release Selection**: Choose specific Overture Maps data releases
- **Progress Tracking**: Detailed logging of each processing step

## Project Structure

- **Views/**  
  - `WizardDockpane.xaml` / `WizardDockpane.xaml.cs`: The UI components
  - `WizardDockpaneViewModel.cs`: ViewModel controlling the UI logic
- **Services/**  
  - `DataProcessor.cs`: Core data handling using DuckDB
- **DuckDBGeoparquetModule.cs**: Main module class
- **Config.daml**: Add-in manifest defining components

## Troubleshooting

### Common Issues

#### Installation Problems
- **Missing SDK Templates**: Ensure you've installed the [ArcGIS Pro SDK for .NET](https://pro.arcgis.com/en/pro-app/latest/sdk/)
- **Access Denied Errors**: Right-click the .esriAddInX file → Properties → Unblock
- **Add-in Not Appearing**: Verify installation path and restart ArcGIS Pro completely

#### Data Loading Issues
- **Timeout Errors**: Large Overture datasets may require multiple attempts
- **Memory Errors**: Try reducing your area of interest or closing other applications
- **Missing Data**: Verify internet connectivity and cloud storage permissions

#### Performance Optimization
- **Slow Processing**: 
  - The add-in now automatically skips empty datasets for faster processing
  - Reduce the area of interest for very large regions
  - Close other resource-intensive applications
  - Ensure you have at least 8GB of available RAM
- **Large Files**: 
  - Recent optimizations provide 5-15% performance improvements
  - Consider using a machine with more RAM for very large areas
  - Process data in smaller geographic chunks
- **Improved Logging**: 
  - Cleaner, more focused progress reporting reduces visual noise
  - Essential debugging information is still preserved

### Debug Logging

The add-in maintains detailed logs that can help diagnose issues:
- Check the log output in the dockpane
- Review system logs for DuckDB-related errors
- Optimized logging reduces noise while preserving essential debugging information

## Recent Improvements

### Version 0.1.2 (Latest)
- **Version Synchronization**: Fixed version mismatch between releases and installed add-in
- **Automated Versioning**: Implemented CI/CD automation to sync versions from git tags

### Version 0.1.1
- **Enhanced DAML Configuration**: Improved UI integration with ArcGIS Pro standards
- **Better Accessibility**: Enhanced keytips and standard condition usage
- **Cleaner Codebase**: Removed unused UI elements and optimized configuration

### Version 0.1.0
- **Performance Boost**: 5-15% faster processing through optimized file operations
- **Smart Dataset Handling**: Automatically skips empty datasets (e.g., bathymetry in inland areas)  
- **Native GeoParquet**: Fully utilizes ArcGIS Pro 3.5's built-in GeoParquet support
- **Cleaner Logging**: 70% reduction in debug noise while maintaining essential information
- **Improved Reliability**: Enhanced error handling and retry logic

## Contributing

Contributions to this project are welcome! Here's how to get started:

1. Fork the repository
2. Create a feature branch: `git checkout -b new-feature`
3. Make your changes
4. Submit a pull request

Please adhere to the existing code style and include appropriate tests.

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## Media Coverage & Community

- 📺 **Demo Video**: [LinkedIn demonstration](https://www.linkedin.com/posts/ryan-lopez-fresnocounty_now-the-arcgis-pro-35-brings-native-geoparquet-activity-7333914884641890307-sYOY) by Ryan Lopez
- 📖 **Technical Guide**: [Comprehensive walkthrough](https://tech.marksblogg.com/overture-maps-esri-arcgis-pro.html) by Mark Litwintschik
- 🚀 **CI/CD Implementation**: [Automated release pipeline](https://www.linkedin.com/feed/update/urn:li:activity:7338847609953947648/) built by Youssef Harby
- 🏆 **Inspiration**: Built upon the excellent work of [Chris Holmes](https://github.com/cholmes) and his QGIS Overture plugin

## Acknowledgments

- [Overture Maps Foundation](https://overturemaps.org/) for providing open map data
- [DuckDB](https://duckdb.org/) for the powerful embedded database engine
- [ArcGIS Pro SDK](https://pro.arcgis.com/en/pro-app/latest/sdk/) for development tools
- [Chris Holmes](https://github.com/cholmes) for pioneering the DuckDB + Overture approach in QGIS
- [Mark Litwintschik](https://tech.marksblogg.com/) for the excellent technical documentation
- [Youssef Harby](https://www.linkedin.com/in/youssef-harby/) for implementing the professional CI/CD pipeline
- All contributors who have helped improve this add-in
