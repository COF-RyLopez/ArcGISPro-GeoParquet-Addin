# ArcGIS Pro GeoParquet Add‑in

Hey there! Welcome to the **ArcGIS Pro GeoParquet Add‑in** repository. This project is designed to make it super simple for users—especially those new to GIS—to ingest cloud‑native data formats (like GeoParquet, CSV, and JSON) directly into ArcGIS Pro. Under the hood, we use [DuckDB](https://duckdb.org/) (via the DuckDB.NET API) to run in‑memory SQL queries and perform spatial data transformations. The add‑in now exports Overture Maps data directly to GeoParquet format, taking advantage of ArcGIS Pro 3.5's native GeoParquet support.

## Installation

### Quick Install
- Navigate to [ArcGIS](https://cofgisonline.maps.arcgis.com/home/item.html?id=8293d1220b7848848ce316b4fa3263b5)
- Press **Download** to download add-in
- Once finished downloading **double-click** the `ArcGISPro-GeoParquet-Addin.esriAddInX`
- This will install the 'ArcGISPro-GeoParquet-Addin.esriAddInX' into your **ArcGIS Pro**

### Install From Source

### Prerequisites
- **ArcGIS Pro 3.5** or later  
- **.NET 8 SDK** (ensure your Visual Studio setup targets .NET 8)  
- **ArcGIS Pro SDK for .NET** installed in Visual Studio

### Steps

1. **Clone or Download the Repo**  
   ```bash
   git clone https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin.git
   ```
2. **Open the Solution in Visual Studio**
- Launch **Visual Studio 2022** (or newer).
- Go to **File → Open → Project/Solution** and select `ArcGISPro-GeoParquet-Addin.sln`.

3. **Build the Add‑in**
- Make sure your build configuration is set to **Debug** or **Release** (your preference).
- Right‑click the project in **Solution Explorer** and select **Build**.
- If everything compiles successfully, a `.esriAddInX` file will be generated (usually in the `bin/Debug` or `bin/Release` folder).

4. **Install the Add‑in**
- Locate the generated `.esriAddInX` file (e.g., `ArcGISPro-GeoParquet-Addin.esriAddInX`) in your bin output folder.
- Double‑click the file to launch the ArcGIS Pro **Add-In Manager**, which will prompt you to install.
- Alternatively, copy the `.esriAddInX` file into your ArcGIS Pro add‑ins folder, typically found at:
  ```bash
   C:\Users\<YourUserName>\Documents\ArcGIS\AddIns\ArcGISPro
  ```
- Restart ArcGIS Pro if it's already open.

5. **Use the Add‑in in ArcGIS Pro**

- Open ArcGIS Pro.
- Look for the **Add‑In** tab on the ribbon.
- Click the **Launch Overture** (or similarly named) button to launch the dockpane.
- Follow the wizard steps to ingest, transform, and export your cloud‑native data!

## Troubleshooting
- **Missing ArcGIS Pro SDK Templates?**
- Ensure you have installed the [ArcGIS Pro SDK for .NET](https://pro.arcgis.com/en/pro-app/latest/sdk/) extension in Visual Studio.
- **Access Denied or Security Warnings?**
- If Windows or your organization's policy blocks add‑ins from unknown publishers, you may need to unblock the .esriAddInX file or add its path to your trusted locations in ArcGIS Pro's Add-In Manager.

## Why This Add‑in?

- **User‑Friendly:** A wizard‑style dockpane guides you through file ingestion, data preview/validation, transformation, and export.
- **Cloud‑Native Data:** Easily point to local files or public buckets (like S3 or Azure) and skip manual downloads.
- **Integrated Workflow:** Ingest, transform, and export data—all in one place!

## Key Features

1. **File Ingestion:**  
   - Enter a file URL or browse for a local file.
   - Ingest data directly via DuckDB's HTTP FS/S3 integration.

2. **Data Preview & Validation:**  
   - Quick attribute preview using a DataGrid.
   - Validate that your data is correctly loaded.

3. **Data Transformation:**  
   - Apply custom SQL queries using DuckDB's in‑memory engine.
   - Preview transformed data before export.

4. **Export to GeoParquet:**  
   - **Current Release:** Exports Overture Maps data directly to GeoParquet format.
   - Takes advantage of ArcGIS Pro 3.5's native GeoParquet support.
   - **Coming Soon:** Additional export formats such as File Geodatabases and GeoPackages.

## Project Structure

- **Views/**  
  - `WizardDockpane.xaml` / `WizardDockpane.xaml.cs`: The WPF UI for the wizard steps.
  - `WizardDockpaneViewModel.cs`: The dockpane's view model (inherits from `DockPane`).
- **Services/**  
  - `DataProcessor.cs`: Contains logic for ingesting data via DuckDB, applying transformations, and exporting data.
- **DuckDBGeoparquetModule.cs**  
  - The main module class for the ArcGIS Pro add‑in.
- **Config.daml**  
  - The add‑in manifest (DAML) that defines the module, buttons, and dockpane.

## Getting Started

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin.git
