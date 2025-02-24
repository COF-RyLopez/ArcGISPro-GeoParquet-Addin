# ArcGIS Pro GeoParquet Add‑in

Hey there! Welcome to the **ArcGIS Pro GeoParquet Add‑in** repository. This project is designed to make it super simple for users—especially those new to GIS—to ingest cloud‑native data formats (like GeoParquet, CSV, and JSON) directly into ArcGIS Pro. Under the hood, we use [DuckDB](https://duckdb.org/) (via the DuckDB.NET API) to run in‑memory SQL queries and perform spatial data transformations. Currently, the add‑in exports Overture Maps data to a shapefile, with plans to support additional export formats (such as File Geodatabases, GeoPackages, and Esri JSON) in future releases.

## Why This Add‑in?

- **User‑Friendly:** A wizard‑style dockpane guides you through file ingestion, data preview/validation, transformation, and export.
- **Cloud‑Native Data:** Easily point to local files or public buckets (like S3 or Azure) and skip manual downloads.
- **Integrated Workflow:** Ingest, transform, and export data—all in one place!

## Key Features

1. **File Ingestion:**  
   - Enter a file URL or browse for a local file.
   - Ingest data directly via DuckDB’s HTTP FS/S3 integration.

2. **Data Preview & Validation:**  
   - Quick attribute preview using a DataGrid.
   - Validate that your data is correctly loaded.

3. **Data Transformation:**  
   - Apply custom SQL queries using DuckDB’s in‑memory engine.
   - Preview transformed data before export.

4. **Export to Shapefile (Overture Maps Data):**  
   - **Current Release:** Exports Overture Maps data to a shapefile.
   - **Coming Soon:** Additional export formats such as File Geodatabases, GeoPackages, and Esri JSON will be supported.

## Project Structure

- **Views/**  
  - `WizardDockpane.xaml` / `WizardDockpane.xaml.cs`: The WPF UI for the wizard steps.
  - `WizardDockpaneViewModel.cs`: The dockpane’s view model (inherits from `DockPane`).
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
