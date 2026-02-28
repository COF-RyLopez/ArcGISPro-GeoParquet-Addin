# Test Plan: ArcGIS Pro GeoParquet Add-in

This document outlines the manual testing procedures for the ArcGIS Pro GeoParquet Add-in. Use this checklist to verify functionality before a new release.

## 📋 Prerequisites

*   **ArcGIS Pro**: Version 3.5 or later.
*   **Network**: Active internet connection (for downloading Overture Maps data from S3).
*   **System**: 
    *   Minimum 8GB RAM.
    *   ~5GB free disk space for temporary Parquet files.
*   **Clean State**: Ensure no previous versions of the add-in are installed (or uninstall them first).

---

## 🚀 1. Installation & Startup (Smoke Test)

| ID | Test Case | Steps | Expected Result | Pass/Fail |
| :--- | :--- | :--- | :--- | :--- |
| **1.1** | **Clean Install** | 1. Double-click `.esriAddInX` file.<br>2. Click "Install Add-In".<br>3. Open ArcGIS Pro. | Add-in installs without error. "Add-In Manager" lists "GeoParquet Add-in". | |
| **1.2** | **Ribbon UI** | 1. Open a new Map project.<br>2. Go to "Add-In" tab.<br>3. Locate "Launch Overture" button. | Button is visible and enabled. Icon renders correctly (light/dark mode). | |
| **1.3** | **Dockpane Open** | 1. Click "Launch Overture". | The "Overture Maps Data" dockpane opens on the right/left side. | |
| **1.4** | **DuckDB Init** | 1. Observe the dockpane immediately after opening. | No error messages about "Failed to load DuckDB extensions". Status should be ready. | |

---

## 🌍 2. Core Functionality: Loading Data

| ID | Test Case | Steps | Expected Result | Pass/Fail |
| :--- | :--- | :--- | :--- | :--- |
| **2.1** | **Theme Selection** | 1. In Wizard Step 1, select "Buildings" from dropdown.<br>2. Select "Places" from dropdown. | Dropdown works. UI updates to reflect selection (if applicable descriptions change). | |
| **2.2** | **Extent: Current Map** | 1. Zoom the map to a **small** area (e.g., a few city blocks in Fresno).<br>2. Select "Use Current Map Extent".<br>3. Click "Next". | The add-in captures the correct coordinates (visible in logs or debug output). | |
| **2.3** | **Load Small Dataset** | 1. Select "Places" theme (usually smaller/faster).<br>2. Use small extent.<br>3. Click "Load Data". | Progress bar updates. <br>Status messages show "Connecting to S3", "Reading schema", "Loading...".<br>Layer appears in TOC. | |
| **2.4** | **Layer Verification** | 1. Right-click the new layer > Attribute Table.<br>2. Check geometry. | Table opens. Attributes are populated. Points/Polygons render correctly on map. | |
| **2.5** | **Symbology** | 1. Load "Buildings" theme.<br>2. Check map visualization. | Buildings layer has distinct symbology (not just default random color). <br>Verify `.lyrx` was applied if available. | |

---

## 🧪 3. Advanced Scenarios & Edge Cases

| ID | Test Case | Steps | Expected Result | Pass/Fail |
| :--- | :--- | :--- | :--- | :--- |
| **3.1** | **Empty Extent** | 1. Pan map to the middle of the ocean (where no buildings exist).<br>2. Load "Buildings" theme. | Add-in should handle empty result gracefully.<br>Log/Status should say "Dataset is empty - no features to process".<br>No empty layer added to map. | |
| **3.2** | **Large Extent (Stress)** | 1. Zoom out to encompass an entire county.<br>2. Load "Admins" or "Places" (avoid Buildings for this test unless on high-RAM machine). | Processing takes longer but does not crash.<br>UI remains responsive (mostly).<br>Final layer loads correctly. | |
| **3.3** | **Multi-Layer Stacking** | 1. Load "Transportation" (lines).<br>2. Load "Buildings" (polygons).<br>3. Load "Places" (points). | Layers should stack logically in TOC:<br>Points (Places) on top.<br>Lines (Transport) middle.<br>Polygons (Buildings) bottom. | |
| **3.4** | **Cancel Operation** | 1. Start loading a large dataset (e.g., Buildings for a city).<br>2. Click "Cancel" (if button exists) or close Dockpane mid-process. | *Note: If cancellation isn't implemented yet, verify it doesn't crash Pro, just runs in background until done.* | |
| **3.5** | **Re-run Same Extent** | 1. Load "Places" for an area.<br>2. Load "Places" again for the *exact same* area. | Should prompt to overwrite or handle file locks gracefully.<br>Should not duplicate layers endlessly or crash due to "file in use". | |

---

## 🛠️ 4. Data Integrity & MFC

| ID | Test Case | Steps | Expected Result | Pass/Fail |
| :--- | :--- | :--- | :--- | :--- |
| **4.1** | **Complex Types** | 1. Identify a theme with nested structs (e.g., `names` in Overture).<br>2. Open Attribute Table. | Nested fields (structs/lists) should be flattened or serialized as JSON strings, not cause errors. | |
| **4.2** | **MFC Creation** | 1. Load multiple datasets (Buildings, Places).<br>2. Check Catalog pane. | A Multi-File Feature Connection (MFC) should be created/updated (if feature is enabled). | |
| **4.3** | **Pro Project Save** | 1. Load layers.<br>2. Save ArcGIS Pro Project (`.aprx`).<br>3. Close and Re-open Project. | Layers persist. Data sources are valid (links to Parquet files are preserved). | |

---

## 🧹 5. Cleanup

| ID | Test Case | Steps | Expected Result | Pass/Fail |
| :--- | :--- | :--- | :--- | :--- |
| **5.1** | **File Cleanup** | 1. Close ArcGIS Pro.<br>2. Check temp folder location. | Temporary files (if any) should be cleaned up, or explicitly stored in the user-defined output folder. | |
