DuckDB Extensions for GeoParquet Add-in
============================================

This folder MUST contain the following DuckDB extension files:
1. spatial.duckdb_extension - For spatial functions and GeoParquet support
2. httpfs.duckdb_extension - For HTTP/Cloud storage access

How to obtain these files (CRITICAL):
-------------------------------------
The extensions MUST match version 1.2.0 of DuckDB to work with this add-in.

OPTION 1: Download pre-built extensions (RECOMMENDED)
----------------------------------------------------
1. Go to: https://github.com/duckdb/duckdb/releases/tag/v1.2.0
2. Download the Windows x64 version: duckdb_cli-windows-amd64.zip
3. Extract the zip file
4. Run these commands in a terminal:
   ```
   ./duckdb.exe -c "INSTALL spatial; INSTALL httpfs;"
   ```
5. The extensions will be created in a .duckdb/extensions folder
6. Copy these files to this Extensions folder:
   - .duckdb/extensions/v1.2.0/windows_amd64/spatial.duckdb_extension
   - .duckdb/extensions/v1.2.0/windows_amd64/httpfs.duckdb_extension

OPTION 2: Direct download links (ALTERNATIVE)
---------------------------------------------
Direct links to extension files for v1.2.0:
- spatial: https://github.com/duckdb/duckdb-spatial
- httpfs: https://github.com/duckdb/duckdb-httpfs

Download these files and rename them to:
- spatial.duckdb_extension
- httpfs.duckdb_extension

CRITICAL:
---------
1. The extensions MUST match version 1.2.0 of DuckDB
2. The files MUST be named exactly "spatial.duckdb_extension" and "httpfs.duckdb_extension"
3. The files MUST be directly in this Extensions folder
4. These extensions will be automatically copied to the output directory during build
   and loaded from that location at runtime, eliminating the need for internet access
   or admin privileges when running the add-in 