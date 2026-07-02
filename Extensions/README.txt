DuckDB Extensions for GeoParquet Add-in
============================================

The add-in needs two DuckDB extensions:
1. spatial.duckdb_extension  - spatial functions and GeoParquet support
2. httpfs.duckdb_extension   - HTTP / S3 cloud storage access

Normally DuckDB downloads these at runtime into %USERPROFILE%\.duckdb.
On locked-down machines an Application Control (WDAC/AppLocker) policy can
block DLLs loaded from the user profile ("An Application Control policy has
blocked this file"). Bundling the extensions in this folder fixes that: they
then load from the add-in's own install folder and no runtime download is
needed.

The extension version MUST match the DuckDB.NET.Data.Full package version in
DuckDBGeoparquet.csproj (currently 1.5.3).

CI does this automatically
---------------------------
The GitHub Actions build downloads both extensions into this folder before
compiling, so release artifacts already contain them. You only need the steps
below for local builds.

OPTION 1: Copy from your local DuckDB cache (fastest)
-----------------------------------------------------
If the add-in ever ran on your machine, DuckDB already downloaded the files:
  %USERPROFILE%\.duckdb\extensions\v1.5.3\windows_amd64\spatial.duckdb_extension
  %USERPROFILE%\.duckdb\extensions\v1.5.3\windows_amd64\httpfs.duckdb_extension
Copy both files directly into this Extensions folder (no subfolders).

OPTION 2: Direct download
-------------------------
  https://extensions.duckdb.org/v1.5.3/windows_amd64/spatial.duckdb_extension.gz
  https://extensions.duckdb.org/v1.5.3/windows_amd64/httpfs.duckdb_extension.gz
Un-gzip each file and place the resulting .duckdb_extension files directly in
this folder.

CRITICAL
--------
1. Version must match the DuckDB.NET package (1.5.3 today; update on upgrade)
2. Names must be exactly spatial.duckdb_extension and httpfs.duckdb_extension
3. Files go DIRECTLY in this Extensions folder (no version/platform subfolders)
4. The csproj packages Extensions\*.duckdb_extension into the .esriAddInX
   automatically; the files are gitignored, so they never enter source control
