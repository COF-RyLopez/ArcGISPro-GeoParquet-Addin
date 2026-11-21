# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Added
- DuckDB `httpfs` now applies `http_proxy`, `http_proxy_username`, and `http_proxy_password` so S3 and Overture reads work behind corporate proxies. The Windows system proxy is auto-detected for the Overture S3 URL (`WebRequest.GetSystemWebProxy` / WinHTTP); an optional manual proxy can be set under Add-in Settings. Proxy passwords are never written to logs.
- `memory_limit` and `max_memory` are configurable in Add-in Settings (defaults remain `2GB` / `4GB`).

### Changed
- When bundled `spatial` and `httpfs` extensions are present, DuckDB never attempts a network `INSTALL`.
- Blocked or offline S3/Overture reads show "Can't reach Overture S3. Check the proxy or network." instead of a raw DuckDB error.
- Bumped DuckDB.NET.Data.Full from 1.5.3 to 1.5.6.1 (DuckDB engine v1.5.6; stays on the 1.5 line, not 2.0). CI and `Extensions/README.txt` now fetch bundled `spatial` and `httpfs` from `https://extensions.duckdb.org/v1.5.6/...`. Patch-line notes between 1.5.3 and 1.5.6 report no breaking changes to the add-in's SQL (`ST_*` spatial functions, `read_parquet`, `SET s3_region` / httpfs settings). The test project runs the same engine.
- Bump the GERSify fallback Overture release from `2026-06-17.0` to `2026-10-21.0`, the newest folder in STAC (`https://stac.overturemaps.org/catalog.json`) and S3 (`s3://overturemaps-us-west-2/release/`). `2026-11-18.0` is still only on the proposed calendar and is not published yet.
- **Overture Maps Schema v2 Support**: Updated to support Overture Maps schema v2 (releases 2026-09-23 and later)
  - Place `categories` field renamed to `taxonomy` with structured `primary`, `hierarchy`, and `alternates` fields
  - Added support for new `basic_category` field in Places
  - Geocoder, MFC generation, and locator services now detect and use either `taxonomy` (v2) or `categories` (v1) at runtime
  - Maintains backward compatibility with pre-v2 releases through dynamic column detection

### Fixed
- Resolve the newest real Overture release folder at runtime via the official STAC catalog (`https://stac.overturemaps.org/catalog.json`), with anonymous S3 ListObjectsV2 (`prefix=release/`) as fallback. The retired `releases.json` endpoint and the literal `latest` S3 folder are no longer used.
- Production AGOL publishing now defaults to the County of Fresno Marketplace item (`https://cofgisonline.maps.arcgis.com/home/item.html?id=8293d1220b7848848ce316b4fa3263b5`) instead of `www.arcgis.com` when portal/item secrets are unset.
- Cached `latestRelease.txt` values are validated against the discovered list; a removed cache entry is replaced with the newest available release and logged.
- Downloads and preview samples now fail with a clear discovery error instead of building `s3://.../release/latest/...` when no release can be resolved. The "release has been removed" message names the newest available folder.

## [0.3.0] - 2026-03-01

### Added
- Style pack model and catalog grouping (`Official`, `Experimental`) for map style organization.
- New `New Amsterdam` style option backed by bundled `NewAmsterdam.stylx`.
- Style category filtering in the dockpane UI.
- `Repair Symbology` command to re-apply style renderers/labels to existing layers.
- Deterministic, style-aware draw-order ranking with explicit geometry/type fallbacks.
- End-of-load diagnostics summary (layers, style, elapsed time, extent).

### Changed
- Layer ordering now consistently keeps building layers above polygon base/context layers.
- Water polygon layers are consistently ranked at the bottom of polygon layers.
- Map layer ordering is re-enforced after bulk layer creation for stable results.
- Cleanup flow now favors safe best-effort parquet cleanup and cache reset behavior.
- Reduced non-actionable per-layer debug logging noise while keeping error/warning diagnostics.

### Fixed
- Intermittent draw-order drift where bulk-created layers could appear in unintended order.
- Cases where stale output/cache state caused older symbology/metadata behavior to persist.
- Point-layer visibility defaults and feature binning handling for dense map contexts.

## [0.2.3] - Previous Release
- See GitHub Releases for historical notes.

