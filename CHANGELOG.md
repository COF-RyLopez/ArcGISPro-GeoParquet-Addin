# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Fixed
- Resolve the newest real Overture release folder at runtime via the official STAC catalog (`https://stac.overturemaps.org/catalog.json`), with anonymous S3 ListObjectsV2 (`prefix=release/`) as fallback. The retired `releases.json` endpoint and the literal `latest` S3 folder are no longer used.
- Cached `latestRelease.txt` values are validated against the discovered list; a removed cache entry is replaced with the newest available release and logged.
- Downloads and preview samples now fail with a clear discovery error instead of building `s3://.../release/latest/...` when no release can be resolved. The "release has been removed" message names the newest available folder.

### Changed
- **Overture Maps Schema v2 Support**: Updated to support Overture Maps schema v2 (releases 2026-09-23 and later)
  - Place `categories` field renamed to `taxonomy` with structured `primary`, `hierarchy`, and `alternates` fields
  - Added support for new `basic_category` field in Places
  - Geocoder, MFC generation, and locator services now detect and use either `taxonomy` (v2) or `categories` (v1) at runtime
  - Maintains backward compatibility with pre-v2 releases through dynamic column detection

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

