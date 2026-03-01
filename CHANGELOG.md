# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

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

