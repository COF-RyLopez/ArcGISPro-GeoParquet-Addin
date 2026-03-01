# ArcGIS Pro GeoParquet Add-in v0.3.0

## Highlights
- Added style pack architecture with grouped style selection (`Official` and `Experimental`).
- Added `New Amsterdam` style using bundled `NewAmsterdam.stylx`.
- Improved layer draw order with deterministic, style-aware ranking.
- Added `Repair Symbology` to re-apply style renderers/labels to existing Overture layers.

## What Changed
- Buildings now consistently draw above polygon base/context layers.
- Water polygons now consistently draw at the bottom of polygon layers.
- Draw order is now enforced after bulk layer creation to prevent ordering drift.
- Added better cache/output cleanup behavior to reduce stale state issues.
- Added load diagnostics summary (layer count, style, elapsed time, extent).
- Reduced noisy per-layer debug logging while retaining actionable warnings/errors.

## UI Improvements
- Added style group dropdown to improve style discoverability.
- Normalized style action button sizing and aligned style UI behavior.

## Operational Notes
- ArcGIS host-side WPF binding warnings in debug output may still appear; these are environment/UI framework noise and not regressions from add-in data load or styling behavior.

## Upgrade Notes
- No migration steps required.
- CI/CD version update remains tag-driven (`v*` tags).

