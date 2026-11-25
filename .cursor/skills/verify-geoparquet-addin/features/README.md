# GeoParquet add-in verification map

Maintained source for verifying user-facing behavior of the ArcGIS Pro
Overture / GeoParquet add-in on Windows. Read this index before driving, then
use the matching feature file.

## Baseline preconditions

- Windows host with ArcGIS Pro **3.7+** installed at
  `C:\Program Files\ArcGIS\Pro\bin\ArcGISPro.exe`.
- Repo builds with MSBuild (see `AGENTS.md`); bundled
  `Extensions\*.duckdb_extension` present for the engine in
  `build/DuckDbVersion.props`.
- Run `pwsh -File .cursor/skills/verify-geoparquet-addin/helpers/doctor.ps1`
  and require `DOCTOR_OK`.
- Prefer project **MyProject2** (or set `VERIFY_APRX`). A map view must be open
  for download features.
- Pro is a **shared** instance — do not start a second Pro for isolation.
- Never call `TogglePattern.Toggle` or `ExpandCollapsePattern.Expand` on Pro.

## Driving conventions

- Start from doctor-healthy Pro with the **Overture Maps** ribbon tab visible.
- Prefer UIA **Name** captions from `Config.daml` over coordinates.
- Type URLs and paths with `ValuePattern.SetValue`; never Browse.
- Evidence goes under `$env:VERIFY_EVIDENCE_DIR` (see skill body). Cleanup keeps it.

## Proof and skip reporting

- Capture the user action and resulting state (status text + file/layer side effect).
- Record feature ID and entry point with every artifact.
- Unreachable paths need the attempted route and unmet precondition
  (`verified-unreachable`).
- Do not mark an entry point verified via a different path.

## Feature entry contract

Each feature file: H1 + one paragraph, then exactly four H2s —
`Sub-features`, `How to get to it (user POV)`,
`Driving it with UIA helpers`, `Gotchas`.

## Features

- [Download Overture](./download-overture.md) — theme select, extent, Add to map snapshot.
- [Add Data Source](./add-data-source.md) — ribbon dialog to save a GeoParquet URL.
- [Custom Sources in loader](./custom-sources.md) — Custom Sources node after add/remove.
- [Overture Geocoder](./overture-geocoder.md) — search project address/place parquet.
- [GERSify Your Data](./gersify.md) — link authoritative data to GERS IDs.
- [Create MFC](./create-mfc.md) — Multifile Feature Connection from downloaded GeoParquet.
