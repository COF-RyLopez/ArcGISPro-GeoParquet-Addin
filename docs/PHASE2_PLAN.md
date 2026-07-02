# Phase 2 Plan (Revised): ArcGIS Pro GeoParquet Add-in

Revision of the original Antigravity implementation plan, updated after a
code audit and verification of the ArcGIS Maps SDK for JavaScript facts the
original plan assumed. Phases are resequenced so cheap, enabling work lands
first and the risky pieces are de-risked before they're built on.

## Verified facts (things the original plan guessed at)

| Assumption | Verified reality |
|---|---|
| JS SDK 5.1 loads via bare `@arcgis/core` ESM imports | Bare specifiers don't resolve in a browser. The Esri-recommended CDN pattern is a single `https://js.arcgis.com/5.1/` script tag + `$arcgis.import()`. AMD `require()` still works but is deprecated at 5.0 and removed at 6.0. |
| `ParquetLayer` config is `data: { urls: [...] }` | It's `urls: [...]` (array, one or more files sharing a schema). Still **beta**. 5.1 added spatially-optimized-parquet streaming and query improvements. |
| Any GeoParquet file will render | **ParquetLayer only reads GZIP, Snappy, or uncompressed files.** This add-in's export default is **ZSTD**, which will not render. The preview logs a warning and suggests re-exporting with SNAPPY/GZIP. A DuckDB-generated GeoJSON sample + `GeoJSONLayer` is the guaranteed fallback path (bridge + page already support `addGeoJsonLayer`). |
| Preview extent is WGS84 | The JS view extent is in the basemap's spatial reference (Web Mercator, wkid 102100). The wkid is sent with every `extentChanged` message and the C# side projects to WGS84 before writing `CustomExtent` (downstream code assumes WGS84). |

## Phase 2a — Cleanup (DONE in this branch)

- Removed dead code from `WizardDockpaneViewModel.cs`: `GetLatestRelease()`,
  `OnThemeSelectionChanged`, `IsThemeSelected`, `ToggleThemeSelection`,
  `CheckInitialThemeSelection`.
- Single shared `static HttpClient` for release checks.
- Deleted `scratch/` and `split_properties.py`; gitignored so they stay local.
- Removed the unused `Esri.ArcGISRuntime` package reference (no code used it;
  verify on first local build).
- `Config.daml` minimum version bumped to Pro 3.7 (matches the 3.7 SDK the
  project now builds against).
- Added `WizardTab` enum; all `SelectedTabIndex` writes use it instead of
  magic numbers (these had already drifted once).

## Phase 2b — Preview tab (DONE in this branch; needs on-machine verification)

- `Views/PreviewMap/preview.html`: `$arcgis.import()` loading, `reactiveUtils`
  extent watching, `ParquetLayer` with `urls`, `GeoJSONLayer` fallback,
  offline notice + `previewUnavailable` handshake when the CDN is unreachable.
- `Services/PreviewBridge.cs`: delegate-based C# ↔ JS messaging (no WebView2
  dependency in the service), `MapReady`/`LayerLoaded`/`LayerError`/
  `ExtentChanged`/`PreviewUnavailable` events, hardened message parsing.
- `Views/WizardDockpane.xaml(.cs)`: Preview tab between Select Data and
  Status; `Microsoft.Web.WebView2` control initialized with an explicit
  writable user-data folder (the add-in runs from the read-only assembly
  cache); `appassets` virtual host serves the bundled page over an https
  origin; `overturedata` virtual host is (re)mapped to the current data
  folder on each preview.
- `Views/WizardDockpaneViewModel.Preview.cs`: Show Extent / Preview Data /
  Clear / Use Preview Extent commands.

**Verification checklist (requires ArcGIS Pro 3.7 on Windows):**
1. `dotnet build` — confirm removing `Esri.ArcGISRuntime` broke nothing.
2. Open Preview tab → map loads, "Preview map ready." appears in the log.
3. Show Extent draws the configured extent; Use Preview Extent round-trips
   (pan/zoom, adopt, confirm the WGS84 coords in Select Data).
4. Export a small area with **SNAPPY**, Preview Data → layers render.
   Repeat with ZSTD → per-layer error + the compression warning in the log.
5. Disconnect from the network, reload the pane → offline notice, no crash.
6. If Pro's bundled WebView2 assemblies conflict with the NuGet version,
   pin `Microsoft.Web.WebView2` to the version Pro ships.

## Phase 2c — Decomposition + tests (NEXT)

Extract from `WizardDockpaneViewModel.cs` (~1,850 lines) into services:
`DataLoadOrchestrator` (load pipeline, bulk replacement, P/Invoke deletion,
extent resolution) and `MfcOrchestrator` (`CreateMfcAsync` + helpers).
Split `DataProcessor.cs` (2,323 lines) into `DuckDBManager`, `S3Ingester`,
`ParquetExporter`, `LayerManager`, `GeocoderEngine`, with `DataProcessor` as
a thin façade.

**Land this with a unit-test project** — the point of the extraction is that
the services become testable without Pro running. Until then "automated
tests" is just `dotnet build`.

## Phase 2d — UI restructure (LAST)

Five focused tabs (Themes / Area & Output / Preview / Status / Tools) plus a
sticky summary bar. Purely cosmetic churn, so it goes last; the `WizardTab`
enum makes the re-index mechanical.

## Deliberately deferred / decided

- **Parallel S3 downloads**: skeptical as originally specified. DuckDB's
  httpfs already parallelizes range requests internally, and concurrent
  ingestion against a single DuckDB connection isn't safe. Measure first;
  if pursued, use one connection per task and rework progress reporting.
- **CDN vs bundled JS SDK**: the preview degrades gracefully offline. For
  locked-down networks, bundling the SDK (or an org-hosted copy) is the
  fix; that's a packaging decision for later.
- **Auto-MFC checkbox & release-version dropdown**: unchanged stretch goals.
