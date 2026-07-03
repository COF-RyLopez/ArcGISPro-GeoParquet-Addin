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
- `Views/WizardDockpaneViewModel.Preview.cs`: Show Extent / Preview Sample /
  Clear commands.

**Post-verification design change:** field testing showed the beta
`ParquetLayer` fails to parse the add-in's DuckDB exports even with SNAPPY
compression (likely Overture's nested columns), and previewing files already
loaded onto the Pro map is redundant anyway. The preview was refocused as a
**pre-load dry run**: "Preview Sample" uses DuckDB (extent-filtered, capped
at 2,000 features per type) to pull a GeoJSON sample straight from Overture
S3 in seconds and renders it via `GeoJSONLayer` — answering "is there data
here and is it what I expect?" before committing to a full download. The
"Use Preview Extent" adoption feature was dropped: the Pro map view already
drives the extent.

**Verification checklist (requires ArcGIS Pro 3.7 on Windows):**
1. `dotnet build` — confirm removing `Esri.ArcGISRuntime` broke nothing.
2. Open Preview tab → map loads, "Preview map ready." appears in the log.
3. Show Extent draws the configured extent from the active Pro map or custom
   extent.
4. Select one or more themes, click Preview Sample, and confirm capped
   GeoJSON sample layers render from S3.
5. Disconnect from the network, reload the pane → offline notice, no crash.
6. If Pro's bundled WebView2 assemblies conflict with the NuGet version,
   pin `Microsoft.Web.WebView2` to the version Pro ships.

## Phase 2c — Decomposition + tests (IN PROGRESS)

**Stage 1 (done):** unit-test foundation + first extraction.
- `DuckDBGeoparquet.Tests` — xunit on net10.0. It links dependency-free
  sources (`Compile Include`) instead of referencing the add-in project, so
  tests build and run without the ArcGIS Pro SDK; CI runs `dotnet test`
  after the msbuild step. Anything moved into a pure class becomes testable
  by adding one line to the test csproj.
- `Services/GeoParquetSql.cs` — pure SQL-fragment builders extracted from
  `DataProcessor`: compression validation, GeoParquet COPY command, extent
  polygon WKT, bbox pushdown predicate. Covered by tests including a
  regression pin for the German-locale decimal-separator bug.
- `Services/PreviewBridge.cs` message handling is covered by tests
  (camelCase contract, wkid default, malformed-message tolerance).

**Stage 2 (in progress):** split `DataProcessor.cs` behind a thin façade.
- `DuckDBManager` (connection lifecycle + extension loading) — DONE.
  `DataProcessor` proxies `_connection`/`_isInitialized` so call sites are
  unchanged; behavior and error messages are moved verbatim.
- `GeocoderEngine` (address/place candidate search) — DONE. Query logic
  moved verbatim; `DataProcessor` keeps thin delegating wrappers for its
  public search API. `EscapeSqlLiteral` promoted to `GeoParquetSql` with
  test coverage.
- `S3Ingester` — pure ingest query builders extracted and DONE.
  `Services/S3Ingester.cs` owns `BuildColumnProjection` (type-specific
  column drops) and `BuildLoadQuery` (the `current_table` query: bbox
  pushdown + `ST_Intersects` filter and `ST_Intersection` clipping, with
  bbox struct repack when present). `DataProcessor.IngestFileAsync` now
  calls it; the SQL is moved verbatim. Covered by `S3IngesterTests`. The
  stateful S3 read (schema discovery, execution) stays in `DataProcessor`.
- Remaining: `ParquetExporter`, `LayerManager`. These share
  mutable per-load state (`_pendingLayers`, `_currentExtent`, session
  suffix), so extract them together or thread the state through a small
  load-context object.
- Stage 3: extract `DataLoadOrchestrator` and `MfcOrchestrator` from
  `WizardDockpaneViewModel.cs` (load pipeline, bulk replacement, P/Invoke
  deletion, extent resolution; MFC creation).

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
