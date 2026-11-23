# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Planned
- **Selected-feature download area** (next): a "Selected Feature(s)" option in Select Area — select a boundary polygon on the map (city, county, state, parcel), and the download uses its exact geometry through the existing polygon pipeline (`ST_Intersects` + optional clip), with large boundaries generalized before querying. Pairs with the Overture Administrative Divisions and OSM Boundaries datasets already in the tree. A named-boundary search (type "Fresno County") is deliberately deferred.
- **Settings page**: move the add-in settings (proxy, memory limits) from the wizard's Save options expander to an ArcGIS Pro Options property page (Settings → Options → GeoParquet), committing on OK instead of per keystroke.
- **Self-bundling DuckDB extensions**: an MSBuild target that fetches the matching `spatial`/`httpfs` extensions when missing, so local builds bundle them like CI does, with the engine version declared once.
- **SQL console**: a "Query Data (SQL)" dockpane for power users — DuckDB SQL against the live snapshot GeoParquet files through the add-in's existing native engine (DuckDB.NET), results grid with CSV/Parquet export, read-only against snapshots.

### Added
- **Per-provider ribbon sections and provider-aware output names.** The GeoParquet tab now has one ribbon group per provider: **GeoParquet** (Download Data, with a new neutral download glyph — the Overture logo now appears only on Overture entries), **Overture Maps** (its download entry plus Overture Geocoder, GERSify, Create MFC), **OpenStreetMap** and **Source Cooperative** (one button each, opening the GeoParquet Data Loader focused on that provider's selectable datasets — mirroring the Overture Maps button, so each provider is picked on the panel rather than the ribbon), and **Custom Sources** (Custom Sources + Add Data Source). A provider button now also **scopes the panel's theme tree to that provider** — the other providers' groups are hidden, a "Showing … only" banner with a **Show all providers** link restores the full tree, and the main Download Data button always opens unfiltered. Select All is hidden while a filter is active so it can't select unseen themes. The **Choose Map Purpose** section (cartographic profiles tuned to the Overture schema) now shows only when Overture data is in play, following the same rule as the release header. Snapshot folders and the Contents-pane group layer are now named by provider instead of always "Overture": folders read `OSM_20261122043436_ab12cd34` / `Overture_2026-11-18.0_…` (release kept only when Overture data is included; a dated release is no longer required for downloads without Overture data), and group layers read "OSM — 2026-11-22 04:34" / "Overture 2026-11-18.0 — …" ("Mixed" when one download spans providers). Existing pre-rename snapshot folders still display as Overture. Dockpane first-show grouping is unchanged (the wizard anchors, the Overture tools group with it); machines that already moved panes keep their saved layout — Pro's Reset Panes restores the defaults.
- **Ribbon rebrand and per-provider entry points.** The ribbon tab and main group are now captioned **GeoParquet** (the add-in covers Overture Maps, OpenStreetMap, Source Cooperative, and custom sources, not just Overture); the wizard dockpane is the **GeoParquet Data Loader** and its show button is **Download Data**. All DAML ids are unchanged, so existing user configs keep working, and the Overture-specific tools (Overture Geocoder, GERSify, Create MFC) keep their names. The Data Sources group gains one ribbon button per provider — Overture Maps, OpenStreetMap, Source Cooperative, Custom Sources — each opening the wizard focused on that provider, expanding its group in the theme tree without changing any selection. The Overture release header at the top of the wizard now shows only when it applies: it hides while a non-Overture provider has focus or when only preset/custom sources are selected, and returns when an Overture theme is selected or the Overture entry is used. Each provider entry has its own icon in light and dark variants, following ArcGIS Pro's icon style: the Custom Sources entry uses Esri's stock FolderWithGISData artwork (from the Apache-2.0 arcgis-pro-sdk-community-samples), and the others are original glyphs drawn in the same muted Pro palette. The OpenStreetMap logo is intentionally not used — the OSMF trademark policy does not expressly permit embedding it as an icon in distributed software.
- **Built-in preset data sources** in the download wizard's theme tree, matching the QGIS GeoParquet downloader's catalog: an **OpenStreetMap (LayerCake)** group (9 US community extracts from data.openstreetmap.us: addresses, boundaries, buildings, highways, parks, POIs, settlements, waterways, wetlands) and a **Source Cooperative** group (7 curated datasets: VIDA Google/Microsoft/OSM Buildings, Microsoft ML Road Detections, GlobalBuildingAtlas, OpenBuildingMap, Foursquare Open Source Places, US Structures from ORNL, Planet EU Field Boundaries). The upstream catalog's USDA Crop Sequence Boundaries is excluded: that file ships Albers-projected geometry, which the add-in's WGS84 spatial filter cannot match. Presets download through the same pipeline as user-defined custom sources — spatially filtered to the chosen area, with bbox row-group pushdown when the dataset ships a bbox column — and need no probe: geometry metadata is shipped and verified against the live URLs. Reads of the dotted `us-west-2.opendata.source.coop` bucket use a DuckDB secret scoped to that bucket (path-style URLs on the regional endpoint; empty keys so no user AWS credentials are sent), leaving Overture and other S3 reads untouched. Presets are excluded from **Select All** (unchecking it still clears them) and cannot be removed like custom sources.
- **Richer area-of-interest tools** in the download wizard's Select Area section, matching the QGIS GeoParquet downloader: alongside the existing current-map-extent and drawn-rectangle options, the download area can now be a **typed WGS84 bounding box** (four coordinate fields with live validation; a lone decimal comma is accepted, thousands separators are not), a **chosen map layer's extent** (any layer in the active map, projected to WGS84), or a **polygon drawn on the map** with a new polygon sketch tool. Downloads with a drawn polygon filter with `ST_Intersects` against the exact polygon (not just its bounding box), while the fast bbox row-group pushdown keeps using the polygon's envelope. An optional **"Clip features to the polygon"** checkbox clips downloaded geometries to the drawn polygon with `ST_Intersection`; like the attribute filter, clipping applies to the ingest, so the snapshot's `_source` files and display layers both reflect it (point-only types are never clipped — an intersecting point clips to itself). Preview samples use the polygon's envelope.
- Optional **attribute filter** in the download wizard's Select Area section: a SQL WHERE condition (e.g. `subtype = 'road' AND class IN ('motorway', 'primary')`) applied to both downloads and preview samples, matching the QGIS GeoParquet downloader's filter. The filter is validated before any query runs — statement chaining, comments, subqueries, and DuckDB functions that read files, the network, or the environment are rejected with a clear message.
- Optional GeoParquet 2.0 export (`GEOPARQUET_VERSION 'V2'`) in Output Settings. GeoParquet 1.x remains the default; in testing ArcGIS Pro read 2.0 display layers, but 1.x stays the safe default. The 2.0 choice is gated on DuckDB 1.4.1+ rather than silently writing 1.x.
- Optional **Save to file geodatabase** output setting on the download wizard (GeoParquet remains the default). After local GeoParquet files are written, the add-in runs ArcGIS Pro's Export Features tool (`conversion.ExportFeatures`) into a user-chosen `.gdb` or `Overture.gdb` under the data output folder, names feature classes from theme and type, overwrites or suffixes existing names to match current replace/skip behavior, and adds those feature classes to the map instead of Parquet layers. Geoprocessing failures are reported in the log and the GeoParquet files are kept. Attribute domains are applied for file geodatabase layers (they remain skipped only for Parquet).
- DuckDB `httpfs` now applies `http_proxy`, `http_proxy_username`, and `http_proxy_password` so S3 and Overture reads work behind corporate proxies. The Windows system proxy is auto-detected for the Overture S3 URL (`WebRequest.GetSystemWebProxy` / WinHTTP); an optional manual proxy can be set under Add-in Settings. Proxy passwords are never written to logs.
- `memory_limit` and `max_memory` are configurable in Add-in Settings (defaults remain `2GB` / `4GB`).

### Changed
- When bundled `spatial` and `httpfs` extensions are present, DuckDB never attempts a network `INSTALL`.
- Blocked or offline S3/Overture reads show "Can't reach Overture S3. Check the proxy or network." instead of a raw DuckDB error.
- Bumped DuckDB.NET.Data.Full from 1.5.3 to 1.5.6.1 (DuckDB engine v1.5.6; stays on the 1.5 line, not 2.0). CI and `Extensions/README.txt` now fetch bundled `spatial` and `httpfs` from `https://extensions.duckdb.org/v1.5.6/...`. Patch-line notes between 1.5.3 and 1.5.6 report no breaking changes to the add-in's SQL (`ST_*` spatial functions, `read_parquet`, `SET s3_region` / httpfs settings). The test project runs the same engine.
- Bump the GERSify fallback Overture release from `2026-10-21.0` to `2026-11-18.0`, the newest folder in STAC (`https://stac.overturemaps.org/catalog.json`) and S3 (`s3://overturemaps-us-west-2/release/`). Overture keeps only two releases public, so the October folder drops when the December release ships.
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

