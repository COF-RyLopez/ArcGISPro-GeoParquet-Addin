# Custom data source

Add any GeoParquet URL (ribbon "Add Data Source" dialog or wizard) — the add-in probes it with DESCRIBE, finds the geometry column, saves it to `user_sources.json`, and loads it like a theme.

## Sub-features

- Probe: `UserSourceStore.BuildProbeQuery(url)` (a DESCRIBE), `PickGeometryColumn(columns)` finds a GEOMETRY-typed or WKB-blob column under any name.
- Validation: `UserSourceStore.ValidateNewSource(name, url, existing)` — shared by dialog and wizard.
- Load with a renamed geometry column: `S3Ingester.BuildLoadQuery(..., geometryColumn:, geometryIsWkb:)` surfaces it as `geometry` and still extent-filters.
- Persistence: `UserSourceStore.Load`/`Save` round-trip `user_sources.json`.

## How to get to it (user POV)

In ArcGIS Pro: ribbon → Data Sources group → Add Data Source → paste URL → Save (validation errors show in the dialog). The source then appears in the wizard's source list.

## Driving it with the dotnet scratch-test harness

`DuckDBExecutionTests.CustomSource_RenamedGeometryColumn_ProbesAndLoads` and `CustomSource_WkbGeometryColumn_ProbesAndLoads` are the canonical drives: fixture parquet with a `geom` column → probe → pick → load → assert `ST_AsText(geometry)` and the outside-extent load returns zero rows. `UserSourceStoreTests` covers validation and the JSON store. Extend a scratch copy for new behavior; tee to evidence.

## Gotchas

- Remote URLs (https/S3) share the same builders but the probe over the network is not exercised here; a local fixture path stands in. Proxy behavior for those is `HttpProxySettingsTests`.
- Windows-only: the ProWindow dialog itself (rendering, validation text display, refresh of an open wizard pane).
