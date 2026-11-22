# GeoParquet export version

The wizard's export version choice: 1.x (default, max compatibility) or 2.0 (native Parquet GEOMETRY types). Applied to both the `_source` archive and display layers.

## Sub-features

- Version normalization: `GeoParquetSql.NormalizeGeoParquetVersion` maps "2"/"2.0"/"2.0.0"/"V2" → `V2`, everything else → `V1`.
- COPY option: `GeoParquetSql.BuildCopyCommand(select, path, compression, geoParquetVersion)` emits `GEOPARQUET_VERSION 'V1'|'V2'`.
- Capability gate: the engine-support check throws when 2.0 is requested on a DuckDB that can't write it (see the "can write GeoParquet 2.0" helper around `Services/GeoParquetSql.cs:95`).

## How to get to it (user POV)

In ArcGIS Pro: GeoParquet wizard → output options → "GeoParquet version" ComboBox (1.x default). Load; the written snapshot parquet carries `geo.version` 1.0.0 or 2.0.0.

## Driving it with the dotnet scratch-test harness

Scratch drive: build a geometry fixture, `BuildCopyCommand` once per version, then assert on the written files — `parquet_kv_metadata('<file>')` shows the `geo` key's version for V1, and for V2 the geometry column's physical type changes (check `parquet_schema`). Assert `NormalizeGeoParquetVersion` mappings directly. Tee to evidence; keep the written files' read-back rows in the log.

## Gotchas

- `parquet_kv_metadata`/`parquet_schema` are fine in a drive even though the attribute-filter validator deny-lists them — the deny list guards user filter strings, not test SQL.
- Windows-only: the ComboBox binding, and loading a 2.0 file into ArcGIS Pro itself (proven once manually, 2026-11-21: Pro reads the 2.0 display layers fine).
