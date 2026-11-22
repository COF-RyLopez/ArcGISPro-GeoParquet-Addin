# Load an Overture theme

The core workflow: pick a release and theme(s), filter by the map extent, load into layers. Under the hood: `S3Ingester.BuildLoadQuery` ingests into `current_table`, `GeoParquetSql.BuildCopyCommand` writes the `_source` GeoParquet archive, `S3Ingester.BuildDisplayTableCommand` derives the flattened display table.

## Sub-features

- Extent filter (`ExtentBounds` → bbox pushdown + `ST_Intersects`): rows outside the extent never load.
- Source preservation (`preserveSource: true`): nested columns (`connectors`, `sources`) survive into the archive.
- Display flattening: nested columns are dropped from the display table; geometry survives as WKT-identical.
- Quoted/exotic file paths (apostrophes) survive `EscapeSqlLiteral`.

## How to get to it (user POV)

In ArcGIS Pro: GeoParquet wizard dockpane → pick release → pick theme (e.g. Transportation/segment) → Select Area (current extent) → Load. Layers appear on the map; the snapshot folder holds the parquet.

## Driving it with the dotnet scratch-test harness

`DuckDBGeoparquet.Tests/DuckDBExecutionTests.cs` already drives this end to end — `ExportRoundTrip_PreservesRoadSourceAndQuotedPath` is the canonical drive (fixture → BuildLoadQuery → archive COPY → BuildDisplayTableCommand → read-back assertions). For a change in this area, extend a scratch copy of that test with the new behavior rather than inventing a new shape; run with `--filter "FullyQualifiedName~DuckDBExecutionTests"` plus your scratch filter and tee to evidence.

## Gotchas

- Fixtures are local parquet files; the S3/`read_parquet` listing of a real Overture release is network and is NOT exercised — `OvertureReleaseDiscoveryTests` covers discovery parsing separately.
- Windows-only: snapshot folder layout on a real Pro project, adding layers to the map, MFC creation.
