# ArcGIS Pro, DuckDB, and Overture platform research

Research date: September 7, 2026. Sources below are official documentation, release announcements, source code, and live Overture catalogs. Recommendations are engineering judgments, not measured speedups. No Windows/ArcGIS runtime benchmarking was performed. Documentation under `latest` or `current` may change; preserve a tested capability matrix with every add-in release.

## Recommended direction

Keep DuckDB and cloud GeoParquet as the analytical foundation, with a normal read-only ArcGIS feature layer as the default user experience. Give users a separate, explicit action to create an editable geodatabase working copy. Preserve original geometry, nested routing attributes, source provenance, and GERS identifiers in the analytical store; flatten only the ArcGIS display projection. A derived routing network and a reviewed contribution should retain links back to those original entities.

The main performance opportunity is less work: avoid unnecessary file discovery, transfer fewer columns and row groups, stop clipping every feature by default, reuse stable local results, and retain ArcGIS display caching. Updating DuckDB alone will not remove ArcGIS caching or MFC limitations.

## Versions and compatibility

The inspected project targets `net10.0-windows`, `win-x64`, `DuckDB.NET.Data.Full` **1.5.3**, and `Esri.ArcGISPro.Extensions30` **3.7.0.1901**. These are repository facts from `DuckDBGeoparquet.csproj`, not evidence that every ArcGIS version mentioned in the UI can load the add-in. `ArcGISProVersionHelper` still branches around Pro 3.5/3.6. Establish the oldest supported host by testing the actual build on Windows; runtime version branching cannot overcome incompatible framework or SDK requirements.

DuckDB **1.5.5**, released July 22, 2026, is the current stable version verified from the official installation page. The release includes crash fixes, external-file memory-leak fixes, and other corrections. Evaluate the corresponding .NET package and matching Windows extensions together. The release calendar lists 1.5.6 for September 16 and 2.0 for the second half of October; these are tentative future releases. [Installation](https://duckdb.org/install/), [1.5.5 announcement](https://duckdb.org/2026/07/22/announcing-duckdb-155), [release calendar](https://duckdb.org/release_calendar)

DuckDB 1.5 moved `GEOMETRY` into core; most spatial functions still require `spatial`. It also introduced an explicit `geometry_always_xy` setting. Set the intended axis convention and validate known coordinates, transformed extents, and distance calculations before changing versions. In particular, do not combine new XY behavior with old coordinate-flipping workarounds without testing. [DuckDB 1.5 announcement](https://duckdb.org/2026/03/09/announcing-duckdb-150)

### Output compatibility is a test requirement, not a proven current bug

The add-in's `GeoParquetSql.BuildCopyCommand` omits `GEOPARQUET_VERSION`. The **pinned v1.5.3 source** initializes the writer to `GeoParquetVersion::V1`; the option accepts `V1`, `V2`, `BOTH`, and `NONE`. Consequently, native in-memory DuckDB geometry does **not** imply that the current export silently defaults to native Parquet geometry or GeoParquet 2.0. Explicitly choose the compatibility format to protect against future default changes. [DuckDB v1.5.3 writer binding source](https://raw.githubusercontent.com/duckdb/duckdb/v1.5.3/extension/parquet/parquet_extension.cpp)

The GeoParquet project's distribution guide confirms default GeoParquet 1.0.0 output and explains native logical types for `V2`. The latest Esri FAQ lists GeoParquet **1.0.0 and 1.1.0**, plus native Parquet **geometry/geography** types. Do not turn this into a blanket claim that every supported Pro host accepts all GeoParquet 2.0 features. Test `V1`, `V2`, and `BOTH` separately, including CRS metadata and mixed geometry handling. [GeoParquet distribution tools](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/distributing-geoparquet-tools.md), [Esri Parquet FAQ](https://doc.esri.com/en/arcgis-pro/latest/help/data/parquet/faqs-parquet-in-maps.html)

## What ArcGIS can provide today

| Path | Verified behavior | Implication for this add-in |
| --- | --- | --- |
| Single Parquet feature layer | Works as a read-only feature layer; ArcGIS uses a local cache; one geometry entity type per layer. | Default for browse, symbology, identify, selection, and conventional read-only workflows; retain the existing geometry-family splitting. |
| Direct cloud Parquet | Supported cloud connections include S3, GCS, Azure Blob, and ADLS Gen2. | Useful access path, but does not eliminate the ArcGIS local cache or make every pan a DuckDB query. |
| MFC | Groups files with the same schema; supports many analysis tools; has distinct performance and selection limitations. | Keep for collection workflows and compatibility, rather than making novices choose it. |
| Editable copy | Export Features/Export Table can write a geodatabase. | Offer “Create editable working copy” with provenance and GERS links. |
| Network dataset | Created inside a feature dataset containing source feature classes, configured, then built. | Generate a geodatabase routing artifact from DuckDB-prepared edges, junctions, and turns. |

Sources: [add a Parquet layer](https://doc.esri.com/en/arcgis-pro/latest/help/data/parquet/add-parquet-to-map.html), [cloud and format FAQ](https://doc.esri.com/en/arcgis-pro/latest/help/data/parquet/faqs-parquet-in-maps.html), [MFC overview](https://doc.esri.com/en/arcgis-pro/latest/help/data/big-data-connections/big-data-connections.html), [MFC analysis constraints](https://doc.esri.com/en/arcgis-pro/latest/help/data/big-data-connections/use-big-data-connections.html), [Parquet supported uses](https://doc.esri.com/en/arcgis-pro/latest/help/data/parquet/parquet-in-arcgis.html), [Create Network Dataset](https://doc.esri.com/en/arcgis-pro/latest/tool-reference/network-analyst/create-network-dataset.html).

Esri's single-file requirements reject nested columns and geometry collections. Flattened display tables remain necessary even with current native support. Current documentation also permits direct Parquet input to geoprocessing and export to Parquet; verify those newer conveniences against the supported host rather than preserving old limitations globally. [Parquet requirements and supported uses](https://doc.esri.com/en/arcgis-pro/latest/help/data/parquet/parquet-in-arcgis.html)

MFC reading is parallelized for GeoAnalytics Desktop tools, but other tools read/process sequentially. A selection cannot be applied to an MFC input for a GeoAnalytics Desktop tool. Tools that modify input, such as Calculate Field and Near, do not support MFC input. The documentation still describes restart requirements before added records/files draw. These are specific constraints; do not inaccurately say MFC layers never support selection. [Use MFCs](https://doc.esri.com/en/arcgis-pro/latest/help/data/big-data-connections/use-big-data-connections.html)

### Two different ArcGIS caches

The Parquet source cache is created per user/machine: fewer than 500,000 rows uses memory; at least 500,000 uses persistent storage. Updating the source modification date rebuilds it. `Create Parquet Cache`/`CreateParquetCache` can move preparation ahead of map insertion. Show this as a distinct “Preparing ArcGIS layer” phase, include disk usage, and avoid rewriting unchanged outputs. The cache documentation has contradictory descriptions of its “Balanced” retention policy; do not encode that policy from prose without checking the host. [Parquet cache management](https://doc.esri.com/en/arcgis-pro/latest/help/data/parquet/cached-parquet-data.html)

Separately, `LayerManager.ApplyLayerSettings` sets both `DisplayCacheType` and `FeatureCacheType` to `None`. Esri says display caching accelerates revisited extents and documents `None` as disabling layer caching. Recommend retaining host defaults or session caching and benchmarking repeated pans. Feature cache support is layer-specific, so do not promise that enabling it helps every Parquet layer or changes Parquet's source-cache behavior. [Esri SDK display and feature cache concepts](https://pro.arcgis.com/en/pro-app/3.6/sdk/api-reference/conceptdocs/docs/ProConcepts-Map-Authoring.html)

## DuckDB performance: separate discovery, scan, computation, and display

1. **Discovery:** `DataProcessor.IngestFileAsync` makes a `LIMIT 0` schema query over the same remote glob subsequently ingested; `S3ReadParquetOptions` always uses `union_by_name=1`. `LIMIT 0` avoids row materialization but does not establish that remote listing and schema binding are free. Schema unification can require inspecting participating schemas. Benchmark listing/footer requests separately; avoid claiming two full data downloads. Prefer an explicit candidate file list, a cached schema keyed by release/theme/type, and schema union only when drift is detected. DuckDB supports explicit file lists and schema union. [Parquet reader](https://duckdb.org/docs/current/data/parquet/overview)

2. **Remote scan:** Preserve the existing scalar bbox overlap predicates followed by exact `ST_Intersects`. Parquet projection/filter pushdown can avoid column chunks and row groups using statistics. That saves scan data after file discovery; it is not a global spatial index that makes unrelated file footers disappear. Use `EXPLAIN ANALYZE` and HTTP instrumentation to verify actual pruning. Avoid `SELECT *` for task-specific display and summary results. [Parquet partial reading](https://duckdb.org/docs/current/data/parquet/overview)

3. **Computation:** `S3Ingester.BuildLoadQuery` currently clips every retained geometry using `ST_Intersection`, and repeats the intersection predicate in a `CASE`. For ordinary selection, study, contribution, and routing, keep whole intersecting features. Make geometric clipping an explicit export choice. This reduces geometry work and avoids invalidating routing connector positions or changing upstream geometry while retaining the same entity ID. The exact benefit is unmeasured; compare output semantics as well as runtime.

4. **Repeated local work:** Materialize frequently queried AOIs in a persistent DuckDB workspace and build an R-tree when repeated small extent queries justify construction cost. R-tree index scans support specific intersecting predicates with a constant argument; the index belongs to a DuckDB table and does not index arbitrary remote Parquet. Loaded index pages remain resident for the connection, so measure memory and choose workspace lifecycle deliberately. [DuckDB R-tree documentation](https://duckdb.org/docs/stable/core_extensions/spatial/r-tree_indexes.html)

5. **Spatial joins:** DuckDB's spatial join operator builds a temporary index during execution; a persistent R-tree is not a prerequisite for every spatial join. Use supported spatial join forms and confirm `SPATIAL_JOIN` rather than assuming an arbitrary distance/candidate query gets optimized. This matters for address-to-building association and data quality analysis. [Spatial joins in DuckDB](https://duckdb.org/2025/08/08/spatial-joins)

6. **Settings:** `DuckDBManager` sets `memory_limit='2GB'` then `max_memory='4GB'`; these are aliases, so the latter overwrites the former. `enable_object_cache` is a legacy no-op. Replace ambiguous tuning with one explicit memory budget, spill-space budget, and supported metadata/external-file cache settings, then report effective values from `duckdb_settings()`. [DuckDB configuration](https://duckdb.org/docs/current/configuration/overview)

7. **Thread and memory balance:** Threads equal to CPU count is not universally best. Stable DuckDB's remote I/O may benefit from more threads, while CPU-bound workloads can slow down with too many. Sorting, grouping, joins, and windows consume memory; some aggregate states cannot spill. Benchmark bounded presets with ArcGIS running rather than setting huge thread/memory values globally. [Tuning workloads](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads)

8. **Output layout:** The existing 100,000-row groups and ZSTD are sensible starting points. Compare spatially ordered output using `ST_Hilbert` and explicit dataset bounds, several row-group sizes, and compression levels. Hilbert order improves locality; it is not a guarantee that Esri's own cache will build faster. Retain original identifiers independently of row order. [Spatial function reference](https://duckdb.org/docs/stable/core_extensions/spatial/functions), [GeoParquet distribution tools](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/distributing-geoparquet-tools.md)

### Promising future improvement: asynchronous I/O

DuckDB's July 31 article measures **v2.0.0-dev**, compared with 1.5.5. Its S3 TPC-H Q6 benchmark falls from 8.230 s to 2.844 s on a 64-vCPU, 512-GB, same-region EC2 machine with external-file caching disabled. This establishes a promising direction, not a predicted Overture/desktop speedup and not functionality already shipping in the add-in. Keep v2.0 on a compatibility benchmark branch until stable and tested with the native extensions and .NET binding. [Asynchronous I/O announcement and benchmark methodology](https://duckdb.org/2026/07/31/asynchronous-io)

## Overture discovery and preview

The inspected wizard uses `https://labs.overturemaps.org/data/releases.json`. Official current guidance instead points to `https://stac.overturemaps.org/catalog.json`. The live root returned `latest: 2026-08-19.0` on the research date, with a corresponding child catalog and the prior release. Resolve latest once per job, then pin its value in the job manifest. Do not select data by directory modification time when reproducible release identity matters. [Official quickstart](https://docs.overturemaps.org/getting-data/), [live root catalog](https://stac.overturemaps.org/catalog.json)

The catalog describes assets, extents, counts, columns, cloud alternatives, and PMTiles. Use this for discoverable task cards, schema validation, coverage hints, and stable preview links. Overture's PMTiles are specifically for inspection, not guaranteed production cartography. They are a good fast preview path before analytical downloads. Keep whole-feature data downloads independent of tile geometry/generalization. [Overture catalog documentation](https://docs.overturemaps.org/getting-data/cloud-sources/)

A live connector STAC item was inspected directly: it has a file-level bbox, `num_rows`, `num_row_groups`, and AWS/Azure assets; its bbox is large and its reported count is 13,149,761 rows in 256 groups. Therefore STAC enables file pruning but file-level bounds alone do not provide row-group selectivity. Cache a compact per-release file manifest locally, select intersecting assets, then keep Parquet bbox pruning. Fetching every item JSON anew for each interaction would substitute one metadata bottleneck for another. [Inspected connector item](https://stac.overturemaps.org/2026-08-19.0/transportation/connector/00000/00000.json)

## Architecture implications and practical tools

- **Catalog service:** STAC discovery, release pinning, schema capabilities, and cached asset bounds; expose purpose and coverage to the UI.
- **Query workspace:** retained DuckDB connection/database per job or workspace with controlled concurrency; immutable source views, AOI extracts, summary queries, optional indexes, and cancellable work.
- **ArcGIS adapter:** flat GeoParquet display projections, automatic geometry-family separation, native layer creation, explicit cache preparation, then editable geodatabase export when requested.
- **Provenance manifest:** release, source asset identity, AOI, query/profile, geometry treatment, engine/extension versions, creation time, and GERS keys. This supports reproducible studies and reviewed updates.
- **Routing adapter:** preserve original segment geometry and connector references; create derived edges and turns with source mapping. ArcGIS Network Analyst creation uses feature classes in a feature dataset, and its basic creation tool requires subsequent configuration/build. Treat licenses and travel-cost/restriction configuration as explicit workflow capabilities. [Create Network Dataset](https://doc.esri.com/en/arcgis-pro/latest/tool-reference/network-analyst/create-network-dataset.html)
- **Developer tools:** DuckDB CLI for reproducible SQL and profiling; `parquet_metadata`, `parquet_schema`, and `parquet_kv_metadata` for format checks; Overture's Python client as a reference implementation of discovery/download; GeoParquet distribution tooling as an independent validation/optimization comparator. [DuckDB metadata functions](https://duckdb.org/docs/current/data/parquet/overview), [Overture Python client](https://docs.overturemaps.org/getting-data/overturemaps-py/), [GeoParquet tooling](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/distributing-geoparquet-tools.md)

## Benchmark plan before claiming improvements

Use pinned urban, rural, and county AOIs with buildings, addresses, places, segments, and connectors. Include a boundary-crossing road, grade separation, empty AOI, and mixed geometry output. Repeat cold and warm runs under a documented network connection and representative Windows hardware; report median and tail timings. Preserve correctness baselines by GERS ID and geometry hashes where geometry should remain unchanged.

| Experiment | Variants | Required measurements |
| --- | --- | --- |
| Discovery | Global glob/schema union; cached schema; STAC candidate file list | Listing, metadata request counts/bytes, bind latency |
| Spatial extraction | Bbox only; bbox plus exact intersection; explicit clipping | Result IDs, geometry fidelity, fetched bytes, CPU time |
| Fields | Existing projection; task-specific projection; full source retained separately | Transfer, peak memory, output schema completeness |
| Local analysis | Parquet view; DuckDB table; table with R-tree | Index build cost, repeat-query latency, peak memory |
| Output | Current layout; Hilbert ordering; alternate row groups | Write time, bytes, subsequent read pruning |
| ArcGIS display | Current disabled caches; host defaults/session | First layer availability, pan/zoom revisit time, cache disk size |
| Compatibility | Supported Pro hosts, DuckDB 1.5.3/1.5.5, V1/V2/BOTH output | CRS, identify/selection, geoprocessing, reopen, geometry-family counts |
| Usability | Fresh installation, no DuckDB knowledge | Time to first useful map, successful analysis completion, cancellation behavior |

Suggested product targets should be set after this baseline, not represented as achieved measurements. Time to first useful preview and time to a selectable ArcGIS layer are different metrics; report both.
