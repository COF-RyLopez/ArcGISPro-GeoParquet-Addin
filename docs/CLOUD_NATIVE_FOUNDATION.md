# Cloud-native foundation: implementation and next steps

This branch implements the first modernization slice: reliable regional downloads, preserved source data, simpler map loading, and more conservative identity matching. It was created from `origin/main` at `ed00783` (v0.8.0), retaining the newer GERS review, source-edit, and tracing features already on main.

The broader product remains a staged build. This branch does **not** implement a routing engine, an NG911 production dataset, an upstream publishing API, or editable remote Parquet layers.

Windows testing exposed a catalog HTTP 403 followed by use of a retired cached release. The [September 8 fix](RELEASE_DISCOVERY_FIX_2026-09-08.md) adds S3 discovery fallback, release preflight, and an explicit retry/status UI; its expanded suite passes 182 tests.

A second Windows finding exposed out-of-range land-cover catalog bounds. The [catalog bounds fix](CATALOG_BOUNDS_FIX_2026-09-08.md) separates file-pruning hints from user-area validation; 194 tests pass and the reported Fresno area selects four live land-cover assets successfully.

## User workflow

1. Open a map, select themes, and choose the map extent or a custom area.
2. Choose a map purpose. Save options are available in a collapsed section; defaults work without configuring compression or source-link fields.
3. Select **Add to map**, or optionally inspect a capped sample with **Preview**.
4. Inspect the result summary. Empty datasets and failures are distinct. Failed downloads do not replace the previous snapshot.
5. New layers appear in a named snapshot group. Once every replacement layer loads, matching layers from previous snapshots are hidden and remain available for comparison. An incomplete group stays hidden while the previous map remains visible.

The static per-square-kilometer estimates were removed from the visible workflow: they were not measurements of the selected area. Actual feature counts are reported during the job and recorded in the manifest.

## Download and data contracts

`DuckDBJobQueue` serializes jobs on each connection and executes native work off the UI thread. Cancelling an active job interrupts DuckDB; cancelling a queued job cannot interrupt a different active job. Initialization, ingestion, preview, local search, export, and GERS matching use this boundary. ArcGIS layer changes still run on its required main CIM thread.

`OvertureCatalog` reads a dated release's STAC collection and complete item inventory, caches the inventory, and selects overlapping file bounds before DuckDB opens Parquet footers. At most eight item requests run concurrently. Incomplete inventory is never cached. Unsupported catalog metadata or availability errors fall back to the full release/type listing with spatial filtering. The first catalog read has its own cost; warmed catalog selection avoids repeating the inventory requests.

The selected features intersect the requested WGS84 area, but retain whole original geometry. This preserves the meaning of source IDs and road measures. The extract is not necessarily a closed routing graph: referenced connectors and restrictions outside the area still need dependency expansion in the routing stage.

Each job stages a unique snapshot:

```text
Data/<release>/
  .catalog/                         complete STAC inventories
  .staging/<snapshot>/              unpublished work
  current-snapshot.json             pointer to the published snapshot
  <snapshot>/
    snapshot.json                   release, UTC time, area, engine, results
    _source/<type>.geoparquet        complete selected source attributes/geometry
    <type>/*.parquet                 ArcGIS display projections
```

Custom save roots can omit the release level. The `.geoparquet` source extension deliberately keeps the source archive outside display consumers' `*.parquet` patterns. Source archives remain readable by DuckDB.

The manifest records per-type row counts, source URI, selected catalog assets when available, relative source/display files, compression, DuckDB version, and the `whole_intersecting_features` policy. Display projections may omit nested fields; the source archive preserves them regardless of the optional display-link setting. Export explicitly requests GeoParquet V1 for compatibility with the existing ArcGIS path. Apostrophes in file paths are escaped.

All requested datasets must complete or be empty before publication. Files must exist, be nonempty, and remain inside staging. Publication renames the completed directory and replaces a same-directory pointer. Old snapshots are retained. Discovery ignores unfinished staging and unpublished snapshot directories, including failed attempts under a new release. It understands legacy per-release/type folders.

Map adoption is separate from disk publication. A layer-creation failure leaves a complete saved snapshot for recovery and an accurate partial count; it does not claim the entire map loaded. Previous layer matching uses file paths from snapshot manifests, plus the legacy release/type layout, rather than editable layer names. Only refreshed dataset types are hidden. Other types can remain visible from older snapshots; consumers such as geocoding deliberately use the latest published snapshot rather than silently combining releases.

## Address identity and auditability

GERS candidate bounds now include the matching radius. Candidate generation handles wrapped longitude and polar searches, followed by WGS84 spheroidal distance. DuckDB's `POINT_2D` distance overload takes latitude before longitude; the SQL constructs that order explicitly. See [DuckDB spatial functions](https://duckdb.org/docs/stable/core_extensions/spatial/functions).

Automatic address GERS assignment requires the complete normalized street address, including supplied unit, to agree. Fractional numbers, suffixes, and hyphenated numbers are no longer reduced to leading digits. Fuzzy candidates remain available for review; stricter acceptance can reduce automatic match rates.

Local geocoding searches postal city and units and retains the full query. Batch geocoding does not successively remove locality until a weak hit appears. It accepts only one compatible complete-address candidate with a matching structured house number and valid coordinates. Scores remain heuristics, not probabilities. The candidate ceiling is 100; broad or ambiguous searches still need human review.

Every processed file record produces a JSON decision containing original source ID, starting line, raw record, matched/review/unmatched status, and candidate alternatives. Blank-address records stay in the report. Quoted multiline CSV records retain their starting line. Separate unit fields are preserved even alongside a full-address field. Existing row limits are reported. Successful matches can still be added as a feature class. GERS input uses the selected layer's query scope, including its definition query.

## Validation and release gate

Validation on September 7, 2026: **169 tests passed, zero failed or skipped** (132 tests on the main baseline). `git diff --check` passed. C# and generated XAML compilation completed without warnings; the full build stopped only at the Esri packaging tasks described below.

The portable test suite uses the same DuckDB.NET 1.5.3 native package as the add-in. It includes real SQL execution for source/display separation, complete road geometry, quoted paths, explicit file selection, full-number/unit matching, postal-city queries, polar/dateline distances, native cancellation, and subsequent connection reuse. Filesystem fixtures cover failed/empty/complete snapshots, discovery across releases, legacy compatibility, and layer replacement plans. Catalog tests inject metadata responses and verify pruning, cache reuse, and refusal to publish partial inventories.

Run:

```text
dotnet test DuckDBGeoparquet.Tests --nologo --verbosity minimal
```

On macOS the full add-in build compiles C# and generated WPF sources, then Esri packaging fails because its `CodeTaskFactory`/`ConvertToRelativePath` packaging tasks require the Windows build environment. This is not a packaged or runtime-tested ArcGIS release.

Before release, on Windows with ArcGIS Pro:

- Build/package and install the add-in, including matching bundled extensions. Test extension initialization both online and offline.
- Load address, building, and transportation data for a small area. Verify selection, tables, identify, symbology, labels, and MFC discovery. Inspect `_source` nested attributes with DuckDB.
- Confirm Add to map works without WebView2 preview. Exercise a projected map, custom extent, map switch during download, and keyboard navigation.
- Cancel during query and export; simulate a network failure. Existing layers/files must remain usable. Retry both the same and a newer release.
- Simulate one layer failing to open. Check the hidden incomplete group, actual added count, recoverable files, and prior visibility. Verify a complete refresh hides only matching prior files across releases.
- Test GERS with a definition query, duplicate business IDs, fractions, units, and nearby boundary points. Verify review decisions and source links. Inspect geocode reports containing blank and multiline input records.
- Benchmark fixed small/medium/large areas, cold/warm catalog and storage caches, and display links on/off. Record source assets touched, elapsed stages, output bytes, peak memory, cancellation latency, and map draw time. Compare against the same release and whole-feature policy; no percentage speedup is claimed here.

## Next implementation milestones

| Milestone | Concrete deliverable | Evidence required before promotion |
| --- | --- | --- |
| Search performance | Reusable normalized local address/place search tables, batched candidate generation, explicit snapshot invalidation | Throughput and precision/recall on a labeled regional corpus; memory and cold/warm timing |
| Familiar layer experience | Project snapshot browser, coverage/release badges, reopen/rollback actions, related source tables, editable correction workspace | Windows usability sessions covering novice download, inspect, select, analyze, and recover tasks |
| High-quality contributions | Reviewable change packages keyed by GERS ID and release; original/proposed values and geometry, source authority/license, evidence, reviewer, and disposition | Schema, geometry, duplicate and attribution checks; reconciliation against newer releases; accepted provider/Overture submission workflow |
| Routable transportation pilot | Buffered source/dependency extraction and connector-based graph; lineage back to source segments/measures; ArcGIS route result layer | Known-route comparisons; disconnected components, direction/access, bridges/tunnels, internal connectors, turn restrictions, and boundary tests |
| NG911 foundation pilot | Jurisdiction profile, field crosswalks, address/road candidates, service-boundary reconciliation, quality report and editable review layers | Local steward approval; authoritative address/road/service-boundary comparison; positional, topology, completeness, and access validation |

Prefer extending the existing Overture routing toolchain over inventing a competing topology interpretation. Evaluate OMF-Bifrost/Valhalla behind a replaceable local process interface, with pinned versions and a Windows distribution/license review. Overture connector IDs and internal connector measures define connectivity; visual road intersections are insufficient. Emergency-vehicle behavior requires its own validated policy and data, beyond ordinary car routing.

Contribution packages should support provider correction and Overture issue/contribution workflows without promising a transactional edit API that does not exist. Publishing a correction is a separate explicit user action after review. Keep jurisdiction-specific NG911 fields in a versioned extension/derived schema with source lineage; never overwrite canonical Overture attributes to imply authority they do not carry.

The NG911 pilot still needs a jurisdiction and its authoritative address, road, and emergency-service boundary sources. Overture and other open data can identify gaps and prepare candidates; an extract alone is not dispatch-ready. The target standards/profile, data licenses, stewardship, and acceptance tests must be recorded for that jurisdiction.

Current tooling research and primary sources are captured in [platform research](PLATFORM_RESEARCH_2026-09-07.md), [routing and NG911 research](OVERTURE_ROUTING_NG911_RESEARCH_2026-09-07.md), and the [original review](MODERNIZATION_REVIEW_2026-09-07.md). Keep DuckDB/native extensions pinned together; evaluate upgrades through the interoperability and performance suite rather than adopting development-only features.
