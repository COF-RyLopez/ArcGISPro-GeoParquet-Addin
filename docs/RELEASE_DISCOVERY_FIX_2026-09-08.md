# Windows validation: catalog 403 and retired cached release

The Windows log shows that bundled DuckDB extensions loaded and settings initialized. Release discovery then returned HTTP 403. The loader retained cached release `2026-06-17.0` and repeatedly tried nonexistent dataset globs. The log does not establish whether the 403 came from the catalog service, a proxy/network policy, or an `OVERTURE_RELEASE_URL` override.

On September 8, direct public S3 listing confirmed zero address keys for June and at least one for `2026-08-19.0`. The release inventory listed July and August. The live STAC catalog also reported August. This reproduces the missing-source symptom independently of ArcGIS. The `IsPortalProject`, `ProjectTitleImage`, and `PerfTools` diagnostics refer to host UI/resources not defined by this add-in; changing our view-model bindings would not address the failed data requests.

## Fix

- Release discovery first uses the STAC catalog (or configured override), then the public S3 release-prefix inventory. It handles pagination, validates dates, and sorts numeric release revisions correctly. It never substitutes a hardcoded or cached date for a failed discovery result.
- Initialization awaits discovery before enabling downloads. If both sources fail, the UI explicitly labels the displayed cached date unverified and offers manual entry. **Refresh releases** retries discovery without restarting ArcGIS.
- Preview and download share a preflight request checking for dataset keys under the selected release. A retired release fails once, before per-type DuckDB queries or snapshot creation, with instructions to select a published date. Manual entry is syntax-checked and still goes through preflight.
- Release and per-file catalog requests now share system-proxy credential behavior, a 15-second timeout, and an identifiable client header. This does not bypass or change network access policy. If Windows blocks both metadata hosts, access still needs to be resolved locally.

The official quickstart documents catalog discovery and the current source path: [Overture Quickstart](https://docs.overturemaps.org/getting-data/). The fallback requests the public bucket's ListObjectsV2 inventory, not a third-party release list.

## Validation

- A deterministic test injecting the Windows HTTP 403 initially failed, then passed after the fallback fix.
- **182 tests pass**, including catalog success, invalid responses, pagination, numeric revisions, cancellation, complete discovery failure, retired/current release preflight, and invalid manual dates.
- A temporary .NET harness exercised the actual service against live metadata: primary discovery found August; an injected catalog 403 recovered August through live S3; August passed preflight; June was blocked. No Parquet datasets were downloaded by this probe.
- C# and generated XAML compiled with zero warnings. Full packaging still stops at Esri's Windows-only `CodeTaskFactory`/`ConvertToRelativePath` tasks on macOS. The Windows network behavior and UI require another user run.

For the Windows retry, stop debugging/close ArcGIS Pro, pull this branch with `git pull --ff-only`, rebuild, and open the loader. Confirm the displayed release/source, then start with one small-area dataset. If automatic discovery remains unavailable, applying `2026-08-19.0` is a verified workaround **as of September 8**; future retries should use the then-current catalog/inventory value.
