---
name: verify-geoparquet-addin
description: "Drive the GeoParquet add-in's data core (DuckDB ingest/export pipeline) on this Mac with the real DuckDB engine and capture proof that a change works. Use after changing Services/ SQL builders, ingest, export, preview, or filter code. The ArcGIS Pro wizard UI itself only runs on Windows — see Gotchas in each feature file for what this harness cannot prove."
---

# Verify the GeoParquet add-in (data core)

The user-facing app is an ArcGIS Pro add-in; ArcGIS Pro runs only on Windows, so the wizard UI cannot be driven from this checkout. What CAN be driven here, with the **real DuckDB engine** (DuckDB.NET + the spatial extension), is the entire data core the wizard calls: `GeoParquetSql`, `S3Ingester`, `UserSourceStore`, `DuckDBManager`, `PreviewBridge`, snapshot/export writers. Every query the wizard runs on Windows is built by these classes, so a proof here covers everything except the XAML/MapView glue. UI-level verification happens as manual test steps posted on the PR, run on a Windows machine with ArcGIS Pro (see `Gotchas` in each feature file for what that leaves to Windows).

## Launch

There is no server. Launch = build the test project once:

```
dotnet build DuckDBGeoparquet.Tests/DuckDBGeoparquet.Tests.csproj
```

Ready = `Build succeeded`. Each drive then runs in its own `dotnet test` process with its own in-memory DuckDB and its own GUID-named temp folder, so drives are isolated and parallel-safe; nothing shared is touched. First run on a fresh machine needs the .NET 10 SDK (`dotnet --version` ≥ 10) and network access once, for NuGet restore and for DuckDB to `INSTALL spatial`.

## Doctor

Is this checkout worth driving? One read-only check — it builds, opens the real engine, loads the spatial extension, and round-trips a parquet file:

```
dotnet test DuckDBGeoparquet.Tests/DuckDBGeoparquet.Tests.csproj --filter "FullyQualifiedName~DuckDBExecutionTests.ExportRoundTrip"
```

Pass = the harness works. Fail on `INSTALL spatial` = no network for the extension download; fix that before blaming the change.

## Drive

A drive is a scratch xunit file dropped into the test project, written the way `DuckDBGeoparquet.Tests/DuckDBExecutionTests.cs` is written (copy its shape: in-memory `DuckDBConnection`, `Spatial()` loader, GUID temp folder, `IDisposable` cleanup). The pattern:

1. **Build a fixture** parquet with real SQL — `COPY (SELECT ...) TO '<file>' (FORMAT 'PARQUET')` — never hand-craft bytes.
2. **Call the same builders the wizard calls** (the feature files name them per feature) and execute the returned SQL on the connection.
3. **Assert the observable end state**: rows in `current_table`, files on disk read back with `read_parquet(...)`, error messages returned to the user.

Name the file `Scratch_<Feature>Tests.cs` in `DuckDBGeoparquet.Tests/`, run only it, and tee the output to evidence:

```
dotnet test DuckDBGeoparquet.Tests/DuckDBGeoparquet.Tests.csproj --filter "FullyQualifiedName~Scratch_" --logger "console;verbosity=normal" 2>&1 | tee .claude/skills/verify-geoparquet-addin/evidence/<date>-<feature>.log
```

Which feature to drive and with which builders: read `features/README.md` and the feature file. A proof that drives one convenient entry point is incomplete when the feature file lists others.

Proof standards: exercise the path the ViewModel takes (validate first, then build, then execute — in that order), not internal shortcuts; assert the action AND the resulting state (e.g. for an export, re-read the written file with `read_parquet`, don't just check it exists); when a filter or guard is the feature, prove both the accept and the reject side.

## Evidence

Evidence lives in `.claude/skills/verify-geoparquet-addin/evidence/` (create it if missing) and survives cleanup. A proof is: the teed test log naming each assertion that ran, plus, when a file was written, a few read-back rows pasted into the log or saved beside it. The log must show the drive's test names passing — a bare exit code is not evidence.

## Cleanup

Delete the scratch file(s) you created — `rm DuckDBGeoparquet.Tests/Scratch_*.cs` — and nothing else; `git status` must show no leftover tracked changes from the drive. Per-drive temp folders are GUID-named under the system temp dir and deleted by each test's `Dispose`; don't hunt processes (there are none to kill — never `pkill dotnet`, other sessions may be building). Never delete the `evidence/` folder.

## Helpers

None shipped: the harness is `dotnet test` plus the shape already in `DuckDBExecutionTests.cs`, which is maintained with the product code and therefore never stale.
