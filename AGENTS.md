# Agent guidance for ArcGISPro-GeoParquet-Addin

This file is for AI coding agents (Claude Code, Cursor, Codex/ChatGPT, Copilot,
and anything else that reads `AGENTS.md`). It covers what the tools can't infer
from the code.

## What this is

An ArcGIS Pro SDK add-in (C#/.NET, WPF/DAML) that downloads GeoParquet data —
Overture Maps, OpenStreetMap LayerCake, Source Cooperative, and custom URLs —
through an embedded native DuckDB engine (DuckDB.NET) with the `spatial` and
`httpfs` extensions. Downloads are spatially filtered, written as versioned
snapshot folders, and added to the map as styled layers.

## Build and test environments

- **Any machine**: `dotnet test DuckDBGeoparquet.Tests` runs the ArcGIS-free
  test suite against the real DuckDB engine. It must stay green. The test
  project compiles a curated set of product files (linked in its csproj);
  code referenced from those files must not pull in ArcGIS assemblies.
- **Windows only**: the full add-in (`DuckDBGeoparquet.csproj`) compiles only
  where the ArcGIS Pro SDK is installed. ArcGIS-facing code (anything under
  `Views/` touching `ArcGIS.*`, plus `Config.daml`) cannot be compiled or run
  on macOS/Linux — validate DAML/XAML with `xmllint --noout` and leave UI
  verification to a Windows run.
- Features ship as **draft PRs** with a Windows test-steps comment on the PR;
  a separate Windows agent executes them and posts results. **Never merge,
  approve, close, or mark a PR ready** — the repository owner decides merges.

## ArcGIS Pro SDK guidance (adapted from Esri's arcgis-pro-sdk-copilot)

Vendored from https://github.com/Esri/arcgis-pro-sdk-copilot (Apache-2.0);
see `docs/ai/README.md` for provenance.

- **Before generating any Pro SDK C#, WPF/XAML, or DAML**, read and follow
  `docs/ai/arcgis-pro-sdk-best-practices.md` (threading via `QueuedTask`,
  DAML conventions, WPF patterns, resource handling).
- **When adapting examples** from Esri samples, snippets, or docs, follow
  `docs/ai/arcgis-pro-sdk-example-resources.md`: the version-matched API
  reference is authoritative; samples are non-authoritative starting points.
- **Pattern questions** (Add-in vs Configuration vs Plugin Datasource vs
  CoreHost, extensibility points, limitations): follow
  `.claude/skills/arcgis-pro-sdk-pattern-help/SKILL.md` and its references.
- **Adding ribbon items/dockpanes/tools** to this add-in: follow
  `.claude/skills/arcgis-pro-sdk-item-generator/SKILL.md` (ignore its VS Code
  extension delegation; generate directly).
- **Broken ArcGIS Pro references after a Pro upgrade** (Windows): run the
  scripts in `.claude/skills/arcgis-pro-sdk-fix-pro-references/`.

The `.claude/skills/` folder is auto-loaded by Claude Code; other agents
should read the SKILL.md files above as plain documentation when the topic
comes up.

## Project invariants (do not break)

- The whole filter pipeline works in **WGS84 (EPSG:4326)**; anything captured
  from the map is projected before use (`Services/ExtentResolution.cs` holds
  the testable decisions).
- `GeoParquetSql.ValidateAttributeFilter` is a **trust boundary** for
  user-typed SQL fragments. After changing it, its deny list, or how filters
  are embedded, run the `formal-verify-attribute-filter` skill (DuckDB-parser
  fuzz harness).
- Snapshot folders are immutable once committed (`Services/DownloadSnapshot.cs`):
  staging → validate → atomic rename → pointer file. Folder names are
  provider-tagged (`OSM_…`, `Overture_<release>_…`); `DescribeFolder` renders
  display names and must keep reading legacy folder names.
- `Services/PresetCatalog.cs` is the **single source** of built-in dataset
  names/URLs — the wizard tree and ribbon both read it; never duplicate the
  list.
- DAML ids are stable API — renaming them breaks user configs; change
  captions, not ids.
- Proxy passwords are never written to logs.
- The data core is testable on any OS: keep new decision logic in ArcGIS-free
  classes linked into the test project (see `SelectableThemeItem`,
  `ExtentResolution`, `GeoParquetSql` for the pattern).

## Verification skills (this repo)

- `.claude/skills/verify-geoparquet-addin/` — drives the DuckDB data core with
  the real engine on any OS; per-feature proof recipes.
- `.claude/skills/formal-verify-attribute-filter/` — adversarial fuzz of the
  attribute-filter validator against the real DuckDB parser.
