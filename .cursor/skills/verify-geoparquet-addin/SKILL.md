---
name: verify-geoparquet-addin
description: >-
  Verify the ArcGIS Pro GeoParquet / Overture Maps add-in on Windows: build and
  register the .esriAddInX, drive the live Pro UI with UIA, and capture proof
  artifacts. Use when proving ribbon buttons, dockpanes, Custom Sources, or
  download behavior after a change.
---

# Verify GeoParquet Add-in (ArcGIS Pro)

Cold-start guide for an agent that has never opened this app. The surface is a
**desktop ArcGIS Pro add-in** (WPF ribbon + dockpanes). Secondary surfaces
(unit tests, CI) are not substitutes for this live proof.

Also read [AGENTS.md](../../../AGENTS.md) for UIA hang rules (never
`TogglePattern.Toggle` / `ExpandCollapsePattern.Expand` on Pro).

## Isolate

ArcGIS Pro is a **shared single-instance** desktop host on this machine. Do not
start a second Pro to "isolate" a run — it will fight the user's session.
Doctor the existing window or relaunch **one** Pro for verification. Never
kill Pro by process name unless this run started that exact PID and documented
it in `$env:VERIFY_PRO_PID`.

## Launch

From the repo root (`ArcGISPro-GeoParquet-Addin`):

```powershell
pwsh -File .cursor/skills/verify-geoparquet-addin/helpers/launch.ps1
```

What launch does:

1. Ensures bundled DuckDB extensions exist under `Extensions/` (see
   `Extensions/README.txt` / `build/DuckDbVersion.props`).
2. Builds Release `"Any CPU"` with VS MSBuild.
3. Clears AssemblyCache `{9e44d8fc-6e1a-4513-9992-85c2de3e2d61}`.
4. Runs `RegisterAddIn.exe` on
   `bin\Any CPU\Release\net10.0-windows\DuckDBGeoparquet.esriAddinX`.
5. Starts Pro with `$env:VERIFY_APRX` if set, else leaves start page for the
   agent to open a project (prefer Recent → `MyProject2`).

Ready means: a Pro window exists **and** the **Overture Maps** ribbon tab is
present (add-in loaded). A bare `ArcGISPro.exe` process is not enough.

Optional env:

| Variable | Purpose |
|----------|---------|
| `VERIFY_APRX` | Full path to a `.aprx` to open |
| `VERIFY_EVIDENCE_DIR` | Evidence root (default `%TEMP%\verify-geoparquet-addin\<run-id>`) |
| `VERIFY_MSBUILD` | Override MSBuild path |

## Doctor

```powershell
pwsh -File .cursor/skills/verify-geoparquet-addin/helpers/doctor.ps1
```

Exit `0` only when all of these hold:

- ArcGIS Pro window found (`MyProject2` preferred, else `ArcGIS Pro`)
- **Overture Maps** tab item exists (add-in registered)
- Optional: if `VERIFY_REQUIRE_PROJECT=1`, title must be a project name (not bare `ArcGIS Pro`)

Doctor is read-only. Run it before the first drive, after any failed drive, and
on every fresh Pro session.

## Drive

Harness: **UI Automation + mouse click / ValuePattern** via helpers under
`helpers/`. Prefer accessible **Name** captions from `Config.daml`:

| User control | UIA name |
|--------------|----------|
| Ribbon tab | `Overture Maps` |
| Download wizard | `Download Overture` |
| Geocoder | `Overture Geocoder` |
| GERSify | `GERSify Your Data` |
| Create MFC | `Create MFC` (dock caption `Create Overture MFC`) |
| Add custom URL | `Add Data Source` |
| Dialog title | `Add GeoParquet Data Source` |
| Dialog buttons | `Add`, `Close` |
| Loader dock | `Overture Maps Data Loader` / `Add to map` |

Type paths and URLs with `ValuePattern.SetValue`. Never open Browse dialogs.

Feature recipes live in [features/](features/README.md). Merge overlapping
setup into one Pro state, then drive features serially.

## Evidence

Default directory: `$env:VERIFY_EVIDENCE_DIR` or
`%TEMP%\verify-geoparquet-addin\<yyyyMMdd-HHmmss>\`.

Each proof folder should include:

- `action.log` — commands and UIA actions taken
- `before.png` / `after.png` — screenshots via computer-use or
  `helpers/capture-window.ps1` when available
- Side-effect reads: e.g. `user_sources.json` copy, snapshot folder listing,
  arcpy `dataSource` lines

Proof standards: exercise the real ribbon/dialog path; capture action +
resulting state; verify side effects (JSON/files/layers), not only the final
screenshot. Network-backed downloads need a working path to Overture S3 (or a
proxy configured in add-in settings).

**Cleanup must not delete this evidence directory.**

## Cleanup

```powershell
pwsh -File .cursor/skills/verify-geoparquet-addin/helpers/cleanup.ps1
```

Removes verification scratch only (test custom sources named `verify-*`,
temp downloads under a run-tagged folder if created). Does **not** kill Pro
unless `$env:VERIFY_PRO_PID` was set by `launch.ps1` in this run and
`$env:VERIFY_KILL_PRO=1`. Does **not** delete `$env:VERIFY_EVIDENCE_DIR`.

## Helpers

All under `.cursor/skills/verify-geoparquet-addin/helpers/`:

| Script | Invocation |
|--------|------------|
| `launch.ps1` | `pwsh -File .../helpers/launch.ps1` |
| `doctor.ps1` | `pwsh -File .../helpers/doctor.ps1` |
| `cleanup.ps1` | `pwsh -File .../helpers/cleanup.ps1` |
| `drive-add-data-source.ps1` | `pwsh -File .../helpers/drive-add-data-source.ps1 -Name <n> -Url <url>` |
| `Resolve-ProWindow.ps1` | Dot-sourced by other helpers |

## Maintain

After the app changes, run `/maintain-verification-skill` against this skill
so the feature map stays honest.
