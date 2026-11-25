# Download Overture

Download Overture lets a user pick themes and a map extent, then write a local
GeoParquet snapshot and add display layers to the map.

## Sub-features

- `download-open` opens the Overture Maps Data Loader dockpane from the ribbon.
- `download-themes` selects a downloadable leaf (e.g. Building under Buildings)
  once themes are visible (release inventory may still be finishing).
- `download-add` runs Add to map and finishes with a versioned snapshot on disk
  and a TOC group named `Overture <snapshotFolder>`.

## How to get to it (user POV)

- Ribbon **Overture Maps** → **Download Overture**.
- Dock auto-hide tab **Overture Maps Data Loader** if the pane was pinned away.

## Driving it with UIA helpers

Preconditions:

- `doctor.ps1` reports `DOCTOR_OK` and a map project is open (`VERIFY_REQUIRE_PROJECT=1`).
- Theme list shows Overture parents (e.g. Buildings); `Add to map` stays disabled
  while `IsLoading` / release check is in progress.
- Network can reach Overture S3 (or a configured proxy).

- **Open loader.** Select tab `Overture Maps`, invoke button `Download Overture` (or `Overture Maps Data Loader`). Dock shows `Add to map`.
- **Wait for themes.** Poll for text `Buildings`. Themes can appear before release
  discovery finishes; do not click `Add to map` while it is disabled.
- **Select Building.** Expand **Buildings** if needed (mouse-click the header —
  never `ExpandCollapsePattern`). Mouse-click the **Building** leaf checkbox
  (`Content` is the leaf display name). Parent **Buildings** is tri-state only.
- **Add to map.** Invoke enabled button `Add to map`. Wait until `Finish` is
  enabled or the dock returns to idle with a new TOC group
  `Overture <release>_…`.
- **Proof.** Confirm a new folder under
  `OvertureProAddinData\Data\<release>\<release>_<timestamp>_<guid>\` with
  `snapshot.json` (and often `current-snapshot.json` beside the release folder),
  and a TOC layer whose `dataSource` points at that snapshot. Save evidence under
  `$env:VERIFY_EVIDENCE_DIR\download-overture\`.

## Gotchas

- `Add to map` stays disabled with no leaf themes selected or while releases are still loading.
- Alternate path: **Preview >** then **Download Data** on the Preview tab runs the same pipeline.
- `TogglePattern` / expander `Expand()` hang Pro — click instead.
- History or Sign In overlays block the dock; dismiss before selecting themes.
- Large themes at continental extents are slow; use a small map scale (e.g. ~1:25k) for smoke runs.
