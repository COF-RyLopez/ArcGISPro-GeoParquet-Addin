# Custom Sources in loader

Custom Sources shows user-saved GeoParquet URLs inside the Overture Maps Data
Loader so they can be selected like themes and removed when obsolete.

## Sub-features

- `custom-list` shows saved sources under the **Custom Sources** parent in
  **1. Choose Themes** after add.
- `custom-select` selects a custom leaf for download/preview.
- `custom-remove` removes the source tied to the selected custom leaf via
  **Remove Selected**.

## How to get to it (user POV)

- Open **Download Overture** / Data Loader. Saved names appear as leaves under
  the **Custom Sources** parent inside **1. Choose Themes** (not under the lower
  form expander titled **Custom Sources (any GeoParquet URL)** — that panel is
  Name/URL + **Add Source** / **Remove Selected**).
- Sources appear after **Add Data Source** (or in-pane **Add Source**) writes
  `user_sources.json`.

## Driving it with UIA helpers

Preconditions:

- At least one custom source exists (drive `add-data-source` first with a `verify-` name).
- Loader dock is open and themes/custom section is visible (scroll if needed).

- **Reveal theme parent.** Mouse-click the **Custom Sources** row in the theme
  tree to expand (not the lower URL-form expander).
- **See source.** Find leaf text matching the saved name under that parent.
- **Select for remove.** Click the custom leaf checkbox (or its named label) so
  `SelectedItemForPreview` carries a `UserSource`, then invoke **Remove Selected**
  in the lower panel.
- **Proof.** `user_sources.json` no longer lists the removed name; evidence copy
  before/after under `$env:VERIFY_EVIDENCE_DIR\custom-sources\`.

## Gotchas

- Collapsed **Custom Sources** hides leaf checkboxes from UIA — mouse-click the
  theme expander header before looking for the saved name.
- If the ribbon add ran while the loader dock was never shown this session,
  `RebuildCustomSourcesNode` may no-op (`Instance` null); reopen **Download
  Overture** so `InitializeThemes` reloads `user_sources.json`.
- **Remove Selected** stays disabled until a custom leaf is selected
  (`SelectedItemForPreview?.UserSource != null`).
- In-pane add button is **Add Source**; ribbon dialog is **Add Data Source**.
- Expanding with `ExpandCollapsePattern.Expand()` can hang — click the header.
