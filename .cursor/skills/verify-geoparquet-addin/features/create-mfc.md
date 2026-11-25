# Create MFC

Create MFC opens a dockpane that builds a Multifile Feature Connection from
GeoParquet downloaded by this add-in (or from a custom folder) and can open
the resulting `.mfc` in the project.

## Sub-features

- `mfc-open` opens the Create Overture MFC dockpane from the ribbon.
- `mfc-settings` shows data-source radios, output folder path, and Create MFC.
- `mfc-run` creates `OvertureData.mfc` when a valid source folder is available
  (optional for smoke; open + control inventory is the default proof).

## How to get to it (user POV)

- Ribbon **Overture Maps** → **Create MFC**.
- Dock caption **Create Overture MFC**.

## Driving it with UIA helpers

Preconditions:

- `doctor.ps1` → `DOCTOR_OK`.
- For a full create: a prior download snapshot (or custom folder with per-type
  subfolders). Without that, only open/smoke the pane.

- **Open.** Select `Overture Maps`, invoke `Create MFC`. Dock shows heading
  `Create Multifile Feature Connection` and button `Create MFC`.
- **Smoke fields.** Confirm radios `Use previously loaded data (recommended)` /
  `Use custom data folder`, `MFC Output Folder` edit, and post-create actions
  `Open Folder` / `Copy Path` / `Add to Project`. Type paths with
  `ValuePattern` — never Browse.
- **Full create (optional).** With a valid source, invoke `Create MFC`; wait for
  status `MFC created successfully: …` and an `.mfc` on disk.
- **Proof.** Open-only: dock caption + button inventory under
  `$env:VERIFY_EVIDENCE_DIR\create-mfc\`. Full run: copy the `.mfc` path and
  status/log text into that folder.

## Gotchas

- Full MFC generation inspects schemas and can be slow on large releases.
- Browse dialogs are unreliable under automation — type folder paths.
- `Create MFC` stays disabled until a resolvable source/output path exists
  (`CanCreateMfc`); missing prior load → smoke-only, not a harness failure.
