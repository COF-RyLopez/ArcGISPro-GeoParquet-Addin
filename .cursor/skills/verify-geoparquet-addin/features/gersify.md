# GERSify Your Data

GERSify links an authoritative map point layer to Overture GERS IDs via a local
release folder, producing a GERSified feature class, candidate/bridge CSVs, and
optional map review / Trace Sources.

## Sub-features

- `gersify-open` opens the GERSify Your Data dockpane.
- `gersify-inputs` shows Input Layer combo, release/output path edits, and
  field/threshold controls (default smoke: inventory only).
- `gersify-run` executes **Run GERSify** when layer + ID + match fields are set
  (long-running; optional).

## How to get to it (user POV)

- Ribbon **Overture Maps** → **GERSify Your Data**.

## Driving it with UIA helpers

Preconditions:

- `doctor.ps1` → `DOCTOR_OK`.
- A local point layer suitable for linkage and (usually) a prior Overture
  release folder. Without those inputs, only open/smoke the pane.

- **Open.** Select `Overture Maps`, invoke `GERSify Your Data`. Dock
  `GERSify Your Data` is visible; note tab **Trace Sources**.
- **Smoke fields.** Confirm Input Layer / Refresh, release and output path
  edits, and **Run GERSify** (often disabled until unique ID + name or address
  fields). List dock buttons into evidence.
- **Full run (optional).** Only when the user/map supplies real inputs — type
  paths (no Browse), invoke **Run GERSify**, wait for output GDB/CSVs.
- **Proof.** For open-only: screenshot/dock caption in
  `$env:VERIFY_EVIDENCE_DIR\gersify\`. For a full run: output CSVs/GDB plus map
  review layers.

## Gotchas

- Full GERSify runs are heavy; default verification is open + control inventory
  unless inputs are prepared.
- Error dialog title may say **GERSify Data** while ribbon/dock say
  **GERSify Your Data**.
- Path Browse dialogs are unreliable under automation — type paths with
  `ValuePattern`.
- Missing entitlement or empty inputs → `verified-unreachable`, not a silent
  skip marked as pass.
