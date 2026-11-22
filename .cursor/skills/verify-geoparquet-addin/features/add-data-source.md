# Add Data Source

Add Data Source lets a user save any http(s) or s3 GeoParquet URL as a named
custom source without opening the download wizard first. The URL is validated
with DuckDB and stored for Custom Sources.

## Sub-features

- `add-open` opens the Add GeoParquet Data Source dialog from the ribbon.
- `add-validate` rejects empty/duplicate/unsupported URLs with status text.
- `add-save` persists a valid source and shows `Added '…'`.

## How to get to it (user POV)

- Ribbon **Overture Maps** → group **Data Sources** → **Add Data Source**.

## Driving it with UIA helpers

Preconditions:

- `doctor.ps1` → `DOCTOR_OK`.
- No existing custom source with the same name (use a `verify-` prefix).
- Network can DESCRIBE the target URL (or expect `Could not add source: …`).

- **Open dialog.**  
  `pwsh -File .cursor/skills/verify-geoparquet-addin/helpers/drive-add-data-source.ps1 -Name "verify-smoke" -Url "https://github.com/opengeospatial/geoparquet/raw/main/examples/example.parquet"`  
  Or manually: invoke button `Add Data Source`. Window `Add GeoParquet Data Source` appears.
- **Fill fields.** Sort dialog `Edit` controls by Y; set Name then URL via `ValuePattern`.
- **Add.** Invoke button `Add`. Status shows `Validating …` then `Added 'verify-smoke' (geometry column: …)` on success.
- **Proof.** Copy project `OvertureProAddinData\config\user_sources.json` into the evidence folder; JSON must contain the name and URL. Helper writes `$env:VERIFY_EVIDENCE_DIR\add-data-source\`.

## Gotchas

- Blank `Name`/`URL` → status `Enter a name and a URL.`
- Duplicate sanitized name → `already exists` (not a product bug).
- Only `http://`, `https://`, and `s3://` are accepted — not local `file://`.
- Dialog edits often have blank UIA Names; use Y-order, not AutomationId.
