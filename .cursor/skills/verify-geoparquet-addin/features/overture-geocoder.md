# Overture Geocoder

Overture Geocoder searches address and place GeoParquet already downloaded for
the current project (under `OvertureProAddinData\Data\<release>\`), not merely
layers visible in the TOC, and can zoom or write results to the map.

## Sub-features

- `geocode-open` opens the Overture Geocoder dockpane.
- `geocode-search` runs a text search (Search button or Enter) against loaded
  address/place parquet.
- `geocode-map` zooms or **Add Results to Map** (new point FC) when candidates exist.

## How to get to it (user POV)

- Ribbon **Overture Maps** → **Overture Geocoder**.

## Driving it with UIA helpers

Preconditions:

- `doctor.ps1` → `DOCTOR_OK`.
- Project has address/place parquet from a prior Download Overture load of
  **Addresses and Places** (UI copy). Missing data → empty search is
  `verified-unreachable`, not a harness failure.

- **Open.** Select `Overture Maps`, invoke `Overture Geocoder`. Dock caption
  `Overture Geocoder` appears.
- **Search.** Focus the search field, `ValuePattern` a known token, invoke
  **Search** (or press Enter). Status like `Found N candidate(s)…` or no-match text.
- **Proof.** Results list or status shows matches; optional zoom / **Add Results
  to Map**. Evidence under `$env:VERIFY_EVIDENCE_DIR\overture-geocoder\`.

## Gotchas

- Empty results without downloaded address/place data are a missing prerequisite.
- Hybrid locator checkbox / **Geocode File...** exist but are out of scope for
  the default three sub-features unless explicitly exercised.
- Do not treat an empty result set as a harness failure when no searchable
  parquet exists — record the missing prerequisite.
