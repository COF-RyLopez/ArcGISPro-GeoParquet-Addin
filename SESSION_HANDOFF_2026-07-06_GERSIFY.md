# Session Handoff - GERSify Data - 2026-07-06

Use this as the starting brief for the next Codex session.

## Copy/Paste Prompt

```text
Continue work in /Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin on branch codex/gersify-esri-tool.

Read SESSION_HANDOFF_2026-07-06_GERSIFY.md first, then continue from there.

Important context:
- We are building the "GERSify Data" tooling in the ArcGIS Pro Overture add-in.
- The immediate proof point is Fresno County address data, but do not hard-code the product around Fresno. The long-term goal is that any jurisdiction, with its own schema and naming conventions, can map local data to stable Overture GERS IDs.
- This should become the aha moment for local governments and GIS vendors: local data can be connected to the broader Overture Maps ecosystem, traceable source records, repeatable cartography, cross-release stable IDs, and downstream workflows.
- Current branch is pushed through commit 11f4260 Fallback to known ZIP fields.
- Tests passed locally: dotnet test DuckDBGeoparquet.Tests/DuckDBGeoparquet.Tests.csproj --no-build -v minimal, 97/97 passing.
- On macOS, the add-in project compiles DuckDBGeoparquet.dll but the final Esri packaging target fails with the known CodeTaskFactory/ConvertToRelativePath issue. Full add-in validation should happen on Windows with ArcGIS Pro.

Please verify the current state first, avoid redoing finished work, and focus on the remaining items listed in this handoff. Keep the tool jurisdiction-agnostic even while using Fresno data as the test fixture.
```

## Product North Star

GERSify Data should let local governments and GIS vendors attach stable Overture GERS IDs to their own authoritative data, even when their schema does not look like Overture's schema.

The tool should demonstrate that Overture is not just downloadable basemap data. It can become a connection layer:

- Local records can be matched to stable GERS IDs.
- GERS IDs can bridge local authoritative records to Overture themes.
- Users can trace GERS IDs back to source datasets through bridge files.
- Local data can benefit from Overture-aware cartography and enrichment.
- Governments can compare their own data against the broader Overture ecosystem.
- Vendors can build workflows around durable IDs instead of fragile names, addresses, or geometry-only joins.

Fresno County data is the current proving ground. The architecture should stay generic enough for other counties, cities, states, utilities, MPOs, school districts, campus systems, and vendor schemas.

## Current Branch / Repo State

- Repo: `/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin`
- Branch: `codex/gersify-esri-tool`
- Working tree was clean at handoff time.
- Latest pushed commits:
  - `11f4260` Fallback to known ZIP fields
  - `e1e9242` Tighten address match labeling
  - `da90cd4` Map Fresno street direction fields
  - `8b16875` Add address target to GERSify
  - `0778706` Preserve place address fields for GERSify
  - `9752321` Isolate add-in dock pane grouping

## What Was Built

### GERSify Data dockpane

The GERSify dockpane now supports target selection instead of being Places-only.

Current targets:

- `Addresses`: strict address validation against Overture Addresses.
- `Places`: POI/place enrichment against Overture Places.

Important behavior:

- `Addresses` is the default because the presentation/demo currently centers on validating authoritative Fresno address points.
- `Allow nearby-only fallback` defaults off. This is intentional. The address validation story should not be based on proximity-only matches.
- Places matching still supports nearby fallback when explicitly enabled.
- Output layers are named with the target suffix, such as `..._address`.

### Overture Addresses matching

The address target uses Overture `addresses/address` GeoParquet files.

Signals used:

- House number equality.
- Street/address similarity.
- ZIP compatibility when ZIP is available.
- Distance within the configured threshold.
- Candidate ranking that prioritizes house-number and ZIP-compatible candidates.

Strategy labels:

- `exact_address`: near-literal address equality, currently `address_similarity >= 0.995`, with house number and ZIP compatibility.
- `address`: accepted address match that is strong but not near-literal exact.
- `nearby_only`: should not appear for strict address runs.

The `exact_address` threshold was tightened because unit-suffixed local records, such as `5071 E BELMONT AVE A`, were matching a base Overture address and being overstated as exact. They should still be accepted where appropriate, but labeled more honestly as `address`.

### Places address scoring

Places matching was improved to preserve/use flattened and nested place address fields.

Important fixes:

- Overture Places downloads/read paths use `union_by_name=1`.
- Places address fields are flattened when needed.
- If an address-only input is matched against old Places files without usable address fields, the tool fails fast instead of silently producing nearby-only results.

### Schema-specific Fresno fixes that should become generic behavior

Fresno data exposed useful schema realities:

- Parsed address fields may use names like `ADDRESS_NUMBER`, `STREET_DIRECTION`, `STREET_NAME`, `STREET_TYPE`, `STREET_POST_DIRECTION`, `ADDRESS_UNIT`, `ADDRESS_ZIPCITY`, `ADDRESS_ZIP5`.
- Direction fields are essential. Without `N/E/S/W`, most true 1:1 matches were scoring as weaker `address` instead of `exact_address`.
- The tool now auto-detects Fresno-style street direction fields.
- A postcode guard now prevents non-numeric fields like `STREET_TYPE` from being exported as postcode. If the selected postcode value has no digits, the exporter falls back to known ZIP field names like `ADDRESS_ZIP5`, `ZIP5`, `POSTCODE`, `POSTAL_CODE`, and `ZIP`.

This is a patch, not the final UX. The better long-term solution is a field-mapping preview/validator that shows exactly what the tool resolved before a user runs a 400k-row job.

## Latest Fresno Run Evidence

The latest reported full run used:

- Input layer: `geoprod.REGIONAL_VIEWS.ADDRESS`
- Input count: `398,762` point features
- Input address coverage: `398,756` with addresses
- Target: `Overture Addresses (addresses/address)`
- Overture candidate address coverage: `1,501,127` with address text and similarity scores
- Accepted matches: `298,638`
- Accepted by strategy:
  - `exact_address`: `277,026`
  - `address`: `21,612`
- Candidate review CSV: `gersify_candidates_20260706_014719.csv`
- Bridge CSV: `gers_bridge_20260706_014719.csv`

This is a strong demo result. It shows the value proposition:

- A large authoritative government address layer can be connected to stable Overture GERS IDs.
- Most accepted matches are true exact address matches after schema-aware field mapping.
- The remaining `address` matches can be used for review or lower-confidence workflows.

## Important Issues Found

### 1. Postcode mapping can be wrong in the UI

One sample output showed `postcode` values like `AVE`, `ST`, `RD`, and `DR`.

The source table screenshot showed `ADDRESS_ZIP5` exists and contains proper ZIP values, so the output issue likely came from the Postcode combo box pointing at `STREET_TYPE`.

Mitigation already pushed:

- `11f4260` adds a fallback so non-numeric selected postcode values are replaced by known ZIP fields when possible.

Recommended next step:

- Add an explicit field-mapping preview/validation surface before running:
  - Show selected Unique ID, Full Address, Number, Prefix, Street, Type, Suffix, Unit, City, State, Postcode.
  - Show a small sample of built output strings.
  - Warn when Postcode has no digits or looks like street type values.
  - Warn when Street Direction exists in the source but is not selected.
  - Warn when Address Unit exists and Overture lacks unit-level address candidates.

### 2. Need better unmatched analysis

The latest run matched `298,638` of `398,762`, leaving about `100k` unmatched.

That may be normal depending on:

- Input extent vs downloaded Overture extent.
- Address points not included in current Overture release.
- Unit-level/subaddress records not represented in Overture.
- Non-site addresses, retired addresses, or records outside the selected data extract.
- Threshold too strict for some rural or unusual addressing.

Recommended next step:

- Add an unmatched/review summary:
  - No nearby candidates.
  - House number mismatch.
  - Street similarity below threshold.
  - ZIP incompatible.
  - Unit/subaddress only.
  - Candidate outside distance threshold.

This would make the tool much more compelling for government data stewardship because it turns "unmatched" into a work queue.

## Remaining Product Work

### Highest priority

1. Verify the latest pushed commit on Windows/ArcGIS Pro:
   - Pull `codex/gersify-esri-tool`.
   - Rebuild add-in.
   - Run the Fresno regional address layer again.
   - Confirm output `postcode` contains real ZIP values, not street types.
   - Confirm `exact_address` count remains high and unit-suffixed rows are labeled `address` when not truly exact.

2. Add field mapping preview/validation:
   - This is the biggest UX improvement for non-Fresno jurisdictions.
   - Users need confidence that their schema was interpreted correctly before a long matching job.

3. Add a match summary/report card:
   - Accepted by strategy.
   - Unmatched reason buckets.
   - Candidate counts.
   - Address/name coverage.
   - Recommended next action.

### Make it jurisdiction-agnostic

Do not keep adding Fresno-only field names directly into workflow logic forever. Better direction:

- Keep common aliases, but organize them into reusable schema profiles.
- Add a scoring/ranking system for field auto-detection.
- Let users save and reuse mappings per organization/dataset.
- Detect suspicious mappings from sample values, not only field names.
- Support both full-address fields and parsed-address fields.
- Handle state/country/postcode variants across jurisdictions.
- Keep a clear distinction between:
  - Required fields.
  - Useful context fields.
  - Optional review/enrichment fields.

### Expand beyond Addresses and Places

The long-term goal is GERSification across Overture themes, not just address points.

Potential targets:

- `addresses/address`: authoritative address validation.
- `places/place`: POI/facility enrichment.
- `buildings/building`: local building footprints or facility/building inventories.
- `divisions/*`: jurisdiction/administrative boundary linking.
- `transportation/segment`: local centerlines, road assets, crash records, maintenance segments.
- `base/land`, `base/water`, etc. may be less about record matching and more about cartographic/context workflows.

Each theme probably needs its own matching strategy:

- Addresses: number/street/ZIP/distance.
- Places: name/address/category/distance.
- Buildings: geometry overlap, centroid distance, parcel/address context.
- Divisions: polygon overlap/name/admin level.
- Transportation: geometry similarity, topology, road name, class, endpoints.

The UI should expose "Match Target" while hiding theme-specific complexity unless needed.

### Stable GERS ID workflows

The aha moment should not stop at matching.

Build toward workflows like:

- Add GERS IDs to local data.
- Trace GERS IDs back to Overture source records.
- Re-run against a new Overture release and compare changed/missing/stable records.
- Join local data to Overture attributes by GERS ID.
- Apply Overture-aware cartography to GERSified local layers.
- Create bridge CSV outputs that vendors/governments can use in ETL pipelines.
- Export a review package for data stewards.

Cartography idea from the user:

- Once local data is GERSified, the add-in can quickly apply Overture-style symbology or theme-aware cartography.
- This is a strong presentation point: instead of manually configuring many layers, a government user can use GERS IDs/theme metadata to enrich and style their data quickly.

## Useful Files

- [Config.daml](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/Config.daml)
- [Models/GersifyOptions.cs](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/Models/GersifyOptions.cs)
- [Models/GersifyResult.cs](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/Models/GersifyResult.cs)
- [Services/GersSql.cs](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/Services/GersSql.cs)
- [Services/GersifyService.cs](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/Services/GersifyService.cs)
- [Views/GersifyDockpane.xaml](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/Views/GersifyDockpane.xaml)
- [Views/GersifyDockpaneViewModel.cs](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/Views/GersifyDockpaneViewModel.cs)
- [DuckDBGeoparquet.Tests/GersSqlTests.cs](/Users/ryanlopez/Desktop/ArcGISPro-GeoParquet-Addin/DuckDBGeoparquet.Tests/GersSqlTests.cs)

## Verification Notes

Commands that passed locally:

```bash
dotnet build DuckDBGeoparquet.Tests/DuckDBGeoparquet.Tests.csproj --no-restore -v minimal
dotnet test DuckDBGeoparquet.Tests/DuckDBGeoparquet.Tests.csproj --no-build -v minimal
```

Test result:

- `97/97` passing.

Known macOS limitation:

- `dotnet build DuckDBGeoparquet.csproj --no-restore -v minimal` compiles `DuckDBGeoparquet.dll`, then fails in Esri packaging with `CodeTaskFactory` / `ConvertToRelativePath`.
- Treat this as expected on macOS; verify package/add-in behavior on Windows with ArcGIS Pro.

## Suggested Next Move

Start with a Windows validation run:

1. Pull `codex/gersify-esri-tool`.
2. Rebuild the add-in in Visual Studio.
3. Run GERSify Data against `geoprod.REGIONAL_VIEWS.ADDRESS`.
4. Confirm the output `postcode` values are real ZIPs.
5. Inspect a few unit/subaddress cases and confirm strategy labels are honest.
6. Capture exact/address counts for the presentation.

Then implement field-mapping preview/validation. That is the next best investment because it converts the Fresno-specific lessons into a tool that any jurisdiction can trust.
