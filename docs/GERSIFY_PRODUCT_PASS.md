# GERSify Your Data — Product Pass (CGIA × Overture)

This pass aligns the ArcGIS Pro add-in with Overture's **link, don't replace** message for local-government audiences (CGIA webinar, July 2026).

## Product goal

Help counties and cities keep their own authoritative layers as the system of record while attaching stable **GERS IDs** for interoperability, enrichment, source tracing, repeatable cartography, and cross-release workflows.

GERSify does **not** replace geometry, overwrite source features, or imply migration to Overture data.

## Where to find it in ArcGIS Pro

1. Open the **Overture Maps** ribbon tab.
2. Click **GERSify Your Data** (same group as Download Overture and Overture Geocoder).
3. Use the dockpane tabs:
   - **GERSify Your Data** — match authoritative points to Overture Addresses or Places.
   - **Trace Sources** — look up upstream source records for existing GERS IDs via Overture bridge files.

## Trust-first workflow (recommended demo)

1. **Load Overture data** for the area of interest (Addresses and/or Places) with Download Overture.
2. **Select the authoritative input layer** (e.g., county address points, facilities, NG911-related sites).
3. **Choose a durable Unique ID field** — prefer `GlobalID`, `site_id`, or program GUIDs. Avoid `OBJECTID`/`FID` (unstable across reloads). Enable **Generate stable link IDs** when only row pointers exist.
4. **Review the Linkage Preview** — shows fields added to the new output layer and bridge CSV provenance columns. The source layer is not modified.
4. **Map match fields** and inspect the mapping preview sample.
5. **Run GERSify** with thresholds appropriate to data quality.
6. **Review artifacts before adoption:**
   - `gersify_candidates_*.csv` — all scored candidates for manual QA (open from **Review Last Run**).
   - `gers_bridge_*.csv` — accepted links with provenance metadata.
   - New `GERSified_*` feature class — copy of input geometry plus linkage fields.
7. **Optional map relate** — when enabled, adds a non-destructive relate from your authoritative layer to the GERSified output on your unique ID. Use the Attributes pane → related records to inspect `gers_id` and match scores without editing source geometry.
8. **Map review (Review Last Run)** — after a run, use **Show Unmatched** or **Show Weak Links** to filter and zoom the GERSified output layer on the map. Unmatched sets of 3,000 or fewer also select the corresponding authoritative features. **Clear Review** removes filters and selections.
9. **Improve Overture at the source** — use **Trace Linked Sources** to query Overture bridge files for upstream providers (OpenStreetMap, Esri Community Maps, Meta, Microsoft, PinMeTo). Open OSM features directly; other providers include contribution guidance so fixes can flow into the next Overture release.
10. **Trace sources** (optional) — the Trace Sources tab supports ad-hoc lookups for any layer with `gers_id` values.

## Outputs and provenance

### New output feature class (source layer unchanged)

Linkage fields appended to a **new** layer:

| Field | Purpose |
| --- | --- |
| `record_id` | Stable bridge key (business key value, or UUID when generated) |
| `source_record_key` | Original selected field value for audit and map relates |
| `gers_id` | Stable Overture GERS identifier |
| `gers_match_score` | Combined match score (0–100 scale) |
| `gers_match_strategy` | How the match was accepted (`exact_address`, `address`, `name`, `nearby_only`, etc.) |
| `gers_match_distance_m` | Distance to Overture candidate (meters) |
| `gers_name_similarity` | Name similarity when available |
| `gers_address_similarity` | Address similarity when available |
| `overture_name`, `overture_address`, `overture_id` | Matched Overture candidate context for review |

### Bridge CSV (accepted matches)

Standard bridge columns plus provenance extensions:

| Column | Example | Purpose |
| --- | --- | --- |
| `id` | GERS UUID | Overture GERS ID |
| `record_id` | local key | Stable bridge key (UUID or business key) |
| `source_record_key` | OBJECTID / site_id | Original selected field value |
| `dataset` | `fresno_addresses` | Dataset label for downstream systems |
| `theme`, `type` | `addresses` / `address` | Overture theme/type |
| `linkage_policy` | `link_dont_replace` | Documents non-replacement intent |
| `source_geometry_policy` | `authoritative_local` | Local geometry remains authoritative |
| `match_strategy` | `exact_address` | Acceptance strategy |
| `match_score` | `92.4` | Score at acceptance time |
| `overture_release` | `2026-06-17.0` | Overture release used for matching |
| `source_layer` | layer name | Provenance back to map layer |
| `linkage_tool` | `arcgis_pro_gersify` | Tool that produced the link |

## Audience entry points (webinar framing)

| Agency need | GERSify path |
| --- | --- |
| Address authority / NG911 alignment | Match to **Overture Addresses** with strict house-number and postcode checks |
| Facilities / POI enrichment | Match to **Overture Places** for category and cross-map interoperability |
| Source maintenance / escalation | **Trace Sources** tab + bridge CSV provenance |
| "We won't replace our footprints" | Linkage policy fields + new output layer (no in-place overwrite) |

## ID strategy (local governance vs interoperability)

- **Local business key** (`site_id`, NG911 GUID, `GlobalID`): keep as your system-of-record identifier in `source_record_key` and/or `record_id`.
- **GERS ID** (`gers_id`): add as the cross-ecosystem interoperability anchor — not a replacement for local governance.
- **OBJECTID/FID**: avoid for bridge/relate keys; enable **Generate stable link IDs** to write UUID `record_id` values while preserving the row pointer in `source_record_key` for the current map session.

## Validation checklist (local government review)

- [ ] Source layer feature count unchanged after run
- [ ] Output layer contains only linkage additions plus copied attributes
- [ ] Candidates CSV reviewed for false positives near threshold
- [ ] Bridge CSV `linkage_policy` and `source_geometry_policy` present on accepted rows
- [ ] `overture_release` matches the release folder used for matching
- [ ] Map relate (if enabled) shows linkage fields on authoritative features via related records
- [ ] Trace Sources returns expected upstream datasets for sample GERS IDs

## Related add-in capabilities

- **Download Overture** — load ROI GeoParquet for Addresses/Places
- **Overture Geocoder** — search loaded Overture data locally
- **Create MFC** — multifile feature connection for Overture themes
