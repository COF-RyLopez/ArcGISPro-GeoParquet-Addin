# Hybrid Locator Build and Publish Guide

This guide describes how to build the hybrid Overture locator and publish it for broader ArcGIS use.

## Scope

- Local hybrid search inside the add-in uses Overture `address` and `place` parquet data.
- Locator build/rebuild is triggered from the geocoder dockpane.
- Publishing supports:
  - ArcGIS Online/Portal locator package item updates.
  - ArcGIS Server geocode service publication (stand-alone server workflow).

## 1) Build the Hybrid Locator in ArcGIS Pro

1. Load Overture `address` and `place` data with the add-in.
2. Open **Overture ROI Geocoder** dockpane.
3. Click **Build Locator** (or **Rebuild Locator**).
4. Confirm status text reports locator readiness.

The add-in writes locator metadata to:

- `OvertureProAddinData/Locators/overture_locator_metadata.json`

## 2) Publish Locator Package to ArcGIS Online/Portal

Use:

- `.github/workflows/publish-locator-item.py`

Example:

```bash
python ./.github/workflows/publish-locator-item.py \
  --auth-method token \
  --client-id "$AGOL_CLIENT_ID" \
  --client-secret "$AGOL_CLIENT_SECRET" \
  --portal-url "https://www.arcgis.com" \
  --locator-package "./release/OvertureHybrid.gcpk" \
  --item-id "$LOCATOR_ITEM_ID" \
  --title "Overture Hybrid Locator" \
  --tags "ArcGIS,Locator,Geocoding,Overture"
```

## 3) Publish Geocode Service to ArcGIS Server

Use:

- `.github/workflows/publish-geocode-server.py`

This script requires an ArcGIS Pro Python environment with `arcpy`.

Example:

```bash
python ./.github/workflows/publish-geocode-server.py \
  --locator-path "C:/path/to/OvertureHybrid.loc" \
  --server-connection "C:/path/to/publisher.ags" \
  --service-name "OvertureHybridGeocode" \
  --out-folder "./release" \
  --overwrite
```

## Validation Checklist

- Hybrid search returns both **Address** and **Place** candidates.
- Locator build succeeds after loading ROI data.
- Rebuild succeeds after refreshing Overture data.
- AGOL/Portal locator package upload updates the expected item.
- ArcGIS Server publish script produces and uploads `.sd` successfully.

## Notes

- The ArcGIS Server script targets stand-alone server publishing and is intended for admin/ops environments.
- Keep locator publish optional so local/offline geocoding remains available even without server/portal access.
