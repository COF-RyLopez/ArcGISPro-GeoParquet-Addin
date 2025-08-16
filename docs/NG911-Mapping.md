## NG911 Prepopulation Plan (Overture → NENA-STA-006.x)

This document outlines an incremental plan to prepopulate the NENA NG9-1-1 GIS Data Model using Overture Maps data within this ArcGIS Pro add‑in.

Reference standard: [NENA NG9-1-1 GIS Data Model Templates (`NENA911/NG911GISDataModel`)](https://github.com/NENA911/NG911GISDataModel?tab=readme-ov-file)

### Goals
- Rapidly bootstrap NG911-compliant layers for a user’s Area of Interest (AOI) from Overture themes already supported by the add‑in.
- Lean on existing downloader and DuckDB/GeoParquet pipeline; add mapping steps that reshape attributes and geometry as needed.
- Provide a pathway to improve Overture schemas over time via OMF contributions while delivering immediate value now.

### Initial NG911 Targets
- Site/Structure Address Points (`SiteStructureAddressPoint`)
- Road Centerlines (`RoadCenterline`)
- Emergency Service Boundaries (e.g., `ESB`, `PSAPBoundary`) when data is available locally

### High-Level Workflow
1. Select Overture theme(s) and AOI in the wizard (existing UI).
2. Download and filter to AOI using current `DataProcessor`.
3. Apply mapping rules (schema/attribute transforms, domain value mapping, geometry normalization) to produce NG911‑ready GeoParquet.
4. Add resulting layers to the map and/or export to a target geodatabase as needed.

### Mapping Strategy (Incremental)
- Start with deterministic attribute mappings (e.g., address number, street name parts, place type) where Overture fields align or can be derived.
- Maintain a small translation layer for domains (directionals, road class, one‑way, parity, etc.).
- Track gaps requiring local enrichment or schema evolution discussions with Overture.

### Deliverables in This Branch
- `Services/Ng911Mapper.cs`: Stub service to host mapping rules and orchestration.
- Iterative mapping implementations beginning with Address Points and Road Centerlines.

### Notes
- The add‑in targets ArcGIS Pro 3.5 (.NET 8). New code must remain compatible with that runtime.


