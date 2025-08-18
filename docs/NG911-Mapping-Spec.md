## Overture → NG911 one-to-one mapping (initial spec)

Reference: NENA NG9-1-1 GIS Data Model Templates (NENA-STA-006.x) https://github.com/NENA911/NG911GISDataModel?tab=readme-ov-file

This document enumerates proposed 1:1 field mappings for two NG911 targets. Transformations and domain mappings to follow.

### SiteStructureAddressPoint
- AddNum: address_number | number | house_number | housenumber
- AddNum_Pre: (derive from label if available) [TBD]
- AddNum_Suf: [TBD]
- PreDir: street_name_pre_dir
- PreType: street_name_pre_type
- StName: street_name | name
- StType: street_name_post_type
- PostDir: street_name_post_dir
- LandUnit: unit | unit_number
- Municipal: city | locality
- State: region | state
- ZipCode: postcode | postal_code | zip
- Country: [TBD]
- PlaceType: [TBD]
- Validation: [TBD]
- SourceOID: id
- Source: source
- geometry: geometry

Notes:
- Directionals and type fields align where present. Number components may require parsing from a full label in edge-cases.
- Domain alignment (e.g., directionals, validation) will require translation tables in a subsequent phase.

### RoadCenterline
- PreDir: street_name_pre_dir
- PreType: street_name_pre_type
- StName: street_name | name
- StType: street_name_post_type
- PostDir: street_name_post_dir
- RoadClass: road_class | class
- OneWay: one_way
- ParityLeft: [TBD]
- ParityRight: [TBD]
- SourceOID: id
- Source: source
- geometry: geometry

Notes:
- Parity and additional NG911-specific road attributes may need derivation or local enrichment.

### Validation approach
- The add-in validates available Overture columns for each NG911 target and reports coverage/missing fields before ETL.
- ETL proceeds using the best available 1:1 mapping; missing fields are filled with NULL until enrichment rules are defined.


