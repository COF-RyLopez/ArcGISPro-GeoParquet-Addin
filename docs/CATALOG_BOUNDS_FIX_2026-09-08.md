# Land-cover catalog bounds rejected a valid study area

The reported Fresno extent is valid WGS84. Replaying it through the live August land-cover catalog reproduced the exact `ArgumentException`: `OvertureCatalog.ValidateAsset` called the map-area validator on a catalog item's bounding box. The next theme after land use was land cover, so the message appeared after successful water and land-use exports.

The [first land-cover item](https://stac.overturemaps.org/2026-08-19.0/base/land_cover/00000/00000.json) contained this box during verification:

```json
[-180.00022888183594, -80.46366882324219, -56.77799606323242, 20.327512741088867]
```

Its western bound lies slightly outside the longitude limit. The previous validator rejected the entire catalog lookup with instructions to redraw the user's area. The loader has a catalog fallback catch, but Visual Studio can stop at the thrown exception before that recovery runs.

## Changed behavior

User-area validation remains strict. Catalog bounds are treated as file-selection hints with their own rules:

- Valid axes continue to prune files normally.
- An out-of-range/nonfinite axis, or reversed latitude interval, is expanded to the full geographic range for that axis. Unknown or malformed bounds retain the file with world bounds. This can read extra candidate files but cannot exclude a candidate because of uncertain bounds.
- West greater than east represents an antimeridian-crossing box and is tested against both sides. [RFC 7946 section 5.2](https://www.rfc-editor.org/rfc/rfc7946#section-5.2) defines that representation; the [STAC Item specification](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#bbox) uses GeoJSON bounding boxes.
- Both four-value and six-value STAC boxes are supported; elevation is ignored for horizontal selection.
- The same handling applies to cached inventories. Asset URL validation remains in place. Source geometry and the requested area are never changed; DuckDB still applies the row-level spatial filter.

## Validation

A regression test using the captured values failed with the user's exact message before the fix. Afterward **194 tests passed**, covering the live values, uncertain-axis inclusion, latitude pruning, cache reuse, legacy cached bounds, wrapped boxes, malformed bounds, 3D boxes, and continued rejection of invalid user areas.

A temporary .NET harness then queried the live August land-cover catalog with the reported Fresno extent: **four files selected, no extent exception**. Repeating from cache returned the same result without network access. This verified metadata selection, not an end-to-end Windows data load.

C# and generated XAML compiled with zero warnings. On macOS, Esri packaging still stops at its Windows-only build tasks. Windows should pull the branch, rebuild, and retry the same area; redrawing it is unnecessary for this defect.
