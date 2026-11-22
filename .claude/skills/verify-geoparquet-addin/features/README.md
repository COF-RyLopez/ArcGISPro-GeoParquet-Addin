# Feature map — GeoParquet add-in

The user-facing features of the add-in, each with how to drive its data core from this Mac. Read a feature's file before driving it; the map is the maintained verification source, so when the wizard gains a feature, add a file here.

| Feature | File | What it proves |
| --- | --- | --- |
| Load an Overture theme | [load-overture-theme.md](load-overture-theme.md) | Extent-filtered ingest → display table → source archive round trip |
| SQL attribute filter | [sql-attribute-filter.md](sql-attribute-filter.md) | Filter validation (trust boundary), filtered ingest, filtered preview |
| Custom data source | [custom-data-source.md](custom-data-source.md) | URL probe, geometry-column pick, load of non-Overture GeoParquet |
| GeoParquet export version | [geoparquet-export-version.md](geoparquet-export-version.md) | 1.x vs 2.0 COPY output, version metadata in the written file |

Not mappable from this Mac (Windows/ArcGIS Pro only): ribbon and dialog rendering, dockpane XAML bindings, adding layers to the Pro map, MFC creation, file-geodatabase export (needs Pro's geoprocessing). Those are proven by the manual Windows test steps posted on each PR.
