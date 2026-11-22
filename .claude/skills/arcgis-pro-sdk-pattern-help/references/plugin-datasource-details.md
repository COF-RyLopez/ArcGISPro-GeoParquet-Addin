<!--
Copyright 2026 Esri
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    https://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
```
license: Apache-2.0
metadata:
  author: Esri, Inc.
  version: "1.0.0"
```
# ArcGIS Pro SDK Plugin Datasources: Technical Implementation Details

## Scope

This document provides an overview of the ArcGIS Pro SDK Plugin Datasource model, with emphasis on:

- Plugin datasource core implementation model
- Key implementation details and characteristics including:
 - Tables, Search, and Row cursor behavior
- Packaging and Deployment
- Instantiation and Delay-Loaded Lifecycle
- Execution Contract
- Catalog view integration

This is an implementation guide, not a high-level pattern comparison. This document complements the Plugin Datasource description in [Patterns Summary](./patterns-summary.md) with additional details. For pattern comparison, refer to [Patterns Decision Guide](./patterns-decision-guide.md).

## Core Implementation Model

Summary:
A Plugin Datasource extension allows third parties to integrate custom and proprietary data sources into ArcGIS Pro as geodatabase-compatible tables
or feature classes.

Plugin Datasources can participate in all ArcGIS Pro data, mapping, query, and analysis workflows HOWEVER data access via a plugin datasource is READ ONLY. There is NO built-in support for data or schema editing.

In addition, they are a non-visual extensibility pattern and do not support deployment of UI or DAML. A Plugin Datasource implementation must use
ArcGIS.Core APIs only; adding references to or accessing `ArcGIS.Desktop.*` APIs from the plugin can crash ArcGIS Pro. Use a separate Add-in or
Configuration when related UI is required.

The Plugin Datasource architecture contains five primary components. Providers implement the datastore, table, and cursor template classes; the
connector and row wrapper are supplied by the API.

Connector: 
- The connector defines the connection to the Plugin Datasource.
- The connector implementation is provided BY THE API.
- The connector uses `ArcGIS.Core.Data.PluginDatastore.PluginDatasourceConnectionPath`.

Datastore: 
- Represents the active connection to the custom or proprietary data store, file store, or service.
- The datastore implementation must derive from `ArcGIS.Core.Data.PluginDatastore.PluginDatasourceTemplate`.
- The datastore acts as a per-thread singleton that manages the lifecycle, opens connection paths, and returns table instances.

Table: 
- Represents the individual datasets or entities within the datastore.
- The table implementation must derive from `ArcGIS.Core.Data.PluginDatastore.PluginTableTemplate`. 
- The table provides read, search, and query capabilities for the underlying data (such as CSV files).
- Tables containing a shape column are treated as a feature class by ArcGIS Pro

Cursor:
- The row cursor streams individual records sequentially (usually in response to a query).
- The row cursor derives from `ArcGIS.Core.Data.PluginDatastore.PluginCursorTemplate`. 
- The cursor delivers forward-only, read-only cursor access to selected rows.

Row:
- The row encapsulates the field values for a specific record. Clients iterate through a cursor to return rows.
- Rows are materialized through `ArcGIS.Core.Data.PluginDatastore.PluginRow` instances.
- PluginRow is provided BY THE API.
- Providers are responsible for populating plugin rows with the correct field/value pairs in the correct order.

## Key Implementation Details and Characteristics

Dynamic Table Discovery: 
- The datasource is responsible for translating its internal storage structure into logical table names via `GetTableNames()` on the PluginDatasourceTemplate implementation (e.g., exposing local proprietary data filenames as uppercase table names without file extensions).
- `OpenTable(string name)` should retrieve the requested data as a "table" retrieved from the relevant internal store or cache.
- Requested table names should be validated against GetTableNames().

Per-Thread Isolation: 
- Plugin datastore tables acting as data sources for layer or tables will be instantiated from the ArcGIS Pro CIM thread pool
- While multiple instances of a plugin datasource can exist simultaneously (one per active executing thread), a single thread from the CIM pool will ALWAYS access its OWN dedicated instance. Plugin datasources, therefore, act as a per-thread singleton.
- The number of framework threads is an implementation detail and must not be treated as a fixed SDK contract.
- Developers relying on static data shared across plugin instances are responsible for managing thread safety and concurrent access.

Note: Use of static data is NOT recommended.

Lifecycle Hooks: 
- `Open()` and `Close()` on the custom datasource demarcate the lifecycle of a thread-specific datasource instance. 
- They are called ONCE during initialization and destruction respectively
- `Open()` is responsible for any required setup and caching
- `Close()` is responsible for resource cleanup (such as disposing tables and freeing unmanaged resources).

SQL Support:
- `IsQueryLanguageSupported()` determines whether the plugin datasource supports SQL where-clauses via QueryFilter.
- Return true to indicate SQL query language is supported. Otherwise return false.

Refer to `Query Execution` for specific details on query processing.

### Tables

Spatial table classification:
- Include a field of type `FieldType.Geometry` in `GetFields()` and ArcGIS Pro treats the dataset as a feature class.
- If no geometry field is returned, ArcGIS Pro treats the dataset as a non-spatial table. `GetShapeType()` should return a valid `GeometryType` for
feature classes and `GeometryType.Unknown` for non-spatial tables.

`GetExtent()`: 
- For spatial datasets (i.e. `GetShapeType()` returns a valid `GeometryType`), returning a valid extent is REQUIRED (even if the table is empty). 
- The extent is used to establish:
 - Spatial reference
 - HasZ/HasM properties
 - Used by the map whenever a layer is added or "zoomed to full extent".

`GetLastModifiedTime()`: 
- Added in ArcGIS Pro 3.1, `GetLastModifiedTime()` allows the framework to detect underlying data modifications. When a newer timestamp is returned, ArcGIS Pro automatically refreshes the full and default layer extents—provided the developer updates the extent returned by GetExtent() prior to updating the timestamp.
- This is useful for when your underlying custom datasource has changed and you need ArcGIS Pro to refresh its cache.

`IsNativeRowCountSupported()` and `GetNativeRowCount()`: 
- Return true from `IsNativeRowCountSupported()` if your underlying data store implements `GetNativeRowCount()` to compute counts efficiently.
- Implementing `GetNativeRowCount()` bypasses the performance penalty of manual cursor iteration (e.g. via a SELECT COUNT(*) query in SQL).
- If `IsNativeRowCountSupported()` returns false, the framework is forced to execute a full search query to manually count every row.

`GetFields()`: 
- Returns a collection of `PluginField` objects representing the column type and order within the table.
- `PluginField` order also determines the `PluginField` order of retrieved search rows
- Each `PluginField` requires a NON-NULL alias name.

### Search

Query Execution ("Search"):
- `PluginTableTemplate` implementations must implement BOTH `Search(QueryFilter queryFilter)` for attributes and `Search(SpatialQueryFilter queryFilter)` for spatial queries
- Search method implementations must handle object IDs, where-clauses, spatial relationships, subfields, and output spatial references.
- If a user attempts a spatial query on a non-spatial Plugin Datasource table (that is, `GetShapeType()` returns `GeometryType.Unknown`), the implementation must explicitly throw an exception.
- Tables that do not support a spatial column must throw an exception from `Search(SpatialQueryFilter)` if it is called.
- Tables must handle SQL-like WhereClause filtering if the parent datasource returns true from `IsQueryLanguageSupported()`.
- If `IsQueryLanguageSupported()` returns false, the framework does not relay the user-provided where clause to the plugin's `Search` method. The
framework filters returned rows itself. Object ID predicates may still be represented through `QueryFilter.ObjectIDs`, which the implementation must handle.

ObjectIDs (OIDs): 
- ArcGIS Pro relies on ObjectIDs (FieldType.OID) for selection, editing tracking, and feature identification.
- If a WHERE clause targets a FieldType.OID field, the framework will populate `QueryFilter.ObjectIDs`. 
- If the underlying source lacks native OIDs, the plugin implementor is responsible for synthesizing them manually (e.g., by mapping sequential row indices 1, 2, 3... to unique OIDs).

Intersection Rules: 
- When multiple filter constraints are applied simultaneously (such as a list of specific ObjectIDs combined with a WhereClause or a SpatialQueryFilter), the plugin datasource is responsible for computing the INTERSECTION of all conditions rather than handling them independently.

### Row Cursor:

`MoveNext()`:
- The cursor implementation responds to `MoveNext()` to traverse selected records row-by-row on the creating thread.
- Cursor navigation is forward-only with READ-ONLY access via the cursor to selected rows

`GetCurrentRow()`:
 - returns a `PluginRow` wrapper containing a `Values` property containing the relevant row fields and their values
- `GetCurrentRow()` returns null if the cursor is past the end of the record set.
- The order of returned columns MUST match the SAME order specified by `GetFields()`
- If a column is filtered out (i.e. excluded from `QueryFilter.SubFields`) an empty placeholder MUST still be provided even though the actual value is skipped. 
- Field alias names are mandatory and CANNOT be left blank.

## Packaging and Deployment

Packaging:
- Plugin datasources are packaged as `.esriPlugin` archives and deployed similar to standard ArcGIS Pro add-ins via `RegisterAddin.exe` or well-known folders. However, a key behavioral nuance is that installed plugin datasources do NOT appear in the Add-in Manager backstage tab within ArcGIS Pro.

Config.xml Manifest: 
- Every plugin datasource must declare its unique identifier inside its configuration manifest file, Config.xml (similar in purpose to Config.daml for add-ins and configurations). Config.xml contains a `<PluginDatasource>` element containing a plugin datasource identifier. This identifier explicitly binds the physical code implementation of the plugin datasource to the `PluginDatasourceConnectionPath.PluginIdentifier` connector. 

Keep one `<PluginDatasource>` declaration per package unless the version-matched schema and API documentation explicitly support another arrangement.

## Instantiation and Delay-Loaded Lifecycle

There are some similarities AND differences between how plugin datasources are handled at runtime compared to add-ins. 

Discovery:
- Similar to add-ins, the framework scans a series of well-known folders at start-up for the presence of Plugin Datasources. The default Plugin
Datasource deployment folder is versioned, unlike the versionless default Add-in folder. ArcGIS Pro catalogs plugins internally by the identifier in `Config.xml`.
- **Unlike** add-ins, the discovered plugin datasources are NOT shown on the Add-in Manager UI but are registered in an internal look up within the **native** tier only.

Instantiation/Activation:
- Plugin datasources can be instantiated explicitly or implicitly.
- **Explicitly** when a client calls `new PluginDatastore(PluginDatasourceConnectionPath path)`.
- **Implicitly** when a layer or standalone table is rehydrated (e.g. when a map opens and renders) and the layer or table data connection references a plugin data source.
- In either case, the `PluginDatasourceConnectionPath` or data connection contains the identifier of the relevant Plugin Datasource. The framework
looks up and invokes that plugin. If no matching plugin is found, explicit instantiation can throw an exception and a layer or table can have a broken data connection.

### Execution Contract

Assuming that a `PluginDatastore` has been opened explicitly with a connector or implicitly through layer or table hydration:

1. The framework instantiates the mapped `PluginDatasourceTemplate` and calls `Open(...)`.
2. A table/feature class is opened by name through `OpenTable(...)`.
3. A query (`Search(QueryFilter)` or `Search(SpatialQueryFilter)`) will return a plugin cursor.
4. The cursor iterates rows through `MoveNext()` + `GetCurrentRow()`.
5. Each row is surfaced as a `PluginRow` with values aligned to `GetFields()`.

Key Implications:
- `PluginDatasourceTemplate.GetTableNames()` is used to enumerate logical datasets.
- `PluginDatasourceTemplate.OpenTable(string name)` must return a concrete `PluginTableTemplate` for each supported dataset name.
- Field schema returned by `GetFields()` and values returned in `PluginRow.Values` must ALWAYS remain index-aligned and type-consistent.
- Failure to maintain deterministic row/value contracts can surface as runtime query errors far from the original plugin code.

## Catalog Integration

Custom Project Items:
- For deep integration of a plugin datasource, developers often pair plugin datasources with a Custom Project Item implemented within a **separate add-in**. 
- The custom project item provides an entry into the ArcGIS Pro catalog window for whenever the custom file type (upon which the plugin datasource is built) is discovered within a connected or browsed folder. 
- The Custom Project Item contains the plugin data source id and can invoke it (via the same explicit invocation pattern described previously) if or when required. 
- Custom Project Items are also integrated into ArcGIS Pro browse dialog experiences (e.g. to "open" a plugin datasource or to add it to a map)

## Authoritative References

- ArcGIS ProConcepts Plugin Datasources: https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-Plugin-Datasources
- ProGuide Plugin Datasources: https://github.com/esri/arcgis-pro-sdk/wiki/ProGuide-Plugin-Datasources
- Simple Point Plugin Datasource sample: https://github.com/Esri/arcgis-pro-sdk-community-samples/tree/master/Plugin/SimplePointPluginTest
- ArcGIS Pro SDK Community Samples: https://github.com/esri/arcgis-pro-sdk-community-samples 