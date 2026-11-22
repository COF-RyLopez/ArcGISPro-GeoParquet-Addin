# SQL attribute filter

Optional SQL `WHERE` fragment the user types in the wizard's Select Area section; it narrows what gets ingested (and therefore both the display layers and the `_source` archive) and also narrows the preview sample. `GeoParquetSql.ValidateAttributeFilter` is a trust boundary: user-typed SQL is embedded in queries run by the real engine.

## Sub-features

- Validation with a user-readable message (semicolons, comments, `$`, deny-listed words such as `select`/`read_parquet` are refused; quoted strings are not scanned).
- `WHERE ` prefix tolerated and stripped (`NormalizeAttributeFilter`).
- Filter applied to ingest (`S3Ingester.BuildLoadQuery(..., attributeFilter:)` — all three query shapes) with `GeoParquetSql.BuildAttributeFilterPredicate` throwing as the backstop.
- Filter applied to the preview sample (`Views/WizardDockpaneViewModel.Preview.cs:145` validates, then passes the filter to the sampler).

## How to get to it (user POV)

In ArcGIS Pro: GeoParquet wizard → Select Area section → "Attribute filter (optional)" text box. Type e.g. `class = 'primary'`; invalid input shows the validation message and blocks the load.

## Driving it with the dotnet scratch-test harness

In a `Scratch_AttributeFilterTests.cs` (shape of `DuckDBExecutionTests.cs`):

1. Fixture: `COPY (SELECT ... ST_Point(...) geometry, 'primary'/'secondary' class ...) TO '<tmp>/roads.parquet' (FORMAT 'PARQUET')` with a known class split.
2. Accept side: assert `GeoParquetSql.ValidateAttributeFilter("class = 'primary'")` returns null, then execute `S3Ingester.BuildLoadQuery(file, "segment", columns, extent, preserveSource: true, attributeFilter: "class = 'primary'")` and assert `current_table`'s row count matches only the primary rows.
3. Reject side: assert `ValidateAttributeFilter` returns a message for `class = 'x'; DROP TABLE t` and for `id IN (SELECT ...)`, and that `BuildLoadQuery` with the same string throws `ArgumentException` (the backstop).
4. No-filter regression: same load with `attributeFilter: null` returns all rows.

## Gotchas

- The validator refuses ALL subqueries (deny-listed `select`), by design — don't "fix" a scratch drive by allowing one.
- A wrong column name passes validation and fails at bind time inside DuckDB; that error path is the engine's message, not the validator's — assert it via the thrown `DuckDBException`, not `ValidateAttributeFilter`.
- Windows-only: the text box binding, the message display in the pane, and the filter reaching a real Overture S3 load.
- Exhaustive adversarial coverage of the validator is the formal check — see the `formal-verify-attribute-filter` skill; this drive proves wiring, not the full input space.
