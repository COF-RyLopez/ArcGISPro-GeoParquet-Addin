---
name: formal-verify-attribute-filter
description: "Adversarially verify the SQL attribute-filter trust boundary (GeoParquetSql.ValidateAttributeFilter) against the real DuckDB parser over tens of thousands of generated inputs. Run after any change to the filter validator, its deny list, NormalizeAttributeFilter, BuildAttributeFilterPredicate, or how S3Ingester embeds the filter in queries."
---

# Formal check: the attribute filter cannot smuggle SQL

**The property:** every filter string the validator accepts, once embedded in a WHERE clause, is seen by DuckDB's own parser as one SELECT statement containing no subquery and no call to a file-, environment-, or settings-reading function. **The harm if it breaks:** a user-typed filter in the wizard becomes SQL injection into the engine, and DuckDB's functions (`read_parquet`, `getenv`, `read_text`, …) can read local files and environment variables.

**Why this tool:** no practical formal verifier runs on C# code, so instead of a hand model of the validator (which would just repeat its assumptions), the check runs the **real** `ValidateAttributeFilter`/`BuildAttributeFilterPredicate` against ~20,000 generated adversarial inputs per run and uses the **real DuckDB parser** (`json_serialize_sql` AST) as the oracle. The bug class this targets — the C# quote-stripper disagreeing with DuckDB's lexer about where a string ends — cannot hide from the engine's own parse.

## Does my change touch the verified code?

Covered (coverage record): `Services/GeoParquetSql.cs` — `NormalizeAttributeFilter`, `ValidateAttributeFilter`, `AttributeFilterDeniedWords`, `BuildAttributeFilterPredicate` — and the embedding shape `WHERE (<filter>)` used by `Services/S3Ingester.cs` `BuildLoadQuery`. **Sync commit: `f117057`** (branch `feature/sql-attribute-filter`). If any of those changed since that commit, run the check; move this sync commit forward only after confirming the checker's fragment grammar and `DangerousFunctions` set still describe the code (in particular: a word added to `AttributeFilterDeniedWords` gets added to `DangerousFunctions` in the checker, and a new embedding site for the filter gets its own embedding in the test).

## Run it

```bash
./.claude/skills/formal-verify-attribute-filter/run.sh
```

It copies `Scratch_AttributeFilterPropertyCheck.cs` into `DuckDBGeoparquet.Tests/`, runs only that test against the real engine, and removes the copy on exit. ~1s for the default 20,000 inputs; `FILTER_FUZZ_ITERATIONS=200000` for a deeper pass, `FILTER_FUZZ_SEED=<n>` to vary the search. The default seed is fixed, so a pass is reproducible and a failure replays exactly.

## Reading a failure

A failure prints the accepted filter string, the embedded SQL, and what the parser found, e.g.:

```
Accepted filter violates the property: AST calls dangerous function getenv.
Filter: getenv('HOME')
Embedded: SELECT 1 WHERE (getenv('HOME'))
```

- **"violates the property" / "smuggled extra statements"** = a real bug in the validator: that exact string passes `ValidateAttributeFilter` and reaches the engine. Report it with the printed counterexample; do not weaken the checker to get green, and do not change product code unless the user asks.
- **"Validator accepted but backstop threw"** = `ValidateAttributeFilter` and `BuildAttributeFilterPredicate` disagree; also a real bug (users would see a crash instead of a validation message).
- **"Generator too hostile" / "Too few accepted filters parsed"** = a modeling error in the checker itself (the fragment grammar no longer produces realistic accepted filters, or the embedding broke). Fix the checker, not the product.

An intended change (e.g. deliberately allowing a new function) that trips the check: update `DangerousFunctions`/the grammar to match the new contract, re-run to green, and move the sync commit forward.

## Scope — what a pass does and does not say

A pass says: over this run's generated inputs (seeded grammar of realistic filters spliced with quote tricks, lexer edge cases, denied words in odd casing, paren-escape and comment smuggling, and every deny-listed function), no accepted string parsed into anything but one safe SELECT. It is a bounded search, not a proof over all strings: inputs outside the grammar's fragment vocabulary are not tried (grow the `Atoms`/`Nasty` arrays as new tricks come up). Accepted-but-unparseable strings are treated as safe because the query merely fails at run time. Binder-level behavior (wrong column names) and the Windows UI path are out of scope — the `verify-geoparquet-addin` skill covers wiring.

Proven able to fail: removing `getenv` from the deny list was caught within 134 ms with the counterexample above (2026-11-21).
