# Claude Code guidance

Read and follow `AGENTS.md` — it is the canonical agent guidance for this
repository (build/test splits, the draft-PR + Windows-agent workflow, ArcGIS
Pro SDK guidance routing, and project invariants).

Claude-specific notes:

- The skills in `.claude/skills/` load automatically; prefer them over general
  knowledge: `arcgis-pro-sdk-pattern-help` for extensibility-pattern questions,
  `arcgis-pro-sdk-item-generator` when adding ribbon items/dockpanes/tools,
  `verify-geoparquet-addin` to prove data-core changes work, and
  `formal-verify-attribute-filter` after touching the SQL filter validator.
- Before writing Pro SDK C#/XAML/DAML, read
  `docs/ai/arcgis-pro-sdk-best-practices.md`.
- Never merge, approve, close, or mark a PR ready; the repository owner
  decides merges after the Windows test run.
