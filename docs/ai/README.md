# AI assistant guidance for the ArcGIS Pro SDK

The files in this folder and the `arcgis-pro-sdk-*` skills under
`.claude/skills/` are adapted from Esri's
[arcgis-pro-sdk-copilot](https://github.com/Esri/arcgis-pro-sdk-copilot)
repository (Apache-2.0, © Esri; vendored at commit `2ce313e`, 2026-11).
Each vendored file keeps its original license header.

## What was adapted, for which tools

| Piece | Location here | Used by |
|---|---|---|
| Best-practices instructions (C#, WPF/XAML, DAML, threading) | `docs/ai/arcgis-pro-sdk-best-practices.md` | All agents, routed from `AGENTS.md` / `CLAUDE.md` |
| Example-resources policy (how to treat samples/snippets) | `docs/ai/arcgis-pro-sdk-example-resources.md` | All agents |
| Pattern-help skill (+ references) | `.claude/skills/arcgis-pro-sdk-pattern-help/` | Claude Code natively; others read SKILL.md as docs |
| Referral skill (authoritative doc links) | `.claude/skills/arcgis-pro-sdk-referral/` | Same |
| Item-generator skill (ribbon items, dockpanes, tools) | `.claude/skills/arcgis-pro-sdk-item-generator/` | Same (VS Code delegation step does not apply) |
| Fix-Pro-references skill + PowerShell scripts | `.claude/skills/arcgis-pro-sdk-fix-pro-references/` | Windows machines after an ArcGIS Pro upgrade |

Skipped from upstream: the Copilot plugin packaging (`.github/plugin/`), the
Copilot `.agent.md` agents (their routing is distilled into `AGENTS.md`), and
the project-creator skill with its templates (this repository already is a
project).

## Per-tool entry points

- **Claude Code**: `CLAUDE.md` + auto-loaded `.claude/skills/`.
- **Cursor / Codex (ChatGPT in repos)**: `AGENTS.md` (read natively).
- **GitHub Copilot**: use Esri's upstream repository directly, or point
  `.github/copilot-instructions.md` at `AGENTS.md`.
- **ChatGPT (web, no repo access)**: paste `AGENTS.md` and
  `docs/ai/arcgis-pro-sdk-best-practices.md` into the project's custom
  instructions/files.
