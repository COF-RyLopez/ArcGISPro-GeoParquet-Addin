---
description: "Use when generating, adapting, retrieving, or validating ArcGIS Pro SDK managed C#, WPF/XAML, or DAML code examples, snippets, starter templates, and implementation samples."
applyTo: "**/*.cs,**/*.xaml,**/*.daml"
license: Apache-2.0
metadata:
  author: Esri, Inc.
  version: "1.0.0"
---

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

## ArcGIS Pro SDK Code Examples Guidance (Managed C#)

Use these instructions for all ArcGIS Pro SDK for .NET examples, whether generated directly or adapted or retrieved from samples, snippets, API reference documentation, or other online resources.

## Authority and Precedence

- Use the version-matched ArcGIS Pro SDK API reference as the primary authority for API availability, signatures, thread affinity, and supported behavior.
- Use `arcgis-pro-sdk-best-practices.instructions.md` as the authority for generated implementation patterns and repository-specific code quality. Its guidance takes precedence over source snippets and samples.
- Treat official snippets and community samples as non-authoritative starting points that must be verified before use.
- If the API reference and repository best practices appear to conflict, preserve the documented API contract and apply the best-practices guidance within that contract. State any unresolved conflict or assumption.

## Tech Stack Constraints
- Determine the installed or explicitly requested ArcGIS Pro version before selecting APIs, target frameworks, project settings, or examples.
- Use the matching SDK and .NET target:
    - ArcGIS Pro 3.3 through 3.6 require .NET 8.
    - ArcGIS Pro 3.7 and later require .NET 10 unless later version-specific documentation states otherwise.
- Use version-specific ArcGIS Pro SDK documentation when targeting an older release. Do not use `latest` documentation to assert availability in an older version without verification.
- For ArcGIS Pro 3.7 development in Visual Studio, use Visual Studio 2026 version 18.4.1 or later with the .NET 10 SDK.
- When using VS Code, treat it as the editor rather than the compiler/runtime requirement. Require a compatible .NET SDK, ArcGIS Pro SDK references, C# language tooling, and project build configuration. VS Code 1.129 or later is the workspace-supported editor version.

## Authoritative API References

- https://doc.esri.com/en/arcgis-pro/latest/sdk/api-reference/
- https://doc.esri.com/en/arcgis-pro/latest/sdk/
- https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Framework

## Example Sources

- https://github.com/Esri/arcgis-pro-sdk-snippets
- https://github.com/Esri/arcgis-pro-sdk-community-samples

## Source Handling Rules

- Prefer code examples and patterns consistent with the repository resources and snippet structure.
- When uncertain, state assumptions and list the ArcGIS Pro API reference or official snippets used.
- Treat source snippets and samples as reference material, not as automatically correct or authoritative implementations.
- Before presenting or adapting a source example, compare it against the applicable repository instructions and verify its ArcGIS Pro and .NET version compatibility.
- Preserve the source behavior only after checking API signatures, thread affinity, async flow, application-context capture, null handling, and required DAML or add-in wiring.
- If a source example conflicts with repository guidance or appears incorrect, correct it before presenting it and briefly identify the assumption or deviation.
- Do not present unverified source code as ready to paste. If the API shape or runtime behavior cannot be verified, label the code as illustrative and state what must be confirmed.

## Output Quality Bar

- For conceptual, architectural, or pattern-selection questions, do not include source code unless the user explicitly asks for a code snippet, starter template, or implementation sample.
- When code is explicitly requested, provide code that is ready to paste and adapt when it can be validated.
- "Paste-ready" means the example compiles in the stated project and version context. When a compatible project context exists, run the narrowest available build, compile, or type check before describing an example as paste-ready.
- If executable validation is unavailable, fails for an unrelated reason, or the snippet depends on project-specific placeholders, do not call it paste-ready. Label it as uncompiled or illustrative and state the required adaptation or verification steps.
- When code is explicitly requested, include required using directives and context notes when a snippet depends on MCT, DAML, or add-in wiring.
- When external references are included, list each URL as a bare URL on its own bullet; do not use titled Markdown links or title-only references.
- For every code example, perform a final conformance check against the applicable best-practices instructions before responding; source provenance does not override local guidance.
- Keep explanations concise and implementation-focused.

## Response mode

- Default to explanation-only for pattern selection, workflow fit, architecture choice, and implementation approach questions.
- Acceptable explicit requests for code include phrases such as "show me code", "give me a snippet", "starter template", "how do I implement this", or equivalent direct intent.
- Explicit code intent overrides the explanation-only default. When intent remains ambiguous, ask whether the user wants explanation only or an implementation example.

### Mandatory Implementation Confirmation Gate
- Before creating or editing workflow implementation files handled by the caller (for example: map tools, commands, dockpanes, menu items, ribbon controls, or DAML wiring), you MUST ask for explicit user confirmation first.
- Do not create or edit implementation files until the user explicitly confirms.
- New project scaffolding is owned by `arcgis-pro-sdk-project-creator`; defer its interview and confirmation behavior to that workflow rather than applying this gate independently.
- If the user declines, remain in explanation-only mode and provide options without code changes.

