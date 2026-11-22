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

# Item Plan and Confirmation

## Objective

Translate the resolved item request into a complete, project-specific creation plan and obtain confirmation before editing implementation files.

## Planning Rules

1. Load the matching item pattern from `04-item-patterns.md`.
2. Identify every new C#, XAML, code-behind, resource, image, GUID, and DAML record required by that pattern.
3. Identify every existing file and declaration that must receive new wiring, including module sections, root sections, ribbon groups, categories, placement references, companion launch controls, and hard-coded lookup IDs.
4. Preserve the existing Add-in or Managed Configuration scaffold. For a Configuration, preserve its one `<Configuration>` block, `ConfigurationManager`, and `<PackageType>Configuration</PackageType>`.
5. Generate the minimum complete pattern. Include optional behavior only when requested.
6. Call out any unresolved built-in `esri_` ID, SDK-version dependency, or runtime assumption that must be verified.

## Confirmation Gate

Present a concise plan containing:

- Target project and detected project type.
- Canonical item type and item name.
- Files to create.
- Existing files to modify.
- DAML declaration, placement, category, and invocation wiring.
- Requested behavior and relevant ArcGIS Pro threading context.
- Validation commands that will be run.

Then ask for explicit confirmation to implement the plan. Do not create or edit C#, XAML, DAML, project, image, or resource files before the user confirms. If the user changes the requirements, revise the plan and obtain confirmation for the revised plan. If the user declines, stop in explanation-only mode.

## Handoff

After explicit confirmation, pass the approved plan and all resolved inputs to the generation and verification step.