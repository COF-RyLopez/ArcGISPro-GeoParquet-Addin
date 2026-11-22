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

# Generation and Verification

## Objective

Generate the approved item pattern without disturbing the existing project scaffold, then validate its complete declaration, placement, code, resource, and invocation graph.

## Generation Rules

1. Follow the approved plan and the exact matching pattern in `04-item-patterns.md`.
2. Apply `../../../instructions/arcgis-pro-sdk-example-resources.instructions.md` to all generated C#, XAML, and DAML.
3. Apply `../../../instructions/arcgis-pro-sdk-best-practices.instructions.md` to workflow implementation, threading, async flow, error handling, and managed C# quality.
4. Reuse project conventions and preserve unrelated project and DAML content.
5. Add declarations under the correct existing parent in schema-valid order. Reuse declarations by `refID`; do not duplicate them for additional placements.
6. Keep all IDs, class names, namespaces, lookup strings, XAML types, bindings, and companion controls synchronized.
7. Generate a fresh GUID where the canonical pattern requires one.
8. Include the complete role-specific using block and required human-readable comments from the canonical pattern.
9. Use the exact item and companion image keys specified in `04-item-patterns.md`, not inherited project or add-in/package images. Ordinary Buttons use `GenericButtonRed16` / `GenericButtonRed32`. Never use `AddInDesktop16` or `AddInDesktop32` for any item; preserve role-specific images and omissions in other patterns.

## Verification

Run the full Validation Mode from `04-item-patterns.md`. At minimum:

1. Parse `Config.daml` as XML and validate against the version-matched DAML schema when available.
2. Verify unique local IDs and resolve all new local references, class names, lookup strings, categories, conditions, placements, views, bindings, resources, and invocation paths.
3. Confirm generated classes use the required ArcGIS Pro base classes and constructor shapes.
4. Confirm WPF views, code-behind classes, runtime view models, project `Page` items, and packaged resources agree.
  Check every new item and companion image against its matching pattern. Treat `AddInDesktop16`, `AddInDesktop32`, or any incorrect role-specific image as a generation defect and correct it before reporting completion. Do not modify unrelated existing item images or package branding.
5. Confirm a Managed Configuration's Configuration declaration, manager, and package type remain intact.
6. Query WPF page inclusion when the item includes XAML:

```powershell
dotnet msbuild .\TargetProject.csproj -getProperty:EnableDefaultPageItems
dotnet msbuild .\TargetProject.csproj -getItem:Page
```

7. Build the target project:

```powershell
dotnet build .\TargetProject.csproj -c Debug
```

8. When ArcGIS Pro can be launched, exercise each generated placement, activation path, menu, binding, and persistence workflow. Do not treat a successful build as proof that DAML references resolve at runtime.

## Output

Report the target project, project type, generated item, files created and modified, DAML and invocation wiring, checks run, build result, and any remaining runtime verification or version-specific assumptions. If validation fails, fix defects introduced by generation and rerun the narrowest failing check before reporting completion.