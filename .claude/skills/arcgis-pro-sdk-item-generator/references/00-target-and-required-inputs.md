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

# Target and Required Inputs

## Objective

Identify one existing ArcGIS Pro SDK Add-in or Managed Configuration project and collect only the information needed to select and name the requested item pattern.

## Target Inspection

1. Find the target `.csproj`. If multiple eligible projects exist and the user did not identify one, ask which project to modify.
2. Inspect the `.csproj`, `Config.daml`, module class, default namespace, target framework, ArcGIS references, and nearby item naming and placement conventions.
3. Classify the project:
   - Add-in: ArcGIS Pro add-in project with `Config.daml` and no Configuration package declaration.
   - Managed Configuration: `Config.daml` contains the Configuration declaration and the project uses `<PackageType>Configuration</PackageType>`.
   - CoreHost or Plugin Datasource: unsupported target. Stop without editing and explain that ArcGIS Pro SDK item templates can only be added to Add-in or Managed Configuration projects.
4. Determine the target ArcGIS Pro version from the project and installed references. Do not mix SDK versions or target frameworks.

## Required Inputs

Resolve these values from the request and target project before planning:

- `TargetProject`: the eligible `.csproj`.
- `ProjectType`: `Addin` or `Configuration`.
- `ArcGISProVersion`: the installed or explicitly targeted ArcGIS Pro version.
- `ItemType`: the canonical item pattern from `04-item-patterns.md`.
- `ItemName`: the user-facing name and safe C#/DAML name.
- `Placement`: the ribbon, backstage, category, tray, or invocation location required to make the item reachable.
- `Behavior`: any requested workflow behavior beyond the minimum framework item pattern.

Use the project's existing safe-name prefix, namespace, indentation, and keytip conventions. Use the exact item and companion images specified in `04-item-patterns.md`, not the project's image conventions or add-in/package branding. Ordinary Buttons use `GenericButtonRed16` / `GenericButtonRed32`; never use `AddInDesktop16` or `AddInDesktop32` for any generated item. Ask one focused question at a time only when a required value cannot be inferred safely.

## Item-Type Disambiguation

- Resolve informal requests such as "selection tool" to the corresponding canonical pattern, normally `MapTool`.
- If "edit tool" or "sketch tool" could mean either a ribbon `MapTool` or a Create Features `ConstructionTool`, ask whether the tool must use the current feature template and appear in Create Features.
- If the request describes a new project rather than an item in an existing project, stop and route to `arcgis-pro-sdk-project-creator`.
- If the request updates or deletes an existing item, stop and explain that this skill creates new item content only.

## Handoff

Pass the resolved target, project type, ArcGIS Pro version, canonical item type, item name, placement, behavior, and observed project conventions to the planning step.