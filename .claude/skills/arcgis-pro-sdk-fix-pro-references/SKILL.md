---
name: arcgis-pro-sdk-fix-pro-references
description: Fix broken ArcGIS Pro SDK DLL HintPath values, Desktop targets imports, DAML or Config.xml schema paths, and ArcGISPro.exe launch profiles in an existing Add-in, Managed Configuration, CoreHost, or Plugin Datasource project. Use for invalid ArcGIS Pro references, moved installations, and optional TargetFramework alignment. Triggers "Fix References", "Fix Assembly Paths","Fix Paths","Fix All","Fix project","Fix ArcGIS Pro references","Fix ArcGIS Pro SDK references", "Repair ArcGIS Pro references", "Repair ArcGIS Pro SDK references", "Update ArcGIS Pro references", "Update ArcGIS Pro SDK references"
argument-hint: Open the existing project in VS Code and specify the .csproj or .vbproj if the workspace contains more than one.
license: Apache-2.0
metadata:
  author: Esri, Inc.
  version: "1.0"
---

<!-- Copyright 2026 Esri
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    https://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. -->

# ArcGIS Pro SDK Fix Pro References

## Purpose

Use this skill only for an existing ArcGIS Pro SDK Add-in, Managed Configuration, CoreHost, or Plugin Datasource project.

## Workflow

1. Run [`00-find-arcgis-pro-projects.ps1`](./scripts/00-find-arcgis-pro-projects.ps1) from the workspace root. The scanner must not descend into any `.github` directory. Never inspect, select, update, verify, or build a project or file under `.github`; reject an explicitly selected project or workspace root under `.github`. Only consider discovered ArcGIS Pro SDK projects, and never include ordinary `.csproj` or `.vbproj` files. The user can select all discovered projects or specific projects. Stop if no ArcGIS projects are found.
2. Detect and validate ArcGIS Pro using `./references/01-ArcGISPro-detection.md`.
3. Set `UpdateTargetFramework` unless the user explicitly refused framework alignment. The runtime configuration provides the target runtime family, not an SDK version. The repair script derives `<runtime-tfm>-windows` and confirms a compatible SDK is installed before editing.
4. Before changing files, ask exactly:

  `Ready to update ArcGIS Pro references in the following project(s): <ProjectPaths>. ArcGIS Pro installation: <ProInstallDir>. TargetFramework update: <Yes/No>. The workflow will create rollback copies, verify references, and build each project. Proceed?`

  Continue only after the user explicitly confirms this prompt.
5. For each approved project, run [`02-fix-pro-references.ps1`](./scripts/02-fix-pro-references.ps1) with `ProjectPath`, `ProInstallDir`, `ArcGISProVersion`, and the current workspace root as `WorkspacePath`. Pass `-UpdateTargetFramework` unless the user explicitly refused framework alignment in step 3.
6. Inspect each returned report. If repair fails, report that modified files were restored, include the error, and stop without processing another project.
7. Verify the repaired project using `./references/02-verification.md`. Stop on verification failure.
8. Run [`04-build-project.ps1`](./scripts/04-build-project.ps1) for the verified project. If the build fails, report the build error and backup directory, then stop without processing another project. Do not restore reference changes automatically after a build failure.
9. Repeat steps 5-8 for the next approved project.
10. Report the approved projects, ArcGIS Pro installation, changed files, backup directories, framework results when requested, and build results.

## Safety Rules

- Update any approved project and its adjacent `Config.daml` or `Config.xml`, project `.vscode/launch.json`, project-root `launch.json`, `Properties/launchSettings.json`, and workspace `.vscode/launch.json` files.
- Preserve non-ArcGIS references and non-ArcGIS launch profiles.
- Treat missing required manifests, ambiguous DLL matches, malformed XML or JSONC, multi-target frameworks, missing SDKs, and failed builds as blocking errors.
- Never delete rollback copies automatically. Return their location to the user when changes were made.