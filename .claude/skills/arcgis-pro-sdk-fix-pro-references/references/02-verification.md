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
# Verification

## Objective
Verify that the selected ArcGIS Pro SDK project uses the detected installation for every applicable reference and configuration.

## Operating Rules
- Use these values captured from previous steps:
	- `ProjectPath`
	- `ProInstallDir`
	- `WorkspacePath`
	- `ExpectedTargetFramework` when framework alignment was requested
- Run [`03-verify-pro-references.ps1`](../scripts/03-verify-pro-references.ps1).
- Do not substitute workspace-wide or inferred project variables.

## Token and Reference Validation
- Confirm that the `.csproj` or the `.vbproj` file ArcGIS `HintPath` values exist under `ProInstallDir\bin`.
- Treat a project with an ArcGIS reference and no Desktop targets import or manifest as a CoreHost project; do not require a manifest for this shape.
- Require `Config.daml` or `Config.xml` when the project imports `Esri.ProApp.SDK.Desktop.targets`.
- Require the Desktop targets import when `Config.daml` or `Config.xml` exists, and confirm that it resolves under `ProInstallDir\bin`.
- Reject a project with no ArcGIS references, Desktop targets import, or ArcGIS manifest.
- Confirm ArcGIS Pro launch profiles in workspace `.vscode/launch.json`, project `.vscode/launch.json`, project-root `launch.json`, and/or `Properties/launchSettings.json` use `ArcGISPro.exe` and its `bin` working directory from `ProInstallDir`.
- When requested, confirm the exact expected `TargetFramework`.

## Suggested Validation Commands

Run from the workspace root unless stated otherwise.

```powershell
& <SkillRoot>\scripts\03-verify-pro-references.ps1 `
	-ProjectPath <ProjectPath> `
	-ProInstallDir <ProInstallDir> `
	-WorkspacePath <WorkspacePath> `
	-ExpectedTargetFramework <ExpectedTargetFramework> `
	-ThrowOnFailure
```

## Coverage Checklist

- ArcGIS reference `HintPath` values resolve from `ProInstallDir`.
- Desktop projects have both a manifest and Desktop targets import; CoreHost projects require neither.
- Manifest schema and launch profile paths resolve from `ProInstallDir`.
- Requested `TargetFramework` matches and a compatible SDK is installed.

## Handoff

When verification is complete, report:

- Checked files
- Any missing files
- Any path/reference anomalies detected

If verification fails, stop and provide the minimum corrective action needed before retrying verification.
