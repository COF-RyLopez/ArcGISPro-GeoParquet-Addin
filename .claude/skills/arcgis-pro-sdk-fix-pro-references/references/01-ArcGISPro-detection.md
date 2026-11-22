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
# ArcGIS Pro Detection

## Objective

Search the registry to determine if ArcGIS Pro is installed. If it is installed, get the value of the Install location and the version.

## Operating Rules
- Resolve and run [`01-detect-arcgis-pro.ps1`](../scripts/01-detect-arcgis-pro.ps1) relative to this reference document, then capture its output object.
- Set ProInstallDir to output.ProInstallDir.
- Set ArcGISProVersion to output.ArcGISProVersion.
- Confirm the returned `BinPath` contains `ArcGISPro.exe`, `ArcGISPro.runtimeconfig.json`, and `ArcGIS.Desktop.Framework.xsd`.
- If no object is returned, treat ArcGIS Pro as not detected and stop with a warning. Do not infer an installation path.

## Coverage Checklist
Capture:
- ProInstallDir
- ArcGISProVersion

## Handoff
When the detection is complete, store the install folder and the version for the next step in the workflow.

