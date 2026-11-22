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
# ArcGIS Pro SDK CoreHost: Technical Implementation Details

## Scope

This document provides an overview of the ArcGIS Pro SDK CoreHost application model, with emphasis on:

- CoreHost implementation and runtime wiring considerations
- Execution contract including assembly resolution and deployment
- Additional considerations including:
  - Threading and async
	- Resource lifetime
	- Geodatabase access and editing
	- Geometry
- Common pitfalls

This is an implementation guide, not a high-level pattern comparison. This document complements the CoreHost description in [Patterns Summary](./patterns-summary.md) with additional details. For pattern comparison, refer to [Patterns Decision Guide](./patterns-decision-guide.md).

## Core Implementation Model

A CoreHost application is a standalone .NET console or desktop process that uses ArcGIS Core APIs outside the ArcGIS Pro UI process.

The minimum CoreHost implementation consists of these parts, in no particular order of importance:

- A 64-bit .NET executable project (`.csproj`) targeting a compatible .NET runtime and ArcGIS Pro SDK assemblies.
- A startup entry point (`Main`) marked with `[STAThread]` that initializes ArcGIS runtime support through `Host.Initialize()` before any ArcGIS.Core object is created.
- License initialization logic that checks out an ArcGIS Pro compatible license level and optional extensions needed by the workflow.
- One or more workflow services or classes that execute supported geodatabase and geometry tasks.
- Deterministic cleanup and disposal (`IDisposable` / `using`) for ArcGIS and data objects to avoid file locks and native resource leaks.
- Configuration for required binaries and runtime dependencies (for local development, CI, and deployment hosts).

The CoreHost model is the primary ArcGIS Pro SDK pattern for running ArcGIS Pro SDK ArcGIS.Core geodatabase and geometry api functionality in automation, batch, ETL-style jobs, and service-like workloads where ArcGIS Pro UI extensibility is not required.

Fit boundary:
- CoreHost use is LIMITED to ArcGIS Pro SDK ArcGIS.Core geodatabase and geometry apis outside ArcGIS Pro as standalone exe applications. CoreHost does NOT extend the ArcGIS Pro application. It does NOT use DAML, modules, ribbon controls, or pane contracts. It does NOT share any architectural aspects with add-ins, configurations, or plugin datasources.

## Host Wiring Requirements

In a CoreHost application, startup bootstrap and capability checks provide the runtime contract.

- The application process MUST initialize host runtime and licensing before workflow execution via `Host.Initialize()`.
- The process must target x64 and its entry thread must use the single-threaded apartment (STA) model. `Host.Initialize()` validates both conditions.
- Dependent assembly resolving is the responsibility of the CoreHost application.
- The correct target .NET framework should be installed and present on the host machine.

### Key implications

- CoreHost applications are typically implemented as Console programs
- CoreHost applications do not have forward compatibility guarantees in the same way that add-ins, configurations, and plugin datasources have.
- Limited forward compatibility is possible through use of the `dotnet.exe` command to invoke the CoreHost application.
- Any UI requirements for a CoreHost application must be satisfied via use of WPF directly
- CoreHost Console applications should centralize initialization and licensing logic in any method or methods OTHER THAN Program.cs `static void Main(string[] args)`

### Additional Implementation Notes

- Assembly resolving must be established BEFORE invoking ArcGIS runtime initialization via `Host.Initialize()`.
- ArcGIS runtime initialization via `Host.Initialize()` must occur before any invocation of ArcGIS Pro SDK geodatabase or geometry apis.
- `Host.Initialize()` triggers underlying licensing and capability checks.
- Forward-compatible CoreHost implementations should always apply the Assembly Resolving algorithm described in ProConcepts CoreHost.
- Initialization failure should stop workflow startup and return a non-zero exit code.

## Execution Contract

Typical runtime sequence:

1. Parse arguments and validate inputs.
2. Trigger any required assembly resolution.
3. Initialize the ArcGIS host runtime.
4. Verify that licensing initialization succeeded.
5. Execute one or more workflows.
6. Flush outputs and logs and return a deterministic exit code.

Key implications:
- Any ArcGIS object creation prior to host initialization is invalid and WILL fail unpredictably.

### Assembly Resolving (Required for Forward Compatibility)

CoreHost technical implementations should always apply the ArcGIS Pro Assembly Resolving algorithm so ArcGIS.Core and ArcGIS.CoreHost are resolved at runtime from the installed ArcGIS Pro location. This is documented in `https://github.com/esri/arcgis-pro-sdk/wiki/proconcepts-CoreHost#assembly-resolving`

Required algorithm:

1. Set Copy Local to No for ArcGIS.Core.dll and ArcGIS.CoreHost.dll references.
2. Set Specific Version to blank or No for ArcGIS.Core.dll and ArcGIS.CoreHost.dll references.
3. Keep Program.cs `static void Main(string[] args)` free of Pro-dependent object creation and move Pro work into a separate method invoked by Main.
4. Resolve ArcGIS Pro install path first (for example from HKLM/HKCU ESRI ArcGISPro InstallDir).
5. Register an AppDomain assembly resolver before calling Host.Initialize:
	- AppDomain.CurrentDomain.AssemblyResolve += ResolveProAssemblyPath;
6. In ResolveProAssemblyPath, construct path as InstallDir\\bin\\<AssemblyName>.dll and load only if the file exists.
7. Invoke `Host.Initialize()` only AFTER the resolver is wired.

Implementation notes:

- Register the resolver as early as possible in Main, immediately after install path validation.
- Restrict resolver behavior to ArcGIS Pro assemblies to avoid unintended assembly binding side effects.
- Keep resolution deterministic and log the resolved Pro path/version to aid unattended diagnostics.

Design implication:
- Separate assembly resolution logic from license validation logic
- Separate license validation logic from business logic
- Each workflow can declare capability requirements cleanly.

Reference implementation:

- ProConcepts CoreHost Assembly Resolving section: https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-CoreHost#assembly-resolving
- Community sample Program.cs: https://github.com/Esri/arcgis-pro-sdk-community-samples/blob/master/CoreHost/CoreHostResolveAssembly/Program.cs

Illustrative startup sequence:

1. Read InstallDir from registry.
2. Register AppDomain.CurrentDomain.AssemblyResolve handler.
3. Call workflow bootstrap method.
4. Inside bootstrap, call Host.Initialize.
5. Open geodatabase/work datasets and execute workflow.

### Build, Packaging, and Deployment

CoreHost applications are deployed as standard .NET executables with ArcGIS Pro SDK runtime prerequisites.

Deployment expectations:
- Target hosts must have compatible target .NET runtime components available.
- Target hosts must have compatible ArcGIS runtime components and licensing available.
- Build pipelines should validate assembly resolution and runtime probing on clean agents.
- Deployment packaging should include application configuration, dependency manifests, and environment-specific settings.

## Additional Considerations

### Threading and Async Model

CoreHost does NOT extend ArcGIS Pro UI or ArcGIS Pro application. As a standalone exe, it does NOT use the ArcGIS Pro Main CIM Thread contract; however, ArcGIS Core objects still require careful threading and lifetime discipline.

Core practices:
- Prefer async and await for I/O-bound orchestration and long-running operations.
- Do not assume `Task.Run` or thread-pool work executes in an STA apartment. Do not share ArcGIS geodatabase objects across threads.
- Keep ArcGIS.Core object use on a compatible STA thread. When asynchronous ArcGIS work is required, use the CoreHost `QueuedWorker` pattern documented
for the targeted SDK version rather than dispatching ArcGIS objects to arbitrary thread-pool threads.
- Avoid blocking patterns such as `.Result` and `Task.Wait()`.
- Implement lifecycle management for all Geodatabase objects using `Dispose()`

### Resource Lifetime and Disposal

CoreHost workflows frequently hold native handles and file/database locks through ArcGIS object wrappers.

Required patterns:
- Dispose `Geodatabase`, `FeatureClass`, `Table`, cursors, rows, and related objects as soon as possible.
- Keep `using` scopes tight around geodatabase operations.
- Use `ArcGIS.Core.Hosting.Threading.Tasks.QueuedWorker()` for asynchronous operations
- Avoid long-lived static references to dataset objects.
- Ensure disposal occurs on both success and failure paths.

### Geodatabase Access and Editing Workflows

CoreHost applications can use supported ArcGIS.Core.Data geodatabase APIs, including stand-alone editing through `Geodatabase.ApplyEdits`. They do
not have access to the ArcGIS.Desktop editing framework. For general information about geodatabase capabilities, consult:

- https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Geodatabase for general information regarding usage and structure of the ArcGIS Pro SDK geodatabase APIs
- https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-DDL for information specific to `SchemaBuilder`, a component of the geodatabase API for schema creation and modification
- https://github.com/esri/arcgis-pro-sdk/wiki/ProSnippets-Geodatabase for code examples

#### Special Considerations for Editing

Within the add-in and configuration patterns, the ArcGIS Pro editor extension provides the transactional environment and coarse-grained api for geodatabase editing. The editor AND ArcGIS Pro application framework also provide an Undo and Redo capability for transaction management in addition. CoreHost does NOT have access to these capabilities. Specifically:

- CoreHost applications do NOT have access to the `ArcGIS.Desktop.Editing.EditOperation` class which provides the preferred editing api and transaction management for add-ins and configurations.
- CoreHost applications, instead, should use `ArcGIS.Core.Data.Geodatabase.ApplyEdits(action)`. ApplyEdits accepts a `System.Action` delegate which should contain the geodatabase code for performing the transaction.
- The scope of the transaction is equal to the scope of the ApplyEdits call. When the action is complete, the transaction scope is likewise complete.
- CoreHost applications commonly use `ArcGIS.Core.Data.Table.CreateRow() or `ArcGIS.Core.Data.FeatureClass.CreateRow()` to apply any inserts prior to transaction completion.
- CoreHost applications commonly use `ArcGIS.Core.Data.Row.Store()` to apply any updates prior to transaction completion.
- CoreHost applications commonly use `ArcGIS.Core.Data.Table.DeleteRows()` to apply any deletes prior to transaction completion.
- There is no built-in Undo or Redo capability for CoreHost in the same way that there is for Add-ins and Configurations.
- `ArcGIS.Desktop.Editing.Events.RowCreatedEvent`, `ArcGIS.Desktop.Editing.Events.RowChangedEvent`, and `ArcGIS.Desktop.Editing.Events.RowDeletedEvent` for Row inserts, updates, and deletes are NOT available for CoreHost.

### Geometry Workflows

CoreHost applications can use the ArcGIS.Core.Geometry API surface supported by the targeted ArcGIS Pro release. Verify individual APIs against the
version-matched API reference. For additional information, consult:

- https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Geometry for general information regarding usage and structure of the ArcGIS Pro SDK geometry APIs
- https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Multipatches for information specific to multipatch geometry type.
- https://github.com/esri/arcgis-pro-sdk/wiki/ProSnippets-GeometryEngine for code examples using `GeometryEngine` coarse-grained api
- https://github.com/esri/arcgis-pro-sdk/wiki/ProSnippets-Geometry for all other geometry api aspects

### Common Pitfalls and Mitigations

Frequent issues:

- Using ArcGIS objects before `Host.Initialize(...)`.
- Calling `Host.Initialize(...)` before registering the ArcGIS Pro AssemblyResolve handler.
- Holding row/cursor/dataset objects longer than needed.
- Treating CoreHost as a generic multithreaded ORM-style data layer without ArcGIS object lifetime discipline.
- Overly broad transactions that increase lock duration and failure blast radius.
- Weak diagnostics that make unattended failures hard to triage.

Mitigations:

- Centralize startup bootstrap and license checks.
- Enforce the ProConcepts Assembly Resolving algorithm in all CoreHost entry points.
- Enforce tight disposal scopes with code reviews and static analysis where possible.
- Partition work into restartable units with explicit checkpoints.
- Standardize logging schema and exit-code taxonomy.

## Authoritative References

- ArcGIS ProConcepts CoreHost: https://github.com/esri/arcgis-pro-sdk/wiki/proconcepts-CoreHost
- Code Snippets: https://github.com/esri/arcgis-pro-sdk/wiki/ProSnippets-CoreHost
- RunCoreHost code sample: https://github.com/esri/arcgis-pro-sdk-community-samples/tree/master/CoreHost/RunCoreHostApp
- CoreHost File GDB reader sample: https://github.com/Esri/arcgis-pro-sdk-community-samples/tree/master/CoreHost/CoreHostGDB