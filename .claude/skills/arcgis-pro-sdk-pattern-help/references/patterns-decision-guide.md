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
# ArcGIS Pro SDK Extensibility Pattern Decision Guide

## Purpose

This guide helps to answer a single question: when should you use each ArcGIS Pro SDK extensibility pattern?

It is based on the synthesis of ArcGIS Pro SDK domain knowledge and expertise encapsulated in [Patterns Summary](./patterns-summary.md).
The result is a practical selection guide for architects and developers deciding among:

- Add-in
- Configuration
- Plugin Datasource
- CoreHost

It intentionally excludes unrelated SDK topics and omits personal names.

## Executive Summary

The ArcGIS Pro SDK supports four primary extensibility patterns, and the right choice depends on what you are actually trying to change:

- If you are extending ArcGIS Pro with commands, tools, panes, workflows, or custom UI, choose an add-in.
- If you need to control the ArcGIS Pro startup experience or deeper UX shaping, choose a configuration.
- If you need role-based UI customization of ArcGIS Pro, choose a configuration.
- If you need custom data source integration for unsupported or proprietary data in ArcGIS Pro, choose a plugin datasource.
- If you need a standalone .NET process outside ArcGIS Pro for geodatabase or geometry work, choose CoreHost.

The best advice is to avoid choosing a heavier pattern than the requirement demands. The SDK should be used with architectural restraint: prefer the narrowest pattern that cleanly matches the business need.

## Authoritative Resources

- ProConcepts Framework: https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-Framework
- ProConcept Configurations https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-Configurations
- ProConcept CoreHost: https://github.com/Esri/arcgis-pro-sdk/wiki/proconcepts-CoreHost
- ProConcept Plugin Datasources: https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-Plugin-Datasources
- ProGuide Plugin Datasources https://github.com/Esri/arcgis-pro-sdk/wiki/ProGuide-Plugin-Datasources


## Fast Decision Tree

Start here:

1. Is the goal to add commands, tools, panes, controls, workflow automation, or custom UI inside ArcGIS Pro?
If yes: choose Add-in.

2. Does the solution need to customize the ArcGIS Pro start page, startup experience, or deeply restructure the Pro application UI?
If yes: choose Configuration.

3. Does the solution require role-based customization of ArcGIS Pro?
If yes: choose Configuration.

4. Is the goal custom data source integration for a proprietary or unsupported data source inside ArcGIS Pro?
If yes: choose Plugin Datasource, but ONLY IF read/query-style custom data source integration is sufficient. Read-write integration is NOT supported.

5. Is the goal to run outside of ArcGIS Pro as a console, batch, Windows app, or service-style process?
If yes: choose CoreHost only if the required functionality is available through the supported ArcGIS.Core geodatabase and geometry APIs. Access to
ArcGIS Pro desktop UI and application APIs is not supported in CoreHost.

6. If none of the above fit cleanly:
Reassess whether the requirement is actually an ArcGIS Pro SDK problem or if it is UNSUPPORTED, or whether tasks, geoprocessing, 
Python, or a non-SDK solution is more appropriate.

Note: CoreHost cannot customize the ArcGIS Pro UI. A Plugin Datasource package is headless, but a separate Add-in or Configuration can provide UI
that consumes its data.

## Pattern-by-Pattern Guidance

## Add-in

### Use an add-in when

- You need ribbon buttons, tools, dockpanes, galleries, property sheets, or custom controls.
- You need to streamline workflows inside ArcGIS Pro.
- You need custom commands or task-specific UI for editors, analysts, or operators.
- You need to invoke ArcGIS Pro APIs, geoprocessing tools, or Python from a structured in-app workflow.
- You need to extend existing ArcGIS Pro experiences such as editing, maps, layouts, reports, metadata, parcel fabric, or framework UI.
- You want the user to remain inside the normal ArcGIS Pro application UI.

### Why add-in is the default

Add-ins are the primary and most common extensibility pattern. They are the standard answer when the 
requirement is “extend ArcGIS Pro and/or the ArcGIS Pro UI” They are also the fastest path for most 
organizational customization work.

### Typical examples

- A custom editing workflow with ribbon commands and a dockpane
- A map exploration tool that adds new commands and camera behavior
- A layout productivity extension
- A reporting or data-quality tool integrated into the existing ArcGIS Pro UI
- A custom workflow that runs geoprocessing tools from buttons or panes

### Engineering characteristics

- Built from SDK templates in Visual Studio or VS Code or from agentic AI in copilot or other AI-centric client IDEs
- Structured around a module class and Config.daml
- Majority of UI defined declaratively through DAML in the Config.daml
- Non-ribbon-based UI, such as dockpanes and property sheets, uses MVVM and WPF.
- Packaged as `.esriAddInX`
- Hosted inside the ArcGIS Pro process

Add-ins can be combined with other add-ins. Multiple add-ins from all different vendors can be loaded together during a 
given ArcGIS Pro session. Multiple Add-ins can be combined with a single Configuration.

### Tradeoffs

- Best for extending the existing ArcGIS Pro experience, it CANNOT replace it
- Not the right choice when you must own startup or deeply customize the ArcGIS Pro application UI
- Not the right choice if the main problem is standalone processing outside of ArcGIS Pro

## Configuration

### Use a configuration when

- You must customize the ArcGIS Pro start page.
- You need control over startup behavior or startup UX.
- You need deeper UI customization than a standard add-in provides.
- You need a guided, role-specific, or streamlined ArcGIS Pro experience.
- You need all the capabilities of an add-in plus additional customization and streamlining capabilities beyond what the add-in provides.

### When configuration is mandatory

- If start page customization is required, configuration is mandatory.
- If role-based UI customization is required, configuration is mandatory

Configurations are the pattern for deeper control of the ArcGIS Pro UI, including startup behavior and custom start pages.

### Typical examples

- A focused editing application built on ArcGIS Pro with a custom start page
- A role-based ArcGIS Pro experience that hides or conditions parts of the application UI
- A specialized solution where startup must guide users into a narrow workflow

### Engineering characteristics

- Includes ALL add-in capabilities plus deeper UX control
- Supports conditional UI customization
- Supports custom start page and startup behavior
- Packaged as `.ProConfigX`
- Hosted inside the ArcGIS Pro process

### Tradeoffs

- Heavier than an add-in
- Appropriate only when start page or startup customization and/or role-based UI control is a real requirement
- Overkill if the only need is to add commands, panes, or workflow tools

Note: Configurations CANNOT BE COMBINED with other Configurations. Only ONE Configuration can be the active Configuration for the 
ArcGIS Pro session. A Configuration can be combined with multiple add-ins.

## Plugin Datasource

### Use a plugin datasource when

- The core problem is custom data source integration inside ArcGIS Pro.
- You have a proprietary or unsupported data source that ArcGIS Pro does not natively support.
- You want ArcGIS Pro to work with it the same as any supported data source in the Geodatabase, such as layers or tables (albeit in a read-only fashion).
- The main value is integration of the data model, not custom UI.

### What it is for

Plugin datasource is a custom data source integration pattern. Plugin datasources make custom data sources available to ArcGIS Pro, including 
proprietary files, GPS-style sources, relational databases such as MySQL, non-relational stores, and other nonstandard formats.
Plugin datasources allow proprietary and unsupported data sources to be consumed by ArcGIS Pro the same as any supported data source in the Geodatabase for read-only access. Read-write access is NOT SUPPORTED.

### Critical limit

Plugin Datasources are READ-ONLY. The Plugin Datasource package is for custom data integration and is not itself a UI extension model. When a solution
also needs commands, panes, or other UI, implement those concerns in a separate Add-in or Configuration.

That means this pattern is a poor recommendation when the real requirement is:

- deep UI customization
- command and pane creation
- startup control
- a custom editing model over proprietary data that requires rich write/schema behavior beyond the pattern’s intended fit

### Typical examples

- A proprietary data source that must display and query in ArcGIS Pro
- A custom data source that should appear as layer content inside a map workflow
- External data that needs to participate in ArcGIS Pro the same as any supported data source in the Geodatabase (albeit in a read-only fashion)

### Tradeoffs

- Solves custom data source integration, not general UI extension
- Solves custom data source integration, but not custom data source editing and updates
- It is a distinct pattern, but it can be deployed alongside an Add-in or Configuration that provides related UI.
- If the requirement includes strong write/edit/schema demands for proprietary data, then this is NOT SUPPORTED.

## CoreHost

### Use CoreHost when

- The solution must run outside ArcGIS Pro.
- You need a standalone batch, console, Windows app, or service-style process.
- You need geodatabase or geometry APIs without the ArcGIS Pro desktop UI.
- You need scheduled or headless processing.
### What it is for

CoreHost is the standalone processing pattern. It provides 64-bit geodatabase and geometry access outside the ArcGIS Pro application.
It is intended as an engine-style Pro runtime for standalone scenarios.

### Typical examples

- A nightly geodatabase maintenance process
- A console application that performs geometry operations
- A background service that processes data outside the desktop UI

### Critical limit

CoreHost is not an ArcGIS Pro UI extension pattern. It does NOT extend the ArcGIS Pro application. If the requirement is to add buttons, panes, or 
commands inside ArcGIS Pro, CoreHost is the wrong recommendation even if the underlying logic is geodatabase-related. CoreHost ONLY supports
geodatabase and geometry API access. If the requirement is for unsupported standalone API access outside those areas, then it is NOT
SUPPORTED. 

### Special Considerations for CoreHost

ArcGIS Pro application MUST BE INSTALLED on the same machine that the CoreHost will be run. CoreHost requires the installed ArcGIS Pro binaries to
support its runtime environment. CoreHost requires an ArcGIS Pro license at runtime. `Host.Initialize()` validates the process architecture,
STA threading model, ArcGIS Pro installation, and licensing at startup. If initialization fails, the standalone executable cannot continue.

### Tradeoffs

- Excellent for noninteractive processing
- No ArcGIS Pro application UI customization possible
- Very limited or narrow range of appropriate use cases as compared with full in-application ArcGIS Pro extension scenarios
- Requires the ArcGIS Pro application to be installed and consumes an ArcGIS Pro license

## Decision Matrix

| Requirement Signal | Recommended Pattern | Why |
| --- | --- | --- |
| New ribbon button, pane, tool, command, report, or workflow inside ArcGIS Pro | Add-in | Standard in-app extension model |
| Need to automate user workflows inside ArcGIS Pro | Add-in | Best fit for custom commands and UI-driven routines |
| Need to run geoprocessing from an ArcGIS Pro-based solution | Add-in | Intended for managed in-app workflow extension |
| Need to customize startup experience, application UI, or role-based UI | Configuration | Startup customization is a core purpose |
| Need role-specific or branded ArcGIS Pro UX | Configuration | Deeper UI/UX customization than add-ins |
| Need proprietary data to appear in ArcGIS Pro as usable layer content | Plugin Datasource | Data-integration pattern |
| Need standalone batch/console processing | CoreHost | Runs outside ArcGIS Pro |
| Need geodatabase/geometry APIs without ArcGIS Pro UI | CoreHost | Headless/standalone model |

## Conflict Cases and How to Resolve Them

### Case 1: “I need custom buttons and a custom start page.”

Recommendation: Configuration.

Reason: Start page customization forces configuration, and configuration includes add-in-style capabilities.

### Case 2: “I need a proprietary data format and also a custom dockpane.”

Recommendation: Likely a combination of patterns, with Plugin Datasource for the data and Add-in or Configuration for the UI.

Reason: Plugin datasource is for data integration only, UI extension requires Add-in or Configuration.

### Case 3: “I need a nightly batch updater for geodatabase content.”

Recommendation: CoreHost.

Reason: The runtime context is standalone, not in-application.

### Case 4: “I need to remove or heavily rework built-in application UI behavior.”

Recommendation: Configuration.

Reason: Deep UI restructuring and startup control require a Configuration.

### Case 5: “I need to extend editing inside Pro with custom commands and editor UI.”

Recommendation: Add-in.

Reason: The goal is workflow and UI extension inside ArcGIS Pro, Add-ins are the primary customization pattern.

### Case 6: “I need to edit a proprietary data format directly through the plugin datasource.”

Recommendation: NOT supported. Do not assume plugin datasource is the right fit.

Reason: Plugin datasources are read-only. If editing or schema mutation is necessary, the request needs custom implementation.

## What Not to Use

### Do not use Configuration when

- The only need is UI customization, tools, or panes.
- There is no startup customization or deep UX requirement.

### Do not use Plugin Datasource when

- The main problem is UI customization.
- The main problem is workflow integration into the ArcGIS Pro application.
- The main problem is anything other than custom data source integration.
- The main problem involves data editing whether the data format is proprietary or not.

### Do not use CoreHost when

- The main problem involves extending the ArcGIS Pro application.
- The main problem requires workflow integration into the ArcGIS Pro application.
- The main problem is anything other than implementing a standalone executable of Geodatabase and/or Geometry api access.

### Do not use Add-in when

- Start page customization is required.
- The solution must be standalone and run without ArcGIS Pro.
- The main problem is exposing proprietary data rather than extending the UI/workflow layer.
- The main problem requires role-based UI customization

## Secondary Design Guidance

Once the pattern is selected, there are a few recurring engineering rules:

- Prefer SDK templates or agents rather than building structure manually.
- Use DAML for declarative UI composition.
- Respect `QueuedTask` and threading rules when calling framework-bound APIs.
- Use WPF and MVVM for implementation of ArcGIS Pro application custom UI.
- Ensure proper version alignment between ArcGIS Pro, the SDK, and .NET. Add-ins and Configurations are forwards compatible NOT backwards compatible

These rules do not determine the pattern by themselves, but influence implementation quality once the decision is made.

## Questions to Ask Before Committing to a Pattern

- Must the solution run inside ArcGIS Pro, or outside it?
- Is startup or start page customization required?
- Is Role-based UI customization required?
- Is the real need UI/workflow extension, or data integration?
- Does the solution need to expose unsupported proprietary data?
- Is a lighter solution such as tasks, geoprocessing, or Python sufficient?

If these questions are answered clearly, the correct pattern is usually obvious.

## Recommended Selection Order

Use this order during design reviews:

1. Confirm runtime scope.
If it must run outside ArcGIS Pro, select CoreHost and stop evaluating UI-centric patterns.

2. Check for startup or application control.
If start page or deep application UI customization is required, select Configuration.

3. Check whether the primary concern is proprietary data integration.
If yes, evaluate Plugin Datasource before considering UI-driven patterns.

4. If the primary concern IS proprietary data integration, determine if Read-write access is required.
If yes, then this pattern is NOT supported.

5. Default to Add-in for standard ArcGIS Pro extension work.
If the requirement is primarily commands, tools, panes, UI, workflow automation, or API-driven in-app behavior, select Add-in. 
Add-in is always the preferred choice over Configurations.

6. Reject unsupported or mismatched requests.
If none of the four patterns fit, do not force a recommendation. State that the requirement is NOT supported.

## Bottom Line

The best ArcGIS Pro SDK pattern is not always the most powerful one. It is the one that most directly matches the requirement.

- Add-in is the default for in-application workflow and UI extension.
- Configuration is the mandatory choice for start page, role-based, and deep application UI customization.
- Plugin Datasource is for read-only custom data source integration in ArcGIS Pro. It does not support any UI customization.
- CoreHost is for standalone processing outside of ArcGIS Pro only.

## One-Page Checklist

Use this checklist during solution design reviews.

### Step 1: Confirm runtime context

- Will the code run inside ArcGIS Pro?
- Will the code run outside ArcGIS Pro as a batch, console, service, or Windows app?

Decision:

- Outside ArcGIS Pro -> CoreHost
- But if access to unsupported APIs outside the CoreHost surface is required -> This is NOT supported
- Inside ArcGIS Pro -> continue

### Step 2: Check for level of Application UI control

- Do you need a custom start page?
- Do you need to control the startup experience?
- Do you need deep application UI customization or a curated role-based UX?

Decision:

- Yes to any of these -> Configuration
- No -> continue

### Step 3: Check for proprietary data integration

- Is the main requirement to expose a proprietary or unsupported data source in Pro?
- Should that data behave like native layers or tables?
- Is the problem mostly about data access rather than UI customization?

Decision:

- Yes -> Plugin Datasource
- But if editing or schema mutation is required -> This is NOT supported
- No -> continue

### Step 4: Check for standard in-app extension needs

- Do you need custom buttons, tools, panes, galleries, or property sheets?
- Do you need workflow automation inside Pro?
- Do you need to call Pro APIs from custom in-app UI?
- Do you need to extend editing, layouts, reports, maps, metadata, or framework UI?

Decision:

- Yes -> Add-in

### Step 5: Reject mismatches early

- If the problem is simple automation, could tasks, geoprocessing, or Python solve it without the SDK?
- If the problem mixes UI extension and proprietary data integration, do not force a single-pattern answer without checking whether 
two patterns are needed.
- If no pattern fits cleanly, evaluate if the pattern is supported or not supported.

## Example Scenarios and Recommendations

| Example Scenarios | Recommended Pattern | Why |
| --- | --- | --- |
| Add a ribbon button that runs a custom editing workflow | Add-in | Standard in-app UI and workflow extension |
| Add a dockpane for quality control review inside Pro | Add-in | Pane-based UI extension inside the Pro application |
| Launch geoprocessing from a custom Pro workflow | Add-in | In-app automation pattern |
| Create a custom map tool or layout productivity extension | Add-in | Extends existing ArcGIS Pro experiences |
| Build a custom startup experience for a specialized editing solution | Configuration | Startup customization is required |
| Replace the default start page with a domain-specific start page experience | Configuration | Start page customization requires Configuration |
| Hide or restructure parts of the application UI for a role-specific app | Configuration | Deep UX control goes beyond a normal add-in |
| Expose a proprietary file format as a usable layer in ArcGIS Pro | Plugin Datasource | Custom data source integration is the core requirement |
| Integrate a non-native relational or proprietary data source into ArcGIS Pro | Plugin Datasource | The pattern is designed for custom data source integration |
| Build a nightly geodatabase maintenance utility | CoreHost | Standalone runtime outside Pro |
| Run geometry or geodatabase logic from a console app | CoreHost | Headless processing model |
| Build a service-like background process that must not depend on the ArcGIS Pro UI | CoreHost | Standalone process. Assumes only Geodatabase or Geometry access is required |
| Add custom editor UI and custom data source integration together | Usually Add-in plus Plugin Datasource | UI and data source integration are separate concerns |
| Need custom startup UX and custom commands together | Configuration | Configuration includes add-in-like capabilities plus startup control |
| Need proprietary-format editing with schema/write behavior | Not supported | Plugin datasource is a read-only fit for unsupported formats |
| Need standalone application that can access mapping and layout APIs | Not supported | CoreHost does not expose ArcGIS Pro desktop mapping and layout APIs |