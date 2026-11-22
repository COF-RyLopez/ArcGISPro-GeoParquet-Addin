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
# ArcGIS Pro SDK Extensibility Patterns Summary

## Executive Summary

The ArcGIS Pro SDK presents extensibility as a spectrum rather than a single technique. Developers should choose the lightest pattern that 
solves the problem:

- Use user settings, tasks, geoprocessing, or Python when that is sufficient.
- Use an add-in as the primary and recommended choice when the goal is to create new tools, UI, or workflow automation inside ArcGIS Pro.
- Use a managed configuration when the solution needs everything an add-in provides plus startup experience ownership, start page customization, role-based UI customization, or deeper application UI streamlining.
- Treat add-ins and configurations as complementary patterns in one solution when both focused feature extension and deeper startup or role-based experience customization are required.
- Use a plugin datasource when the core problem is custom data source integration and the data should behave the same as any supported data source in the Geodatabase inside ArcGIS Pro.
- Use CoreHost when the requirement is for a standalone process or service that needs geodatabase and geometry access external to the 
ArcGIS Pro application.

## Authoritative Resources

- ProConcepts Framework: https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-Framework
- ProConcept Configurations https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-Configurations
- ProConcept CoreHost: https://github.com/Esri/arcgis-pro-sdk/wiki/proconcepts-CoreHost
- ProConcept Plugin Datasources: https://github.com/Esri/arcgis-pro-sdk/wiki/ProConcepts-Plugin-Datasources
- ProGuide Plugin Datasources https://github.com/Esri/arcgis-pro-sdk/wiki/ProGuide-Plugin-Datasources
- Internal add-in details reference: [Add-in Details](./addins-details.md)
- Internal configuration details reference: [Configuration Details](./configurations-details.md)

## Core Concepts

The ArcGIS Pro SDK depends on the following core concepts:

- Declarative UI definition through DAML within an add-in or configuration Config.daml
- Module-based add-in and configuration structure. Both are based on very similar principles.
- Utilization of WPF and MVVM-style desktop development patterns for custom UI implementation
- A modern asynchronous API model built around the concept of the Main CIM Thread, MCT, and exposed as the `QueuedTask`.
- Project and item templates for Visual Studio that generate most of the required scaffolding for all SDK patterns
- Project creation support for Visual Studio Code through the ArcGIS Pro extension and this repository's Copilot resources
- Agents and skills for use in AI-assisted development environments such as GitHub Copilot
- A continually evolving public API that is enhanced with each ArcGIS Pro release

Core concepts are further elaborated below.

### Public API Functionality Areas

The major capability themes, in no particular order, are:

- Content: For working with catalog content based on the concept of items. This includes the project itself to include portal projects. It 
includes project items which are persisted directly within the ArcGIS Pro project which includes map, layout, chart, and investigation project items, 
geodatabase toolboxes, database connection and folder connection project items, style project items and more. It also includes items which
are not persisted in the project and are typically file-based item content discovered on disk such as images, text files, documents, zip archives,
folders, file geodatabases, coverages, and shapefiles. It also includes programmatic access to application and project options and settings.
- Catalog: Portal items, portal queries, and retrieval of portal content.
- Geometry: Including support for points, lines, polygons, multi-patches, and multi-points as well as projections, spatial references, and
many, many different spatial operator and spatial relationship implementations consolidated within the `GeometryEngine`.
- Geodatabase: Includes support for all Esri supported databases and file-based workspaces. Includes support for raster/image and vector data formats
and fine-grained API access for data query, retrieval, and editing. It also includes schema creation and editing capabilities.
- CIM: Access to the Cartographic Implementation Model, CIM, for access to fine-grained properties and settings underpinning ArcGIS Pro model objects
including all map and layer types, standalone tables, and their respective symbology. Application model state is persisted in the project via the CIM.
- Editing: 2D and 3D editing tools, editing operations, parcel workflows, editor customization
- Framework/UI: ribbon customization, controls, panes, conditions, startup UI, command automation, sdk pattern lifecycle control with the exception of
CoreHost which runs external to the application.
- Geoprocessing: invoking geoprocessing tools from custom .NET code
- Map exploration: Map views, animations, camera control, map tools, tray tools
- Map Authoring: Maps, layers, standalone tables, charts, symbology, as well as specialized APIs for graphics layers, LAS, point clouds, voxel layers,
Stream layers, Scenes, Stereo mapping, and Raster. Application context such as selections, map toc, selected layer, selected table.
- Layout: layout elements, layout views, selections, layout tool patterns as well as layout context such as active layout view, and selected elements.
Includes support for reports and presentations.
- Metadata: Item metadata and custom metadata forms via the Metadata Toolkit
- Extensions: integration with products such as Data Reviewer and Workflow Manager
- Parcel Fabric: Parcel and ownership data, parcel topology, surveys, traverses, COGO, records, specialized editing tools
- Knowledge Graph: Investigations, link charts, openCypher queries, advanced analysis such as centrality and filtered find path analysis, schema
editing, documents, and provenance
- Tasks: Programmatic access to tasks and steps stored within the project. 

The important architectural insight is that the implementation pattern (Add-in, Configuration, Plugin Datasource, or CoreHost) determines hosting,
deployment, lifecycle, and integration with ArcGIS Pro. The API domain determines what operations are available within that pattern's supported
runtime boundary. Selecting an API domain does not, by itself, determine the extensibility pattern.

### The Module as the Unit of Composition for Add-ins and Configurations

Add-ins and Configuration lifecycles are controlled by their Module. In practice, this means:

- The module is the logical container for the add-in and configuration
- The module is the point of entry for the ArcGIS Pro framework looking to instantiate an add-in or configuration.
- The module acts as a sub-system. It allows JIT loading of add-ins and their components.
- Configurations, theoretically, can also be JIT loaded BUT because they must customize the startup experience, they are always force-loaded at
Application startup at the beginning of the ArcGIS Pro session.

The module concept reinforces that ArcGIS Pro extensibility is intentionally structured. Developers are not writing isolated event 
handlers scattered across an application. They are composing extension elements consolidated and deployed within an application framework-owned 
model.

### DAML and Declarative ArcGIS Pro Ribbon UI Customization

Declarative programming with DAML is one of the central technical themes for ArcGIS Pro UI customization. The key points are:

- Add-ins use DAML to define UI elements in Pro. Ribbon buttons and tools (i.e. "commands"), menus and menu items, and related UI components are 
defined declaratively within a special file, the Config.daml.
- DAML covers buttons, dockpanes, galleries, property sheets, tools, and related UI pieces.
- DAML is XML-based and stored in a `Config.daml` file deployed within the add-in or configuration archive.
- DAML supports adding, updating, or removing UI elements. It is transactional meaning that inserts, updates, and deletes of ArcGIS Pro UI 
components are applied by Framework at application start-up.
- All Ribbon UI customizations are purely declarative via DAML. Workflows initiated via ribbon UI are implemented in add-in and configuration .NET 
code-behind associated with the DAML.

This is a major design choice in the SDK. Rather than forcing developers to construct all UI imperatively, Pro exposes a 
declarative layer for composition and registration. This has several benefits:

- Predictable integration into the framework
- Declarative UI customizations can be read at start-up WITHOUT JIT loading the add-in module. This allows add-in ArcGIS Pro application UI 
customizations to be applied while deferring the add-in loading expense until some aspect of the add-in is invoked (for example, via a button click)
- Cleaner separation between UI declaration and implementation code
- Declarative support for application framework concepts such as conditions and states

Note: DAML is transactional. They are part of a coherent application framework-managed configuration model.

### DAML Conditions, States, and UI Responsiveness

- Conditions declared in DAML control the enabled or disabled state for ribbon controls and visibility for tabs, tab groups, and panes.
- States can be combined via Boolean operators within the Config.daml to produce conditions.
- A condition becomes true when the combination of its constituent states and their boolean operators evaluate to true. States evaluate specific,
contextual events within the application. They are transitory and evaluate to true or false depending on the application context at any one instant.
- Conditions are associated with declarative ArcGIS Pro UI. When the condition is true, all ArcGIS Pro UI with which it is associated is enabled and
vice versa when the condition is false.
- Each declarative ArcGIS Pro application UI element can be associated with a SINGLE condition. However, a single condition can be associated with
MULTIPLE application UI elements.
- Conditions and states are DAML constructs. They CANNOT be used within any WPF implementation of custom user controls (e.g. that is hosted on a
dock pane)

ArcGIS Pro UI extensibility expects developers to model availability declaratively rather than scattering ad hoc enable/disable logic 
everywhere. That is a strong pattern for maintainability, especially in larger enterprise add-ins.

## Programming Patterns for Add-in and Configuration Authors

### MVVM and Desktop Patterns

The programming pattern for implementing custom UI other than on the ribbon requires WPF and MVVM. Specifically:

- MVVM is the preferred UI architecture pattern
- Implementations must use modern asynchronous API design
- Implementations must use standard .NET desktop development practices

The ArcGIS Pro SDK expects developers to use mainstream .NET desktop engineering patterns and combine them with ArcGIS Pro-specific 
application framework rules.

### `QueuedTask` and the Main CIM Thread

Developers must respect the documented thread affinity of each ArcGIS Pro API member. Many synchronous application and CIM state operations require
the ArcGIS Pro Main CIM Thread (MCT), accessed through `QueuedTask.Run`:

- Synchronous API members documented as requiring the MCT must be invoked on `QueuedTask`.
- The asynchronous runtime environment within ArcGIS Pro has been designed to simplify asynchronous programming for developers.
Developers need only consider two threads within their implementations: The QueuedTask and the UI thread.
- Some synchronous members are UI-thread-bound, and asynchronous members documented as callable from any thread should not be wrapped in `QueuedTask`.
- The `QueuedTask.Run` serializes or "queues" all requests to run functionality on it such that only one request is run at any one time. All other 
requests are queued.
- The `QueuedTask` serializes all requests to ensure maintenance of consistent application state.
- The primary purpose of the `QueuedTask` is to provide a responsive application UI and NOT to provide support for parallel processing.
- Consolidate related MCT-bound SDK operations into coherent `QueuedTask` workflow segments. Keep WPF interaction on the UI thread and independent
managed work on an appropriate .NET thread.
- Use of the `QueuedTask` is enforced via a `CalledOnWrongThread` exception. It is not intended that this exception would be thrown in production code.
Correct any `CalledOnWrongThread` exceptions by moving the relevant API call to the thread required by its API reference documentation.
- The API reference and intellisense explicitly marks methods that require the MCT. Use the API reference as the authority on thread affinity

The ArcGIS Pro application runtime framework deliberately prioritized making the multithreaded architecture easier to program against. That is 
useful context: `QueuedTask` is not accidental complexity, but a framework pattern designed to keep the UI responsive and the state model safe.

### Method Categories and Execution Boundaries

Within the ArcGIS Pro application API are three broad categories of methods:

- Synchronous methods that must run inside `QueuedTask`. This includes the majority of methods. 
- Coarse-grained asynchronous methods that can be called from any thread. An example is the Geoprocessing API `ExecuteToolAsync()` for invoking
geoprocessing tools from .NET.
- Coarse-grained asynchronous methods that MUST be run on the UI thread. An example is the set of methods for creating application UI such as map and layout panes. 
There are a very small number of these methods. Calling them on the `QueuedTask` will result in an `InvalidOperation` exception.

- Note: Non-API .NET methods can be run on any thread and their correct use is at the discretion of the developer. 

This distinction suggests a practical engineering pattern:

- Consolidate add-in and configuration business logic and computation workflows into predominantly synchronous methods. These 
synchronous methods should be invoked via the `QueuedTask`.
- This advice may seem contradictory in that developers are encouraged to implement predominantly synchronous methods for use in an asynchronous
application. However, by implementing a predominantly synchronous api, developers can combine individual synchronous methods into more complex 
workflows that can then be called, in their entirety, asynchronously. The converse is to write all methods as asynchronous which can then become difficult
to combine into larger, sequential, more complex workflows given that all the individual components are asynchronous.
- Fine-grained asynchronous methods do not perform well when combined with other fine-grained asynchronous methods. The impact of constantly switching
threads between the QueuedTask and the UI as well as UI refreshing triggered by the context switch can seriously degrade application performance.
- Workflows are best exposed as custom asynchronous methods that invoke their internal, synchronous components on the `QueuedTask` same as ArcGIS Pro's
own coarse-grained asynchronous methods.
- Asynchronous methods should be awaited when sequential processing is desired and/or application UI to be made responsive while the 
asynchronous operation is running.


## Runtime Packaging, Installation, and Sharing of SDK Implementations

### Packaging

The packaging models are clearly documented:

- Add-ins are packaged into a single compressed `.esriAddInX` zip formatted archive.
- Configurations are similarly packaged, but with a `.ProConfigX` extension.
- Plugin datasources are packaged as a `.esriPlugin` zip formatted archive.
- CoreHost are custom, standalone executables, i.e. ".exe's".

### Deployment

- Add-ins, Configurations, and Plug-in datasources can all be deployed via "double-click" deployment on any machine on which ArcGIS Pro is installed.
- ArcGIS Pro SDK archive file types have a file-association established with `RegisterAddin.exe`, installed to the ArcGIS Pro bin folder. Despite its
name, `RegisterAddin.exe` will deploy all package types to their respective default folder location within the logged-on user's `Documents\ArcGIS`
folder.
- ArcGIS Pro SDK archives can also be installed to the desired target location via standard XCopy.
- `RegisterAddin.exe` supports command line options for use with custom deployment scripts, e.g. via powershell or .bat files.
- CoreHost executable deployments are custom and are the responsibility of the developer or owner.

Note: Add-in, Configuration, and Plug-in datasource archives can be deployed to any folder that is accessible from the host machine. If the deployment folder
is other than the default location (for the respective archive type), it must be configured as a `Well-known folder` in the registry to be discovered at 
runtime.

Note: add-ins and configurations can be listed, distributed, and monetized through ArcGIS Marketplace.

### Runtime Loading

ArcGIS Pro scans the applicable default and registered well-known folders for Add-ins and Plugin Datasources at startup. A Configuration is not
passively activated by this scan; it must be selected with `/config:` or by registry setting. When `/config:` specifies a Configuration name rather
than a full path, ArcGIS Pro searches the configured Configuration folders for that package. Packages must also satisfy applicable version and
security requirements.

- Addins that have a DAML `insertModule` element `autoLoad` attribute set to false (default) will be JIT loaded when they are first invoked
during the session. This could be in response to a button click, menu item selection, or event, depending on the nature of the add-in.
- Addins that have a DAML `insertModule` element `autoLoad` attribute set to true are immediately loaded when the application starts up.
- Addins need NOT be loaded in order to read the add-in user customizations from the Config.daml. UI customizations declared within the Config.daml are 
applied to the application UI at start-up
- Plugin datasources are always JIT loaded. They are invoked/instantiated whenever their data source is first accessed by the ArcGIS Pro application
 (e.g. when they are added as the datasource to a layer or table or are accessed via code utilizing the plugin workspace).
- Configurations can only be loaded via the ArcGIS Pro application command line, usually configured within a desktop shortcut. It is loaded 
immediately on application startup and its UI customizations applied. A configuration can be combined with any number of add-ins and plugin 
datasources per session. However, only ONE configuration can be loaded per ArcGIS Pro application session. 

Note: A registry key can also be defined to always run ArcGIS Pro under a specific configuration.

- CoreHost standalone applications are NOT loaded by the ArcGIS Pro application. As standalone executables, they are loaded and invoked via the host
operating system directly (via command line, batch program, desktop shortcut, etc.)

### Versioning

Packaged extensions use `desktopVersion` as a minimum ArcGIS Pro version contract:

- Within the Config.daml, the `AddinInfo` element's `desktopVersion` attribute specifies the minimum required version of ArcGIS Pro that the add-in, 
configuration, or plugin datasource supports. 
- If the version of the ArcGIS Pro application is EQUAL to, or GREATER than (i.e. "better" or "later" than) the minimum required `desktopVersion`, then
the add-in, configuration, or plugin datasource can be loaded.
- If the version of the ArcGIS Pro application is LESS than (i.e. "earlier" than) the minimum required `desktopVersion`, then
the add-in, configuration, or plugin datasource cannot be loaded.
- Forward compatibility still requires validation against the target release, particularly when API or .NET runtime requirements change.
- CoreHost compatibility is governed by its target .NET runtime, referenced ArcGIS Pro assemblies, and runtime assembly resolution. It is the
responsibility of the developer or owner to validate each target environment.

Note: Plugins use a Config.xml and not a Config.daml.

### Security

The ArcGIS Pro application supports the following security levels to control add-in loading:

- Load all add-ins. This is the default. All add-ins are loaded that meet the versioning requirements specified in the "Versioning" section
- Load all signed add-ins. Load only add-ins that have been signed (i.e. are from trusted sources)
- Load all signed add-ins that are signed by Esri. Load only add-ins that have been signed by Esri (i.e. they are from Esri).

The same security levels apply to Plugin datasources and Configurations. The Configuration must be specified on the command line and meet 
the configured security level to be loaded.

## The Supported Patterns

### Add-ins

Add-ins are the default pattern for extending ArcGIS Pro. They are used for:

- Creating new commands, tools and routines
- Adding custom controls, panes, galleries, property sheets, and buttons to the application UI
- Implementing custom workflows or consolidating core UI workflows to reduce repetitive clicks
- Organizing or re-organizing the ribbon tabs with new or existing tools
- Integrating geoprocessing tools into custom routines
- Building editing tools, layout tools, and map exploration tools

In general, add-ins are the most common and recommended way to extend ArcGIS Pro quickly for organizational or customer-specific needs. They are the default
pattern for ArcGIS Pro UI and workflow extension.

### AllSource Add-ins

ArcGIS Pro SDK supports Add-ins for Esri ArcGIS AllSource. ArcGIS Pro Add-in capabilities and extensibility
points are likewise available to AllSource add-ins.

Configurations are NOT supported for AllSource.

### Configurations

Configurations, sometimes referred to as "Managed Configurations", can be thought of as "super" or "uber" add-ins. They can do "everything an add-in 
can do" plus provide a deeper level of UI customization and streamlining (than an add-in). Differentiators with an add-in are:

- Configurations can provide a custom start page UI and/or startup experience
- Conditional customization of the ArcGIS Pro UI based on the logged-on user role (i.e. role-based customization at start-up)
- A deeper level of UI customization to streamline the application than a standard add-in can provide

Configurations and add-ins are complementary. Start with an add-in for core extension requirements, and use a configuration when additional startup,
start page, role-based UI, or deeper streamlining requirements are present.

### Plugin Datasources

Plugin datasources address custom data integration. The decks describe them as the way to make non-native content available inside ArcGIS Pro, 
with examples such as:

- Unsupported relational databases such as MySQL
- Proprietary file formats such as GPS data
- Non-relational databases
- Other proprietary or file-based data stores

Plugin datasources allow ArcGIS Pro to work with a custom data source the same as any supported data source in the Geodatabase for standard feature layers or tables. Access is read-only. Read-write access to custom data is NOT supported.

### CoreHost Applications

CoreHost is a standalone and/or headless pattern. It is:

- A standalone application exe
- A console or service-oriented model for 64-bit geodatabase and geometry api processing outside the ArcGIS Pro application

It does not:
- Provide access to any OTHER ArcGIS Pro apis
- Customize the ArcGIS Pro application

Note: CoreHost standalone exe's require an ArcGIS Pro license at runtime and can only run on a machine where ArcGIS Pro has been installed.
CoreHost has been compared to the original ArcObject's engine-style API model. That comparison explains the purpose of CoreHost: 
It is not for in-application ArcGIS Pro UI customization or extending, but for background or independent processing where the 
ArcGIS Pro application UI is unnecessary.

### Pattern Combinations

It is not unusual for a combination of add-ins, a configuration, and plugin datasources to be loaded within a single ArcGIS Pro session. All package
components may be from the same or different vendors. In general:

- Any add-in or plugin datasource may be "activated/loaded" alongside any number of other add-ins or plugin datasources within the same
ArcGIS Pro session
- A single configuration only may be "activated/loaded" alongside any number of other add-ins or plugin datasources within the same
ArcGIS Pro session.

However:
- A configuration cannot be "activated/loaded" alongside another configuration within the same ArcGIS Pro session. The configuration is a
singleton pattern. Only one configuration can be active, and it will remain active, for the entire ArcGIS Pro session.
- CoreHost cannot be combined with any of Add-ins, Configurations, or Plugin Datasources. CoreHost is standalone whereas the other patterns extend
the ArcGIS Pro application.

## Project Structure and Development Workflow

### Templates, Agents, and Skills are the Starting Point

The templates, agents, and skills provide:

- The templates provide a basic framework for generating the necessary UI, code behind classes, and scaffolding for the ArcGIS Pro SDK patterns and
all supported ArcGIS Pro UI extensibility points.
- The agents and skills provide an equivalent framework that can be invoked through natural language prompts using copilot or any other AI-enabled
client IDE.

It is assumed that SDK developers will work from generated scaffolding as their starting point and then fill in the desired behavior and customizations.
It is not intended that SDK developers hand-build every structural file from scratch.

## Pattern-by-Pattern Guidance

This is extensively addressed in [Patterns Decision Guide](./patterns-decision-guide.md). This borrows from the `Executive Summary` in the
afore mentioned decision guide:

The ArcGIS Pro SDK supports four primary extensibility patterns, and the right choice depends on what you are actually trying to change:

- If you are extending ArcGIS Pro with commands, tools, panes, workflows, or custom UI, choose an add-in.
- If you need to control the ArcGIS Pro startup experience or deeper UX shaping, choose a configuration.
- If you need role-based UI customization of ArcGIS Pro, choose a configuration.
- If you need custom data source integration for unsupported or proprietary data in ArcGIS Pro, choose a plugin datasource.
- If you need a standalone .NET process outside ArcGIS Pro for geodatabase or geometry work, choose CoreHost.

The best advice is to avoid choosing a heavier pattern than the requirement demands. The SDK should be used with architectural restraint:
prefer the narrowest pattern that cleanly matches the business need.

In summary:

### When to Prefer Add-ins

Choose add-ins when the requirement is to:

- Add commands, buttons, tools, or panes to Pro
- Reduce repetitive user workflows
- Combine API access with custom UI
- Run geoprocessing from within a custom workflow
- Extend editors, maps, layouts, reports, or other in-app experiences

They are the best fit when the user remains inside the normal Pro shell and needs custom capability rather than a redefined product shell.

### When to Prefer Configurations

Choose configurations when the requirement is to:

- Configure or customize the ArcGIS Pro application startup experience
- Utilize a custom start page
- Conditional customization of the UI at runtime based on the logged-on user role - i.e. role-based configuration
- Streamlining of the application UI, to include removing unnecessary/unneeded application ribbon tabs, to support very specific workflows

Note: Configurations are singletons and cannot be combined with other configurations, only add-ins (and plugin datasources) 

### When to Prefer Plugin Datasources

Choose plugin datasources when the requirement is for custom data source integration:

- Integrate proprietary, custom, or unsupported data sources into ArcGIS Pro, typically as  data source
- Use proprietary, custom, or unsupported data sources as the data source for layers or standalone tables (e.g. within a map)

Note:
- Data access is read-only. There is NO read-write capabilities within Plugin Datasources.
- Plugin datasources do NOT support any UI customization capabilities.

### When to Prefer CoreHost

Choose CoreHost when the requirement is to:

- Run a standalone program that requires geodatabase or geometry api access.
- Execute scheduled or batch processes without invoking the ArcGIS Pro application.
- Build service-like or console-based processes around the geodatabase or geometry APIs

Note:
- ArcGIS Pro must still be installed on the same machine as the one the CoreHost application is running on.
- CoreHost executables consume an ArcGIS Pro license at startup
- CoreHost executables can NOT customize the ArcGIS Pro application UI

CoreHost is especially relevant when an organization wants ArcGIS Pro SDK geodatabase or geometry api capabilities integrated with automation pipelines, 
maintenance jobs, or noninteractive scenarios via a standalone application.

## Conclusion

The ArcGIS Pro SDK supports a coherent extensibility model built around four primary patterns, with a continually evolving and expanding
api. Add-ins are the default pattern for extending and customizing ArcGIS Pro, while configurations provide a deeper level of UI 
customization as well as role-based customization. Plugin datasources allow read-only integration of custom data sources into the ArcGIS Pro 
application. CoreHost is exclusively used for implementation of standalone executables that need access to the geodatabase and geometry apis.

Add-ins and Configurations can extend the ArcGIS Pro application and customize its UI; Configurations additionally control startup and application
policy. CoreHost is the standalone pattern. Plugin Datasources integrate custom or proprietary data sources into ArcGIS Pro as read-only tables or
feature classes.