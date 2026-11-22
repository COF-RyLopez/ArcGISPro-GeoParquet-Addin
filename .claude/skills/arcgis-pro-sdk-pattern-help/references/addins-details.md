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
# ArcGIS Pro SDK Add-ins: Technical Implementation Details

## Scope

This document provides an overview of the ArcGIS Pro SDK Add-in and its available UI extensibility points, with emphasis on:

- Add-in project structure and runtime wiring
- Declarative DAML UI Model and Module contract
- JIT loading, activation, and UI realization behavior
- Extensibility points
- Tool Specializations
- Panes, Pages, and ProWindow Summary
- Additional Considerations

This is an implementation and overview guide, not a high-level pattern comparison. This document complements the Add-in description in [Patterns Summary](./patterns-summary.md) with additional details. For pattern comparison, refer to [Patterns Decision Guide](./patterns-decision-guide.md).

## Core Implementation Model

An ArcGIS Pro add-in is packaged as a .esriAddInX zip archive and loaded by ArcGIS Pro from a well-known folder.

The minimum Add-in Implementation consists of these parts, in no particular order of importance:

- A Module class deriving from `ArcGIS.Desktop.Framework.Contracts.Module`. This is REQUIRED.
- A Config.daml file containing AddInInfo metadata and a DAML declaration for the Module including its DAML ID. This is REQUIRED
 - The Module Daml ID is its unique identifier and is used by the ArcGIS Pro framework to create and retrieve the Module instance at runtime
- A .csproj or .vbproj file defining the add-in, necessary assembly references and a reference to the `Esri.ProApp.SDK.Desktop.targets` targets file which contains the necessary custom build logic required to create and deploy the .addinX archive
- Zero or more optional plugin classes such as buttons, tools, combo boxes, galleries, panes, dock panes, property pages, backstage tabs, 
  and custom controls. Plugin classes all derive from `ArcGIS.Desktop.Framework.Contracts.Plugin` and contain both a DAML declaration in the Config.daml and a concrete class (deriving from the relevant "Contract" class) in a code-behind file.
- Resources such as images, XAML, strings, and supporting assemblies

The add-in model is the primary and recommended ArcGIS Pro SDK pattern for standard UI extension and workflow implementation inside the Pro application.

Fit boundary:

- Add-ins extend the normal ArcGIS Pro UI; they do not replace it.

## Declarative UI Model in Config.daml

For add-ins, much of the ArcGIS Pro UI customization model is declarative.

Core ideas:

- Ribbon UI composition is primarily declared in Config.daml.
- DAML defines the static identity, placement, captions, tooltips, images, conditions, and layout of many UI elements.
- Managed code supplies the active behavior behind those declarations.

Think of the Config.daml as the declarative UI registration layer and the C# classes as the behavior layer.

## DAML Wiring Requirements

In the Config.daml, the AddInInfo block and module declarations provide the runtime contract:

- Exactly one AddInInfo element per add-in DAML
- Required AddInInfo metadata including id, version, and desktopVersion
- At least one `insertModule` declaration within the `modules` section. Most Add-ins use one module; use `updateModule` to modify content owned by another module.
- Plugin daml declarations nested under or associated with their owning module. Most commonly within the `<controls>` child element. 
- Other daml declaration groupings include, but are not limited to `<dockpanes>`, `<menus>`, `<palettes>`, and `<propertySheets>`. All module content MUST be declared within the scope of its `<insertModule>` element.
- Other optional top-level sections such as conditions, categories, backstage, and dropHandlers

### Key implications:

- All add-in content is tethered to a module. The module class is instantiated BEFORE any other add-in content.
- DAML provides the declarative UI structure. DAML IDs map from an element's declaration (in Config.daml) to its concrete type implemented in code.
- DAML IDs MUST be unique across the entire ArcGIS Pro application.
- IDs are the stable integration surface or reference point for updates, deletes, dependencies, command search, and runtime lookup for all DAML element content.

### Additional implementation notes from framework guidance:

- All DAML from all ArcGIS Pro extensions and all add-ins is consolidated into a single in-memory representation of the UI at runtime.
- DAML is transactional. All Inserts are applied, followed by ALL updates, followed by all deletes to determine the final ArcGIS Pro UI configuration.
- Add-ins can insert, update, or delete DAML elements created by OTHER contributors if load ordering and dependencies are correct. Meaning they can modify DAML content loaded BEFORE them in the start-up cycle.
- ArcGIS Pro application framework can read add-in DAML to apply declared UI customizations at startup WITHOUT loading the add-in module itself.
- Assuming the Module is being delay-loaded (default behavior), the Module will NOT be instantiated UNTIL one of its UI components is accessed or executed (e.g. a button is clicked, a dockpane is activated, and so on)
- Use the dependencies element with dependency name set to the target add-in id when your DAML updates another add-in's UI.
- This ensures the depended-on add-in loads first regardless of folder location.

Note: An active Configuration can still make additional changes to the finished ArcGIS Pro UI DAML representation within the `ConfigurationManager` `OnUpdateDatabase(XDocument database)` callback.

## AddInInfo Contract

The AddInInfo element stores the add-in identity and compatibility metadata within the Config.daml. It consists of the Add-in metadata detailing the add-in name and description as well as who and which company authored/owns the add-in and the creation date.
`AddInInfo` has three REQUIRED attributes and one OPTIONAL attribute:
- id: GUID identity for the add-in (required)
- version: Author-controlled add-in version metadata (required)
- desktopVersion: Minimum ArcGIS Pro version required to load the add-in (required)
- product: Set to one of two values: `ArcGISPro` or `ArcGISAllSource`. (optional). Product is processed as follows:
 - If not present (default), ArcGIS Pro is assumed as target application. Add-in will be loaded by ArcGIS Pro application (assuming `desktopVersion` is compatible)
 - If present and set to `ArcGISPro`, ArcGIS Pro is set as target application. Add-in will be loaded by ArcGIS Pro application (assuming `desktopVersion` is compatible)
 - If present and set to `ArcGISAllSource`, ArcGIS Pro AllSource product extension is set as target application. Add-in will be loaded by ArcGIS Pro AllSource extension, assuming it is the current running application.
 - If present and set incorrectly to neither the `ArcGISPro` or `ArcGISAllSource` strings, ArcGIS Pro is assumed as target application. Add-in will be loaded by ArcGIS Pro application (assuming `desktopVersion` is compatible)

Important versioning behavior:

- `desktopVersion` should specify the ArcGIS Pro major version your add-in targets.
- Add-ins are forward-compatible across minor versions within the same major version.
- Add-ins are NOT backward-compatible across ArcGIS Pro versions whether within the same major version "line" or not.
- If multiple copies of the same add-in are discovered by the ArcGIS Pro application at start-up, the highest (compatible) version will be loaded.

## Module Contract

All add-in subsystem behavior is organized around classes deriving from:

- ArcGIS.Desktop.Framework.Contracts.Module

The module is the central access point and lifecycle owner for the module's subsystem.

Important module characteristics:

- Modules are singletons. The Module class exposes a static Current accessor that is the Module's singleton instance. 
- Accessing any Module within the ArcGIS Pro application sub-system via its Module ID (using `FrameworkApplication.FindModule(string id)`) always returns the SAME instance
 - Accessing a Module via `FrameworkApplication.FindModule` will trigger loading of the Module if it is not already loaded.
- Modules act as the logical parent of their controls, panes, dock panes, and related state.
- Modules are the framework entry point for the add-in subsystem.
- Business logic and state common to the add-in should generally be centralized in the module or in helpers managed by the module. The module can ALWAYS be accessed from anywhere within the add-in.
 - Plug-ins refer back to the module for shared state and behavior.

## Runtime Loading and Activation Model

Add-ins are normally loaded just in time or "JIT" loaded. This behavior is controlled by the `<insertModule>` daml element's autoLoad attribute. 
 - `autoLoad="true"` means the add-in will be automatically loaded at start-up.
 - `autoLoad="false"` means the add-in will be JIT loaded (default) whenever any aspect of it is accessed via the ArcGIS Pro application.
 
 JIT loading is a performance optimization to allow the ArcGIS Pro application to load quickly by deferring loading of all extensions and add-ins until such a time as they are needed.

Module loading triggers include:

- A constituent control is clicked or created
- A pane or dock pane owned by the module is created
- A module condition becomes satisfied
- Certain licensing/configurable extension entry points are activated

Key JIT loading implications:

- Startup cost stays low because most add-in objects are deferred.
- Modules are not unloaded when a condition later becomes false.
- Visibility and activation are not the same thing; a control can exist in DAML and be visible/rendered ON THE RIBBON while remaining UNLOADED until it is invoked.
 - Declared UI can therefore appear integrated into ArcGIS Pro BEFORE the add-in's managed code (via its Module) is actually instantiated.

### Loading Order, Well-Known Folders, and Dependencies

ArcGIS Pro framework will detect and load add-ins from "well-known" folders and the logged on user's default folder at start-up. The order of precedence in processing well-known folders and the default folder for add-ins is as follows:

1. Well-known folders declared within the DAML of a Configuration
2. HKLM admin well-known folders. Refer to `https://github.com/esri/arcgis-pro-sdk/wiki/ArcGIS-Pro-Registry-Keys#settings-keys`
3. HKCU current-user well-known folders. Refer to `https://github.com/esri/arcgis-pro-sdk/wiki/ArcGIS-Pro-Registry-Keys#settings-keys-1`
4. Default user add-in folder

Note: Within a folder add-ins are loaded in alphabetical order determined by id.

### Build, Packaging, and Deployment

- Standard build output for add-ins is a .esriAddInX zip archive.
- Normal developer builds install the add-in for debugging on the local machine.
- Build servers can compile add-ins without ArcGIS Pro installed by using the ArcGIS Pro Extensions NuGet or by manually staging required Pro assemblies and targets. Refer to `https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Advanced-Topics#configure-build-server-to-build-add-ins-and-configurations`
- MSBuild can be used for command-line builds.

Installation behavior:

- Add-ins can be deployed by copying them to a well-known folder.
- RegisterAddin.exe supports installation workflows including EULA presentation.
- Local add-ins appear in My Add-ins; add-ins from well-known shared folders appear as Shared Add-ins.
- An add-in can coexist with any number of other add-ins and plugin datasources in the same ArcGIS Pro session.

## Extensibility Points

The add-in framework supports a broad set of UI and workflow extensibility points, including:

- Ribbon tabs, tab groups, groups, subgroups, and toolbar arrangements
- Buttons, tools, check boxes, combo boxes, edit boxes, spinners, menus, dynamic menus, galleries, split buttons, and custom controls
- Panes and dock panes
- Property sheets and property pages
- Backstage tabs and buttons
- ProWindow dialogs
- Component categories and embeddable control registration
- Drag-and-drop handlers and custom drop handlers
- Custom items and custom project items

This breadth is why add-ins are the standard pattern for in-application customization.

Product note:

- The same add-in pattern also applies to ArcGIS Pro AllSource extension add-ins; Configurations are not supported for AllSource.

### Plug-in Class Model

A plugin, SPECIFICALLY, is a ArcGIS Pro UI component that derives from the `ArcGIS.Desktop.Framework.Contracts.Plugin` base class. All plugins have a declaration within the Config.daml. Typically within the `<controls>` daml subsection.

ArcGIS Pro available Plugins are, in no particular order:

- `ArcGIS.Desktop.Framework.Contracts.Button`
- `ArcGIS.Desktop.Framework.Contracts.CheckBox` (derives from `Button`)
- `ArcGIS.Desktop.Framework.Contracts.ComboBox`
- `ArcGIS.Desktop.Framework.Contracts.CustomControl`
- `ArcGIS.Desktop.Framework.Contracts.DynamicMenu`
- `ArcGIS.Desktop.Framework.Contracts.EditBox`
- `ArcGIS.Desktop.Framework.Contracts.Gallery`
- `ArcGIS.Desktop.Framework.Contracts.PaneBase`
- `ArcGIS.Desktop.Framework.Contracts.Pane` (derives from `PaneBase`)
- `ArcGIS.Desktop.Framework.Contracts.Dockpane` (derives from `PaneBase`)
- `ArcGIS.Desktop.Framework.Contracts.Spinner`
- `ArcGIS.Desktop.Framework.Contracts.Tool` (derives from `Button`)
- `ArcGIS.Desktop.Mapping.MapTool` (derives from `Tool`) - note: namespace `ArcGIS.Desktop.Mapping`
- `ArcGIS.Desktop.Layouts.LayoutTool` (derives from `Tool`) - note: namespace `ArcGIS.Desktop.Layouts`

All Plugins have the following properties:
- DAML ID (must be unique)
- Caption 
- Tooltip
- TooltipHeading
- DisabledTooltip
- SmallImage
- LargeImage

Key behavior notes:

- Declarative metadata such as caption, tooltip, images, and IDs are supplied through DAML.
- Code-behind is only needed for active behavior such as OnClick, OnUpdate, OnPopup, OnSelectionChange, InitializeAsync, or OnActivate.
- Plug-in wrappers can be retrieved and updated at runtime through `FrameworkApplication.GetPlugInWrapper(string damlId)`.

### UI Composition Mechanics

In add-ins, control declaration and control placement are separate concerns.

Important composition rules:

- Controls are declared under controls.
- Groups determine how controls appear and are organized within tabs, including size and separators.
 - Tabs place groups on the ribbon.
 
Much of the ribbon UI scaffolding and some of the general application UI is DECLARATIVE only. The following UI constructs are declared in the DAML but have no concrete class in the code behind. They are used to organize and arrange their respective content. In most cases these are other controls (such as buttons and tools). In other cases, such as Property Sheets, they organize child Property Pages within the Property Sheet dialog UI table of contents, TOC:
 - Tab (ribbon)
 - Tab group (ribbon)
 - Group (ribbon)
 - Sub group (ribbon)
 - Palette (ribbon)
  - Button palette (ribbon)
  - Tool palette (ribbon)
 - Split button (ribbon)
 - Menu 
 - Radial menu
 - Toolbar
 - Mini toolbar
 - Wizard
 - Property sheet
 
 Essentially, ANY DAML element that does NOT HAVE a `className` attribute is declarative ONLY.

Note: Useful layout behaviors:

- `appearsOnAddInTab=true` attribute for group elements is a quick path for surfacing controls on the special default "Add-In" tab. 
The "Add-in" tab is only shown if there is content that is being hosted on it (via the `appearsOnAddInTab=true` attribute). Content CANNOT be explicitly placed with an Add-in tab daml declaration.
- Subgroups and sizePriorities allow further control over content arrangement on Tabs.
- Conditions can be applied to tabs and groups as well as controls.

### Ribbon UI Extensibility Summary

The ribbon model is built from tabs, groups, and controls declared in DAML.

Tabs and groups:

- Tabs are the top-level ribbon surface and can be global or context-sensitive.
- Groups hold the controls that appear on a tab and determine per-control size and separators.
- Tab groups cluster related contextual tabs.
- Group launcher buttons can open supporting dialogs for less-frequently used actions.
- Subgroups allow tighter control over ribbon collapse behavior and vertical stacking as the application width changes.

Control families:

- Buttons are the simplest command surface and support captions, tooltips, keytips, disabled text, extended captions, preload behavior, runtime wrapper access, and runtime image changes.
- Check boxes behave much like buttons but expose checked and tri-state behavior directly on the ribbon.
- Edit boxes provide typed text entry, edit hints, validation hooks, and size control through DAML.
- Combo boxes provide dynamic item lists, selection-change callbacks, optional editability, and custom item templates through dataTemplateFile and templateID.
- Label controls are purely declarative and are useful when a group needs explanatory text rather than an active command.
- Spinners are numeric edit controls with increment and range semantics.

Container controls:

- Menus are declarative collections of controls and can be reused inline in other menus.
- Context menus can be attached to tools automatically or created manually on the UI thread, and dynamic menus can populate items on the fly in OnPopup.
- Tool palettes and button palettes group related tools or buttons under a collapsed surface that promotes the active choice.
- Split buttons expose a primary command plus related commands or galleries.
- Galleries provide richer visual choice surfaces than menus and are typically populated at runtime.
- Galleries can be drop-down or inline, can use custom item templates or template selectors, and support loading feedback with a spinner and loading message.
- Toolbars provide declarative grouped command strips inside ribbon groups.

Visual and accessibility features:

- Keytips and menuKeytips support keyboard access on ribbon and menu surfaces.
- Extended captions disambiguate repeated labels in customize/task dialogs.
- XAML icons can be used in place of raster images for higher-resolution ribbon presentation.

### Conditions and State Model

Add-ins use a declarative condition system to control visibility, enabled state, and context-sensitive loading. States and conditions are a major part of how add-ins keep UI relevant to application context.

Scope clarification:

- Conditions and states primarily control the enabled state of declarative command surfaces such as buttons, tools, menus, split buttons/split tools, galleries, and related ribbon controls.
- Conditions and states also control visibility for ribbon tabs, tab groups, and groups, and can control visibility of property pages in a property sheet.
- Embeddable control visibility is not primarily controlled through DAML conditions/states; for map-tool-associated embeddable controls, visibility follows activation and deactivation of the associated map tool.

Core ideas:

- State is a named Boolean (or binary) "fact" about application or pane context or "state". A state is EITHER TRUE or FALSE. Examples include: is a pane active, is a layer selected in the TOC, is a layer legend selected, is a feature selected, and so-on.
- Condition is a DAML expression composed from one or more states that are defined globally in DAML. States are combined into conditions within the daml via boolean operators: And, Or, XOr, Not.
- States are continuously activated or deactivated within the application in response to changing application state or UI context.
- Depending on the given application state at any particular instant, a given condition will evaluate to TRUE or FALSE depending on the underlying combination of its component state and the boolean operators combining them.
- Conditions are referenced, via DAML, on controls, tabs, groups, views, and other UI element declarations via a `condition` attribute set to the DAML id of the desired condition.
- Only ONE condition can be referenced by a given element (in the DAML).

Important runtime behavior:

- Controls are ENABLED or DISABLED based on their associated condition.
- Tabs, tab groups, and groups on the ribbon can be shown or hidden based on conditions.
- Property pages can be shown or hidden within their parent property sheet based on conditions

State locality matters. ArcGIS Pro maintains application-level state and pane-level state. Activate global facts through `FrameworkApplication.State`;
activate facts that belong to a specific pane through that pane's state table. Condition evaluation combines application state with the active pane's
state, so using the correct scope prevents one pane's context from leaking into another.

Note: Conditions do not directly control arbitrary WPF controls inside a custom pane or dockpane. Use standard WPF techniques for this purpose.

Note: Implicit states are maintained by the framework, such as active pane, active tab, active tool, and module loaded. Implicit states can be used as CONDITIONS. For example, to use the activation state of a map pane as a condition, use the map pane DAML id as the condition: `esri_mapping_mapPane`.
- Conditions can evaluate using BOTH application-level state and (implicit) pane state. They simply need incorporate the relevant pane daml ids into their DAML declaration.

Design implication: 

- Prefer conditions and state over explicit (i.e. "coded") event wiring when you want your add-in Ribbon UI content to follow application context.
- Developers can define and toggle explicit application states using FrameworkApplication.State.Activate and Deactivate.
- ArcGIS Pro defined conditions can also be reused directly by add-ins when they match the required context.

### Delegate Commands

Delegate Commands are a pattern for simplifying the creation of buttons in the ribbon. Instead of writing a complete button plug-in class that inherits from the abstract button class, you can instead declare, in the button DAML declaration, that your button is really just a static method on the module class.
The `className` attribute is declared as a static method on the Module class. For example, given a Module with `id="MyAddIn_Module"`, a delegate command for the button `id="MyAddIn_Module_Button"` would be declared as:
`<button id="MyAddIn_Module_Button" className="MyAddIn_Module:OnCustomButtonClick" .../>` where `OnCustomButtonClick` is the name of a static method defined on the module class.
- Optional OnUpdate behavior can be supplied with a matching static Can method or property pattern.

### Configurable Extensions for Licensing and Conditional Enablement

- An add-in can expose proprietary licensing or enablement logic through the `<extensionConfig>` DAML element and implementation of `IExtensionConfig` on the module class.
- Configurable extensions can execute their own proprietary licensing logic to determine their enabled state within their respective `IExtensionConfig` implementations.
- Configurable extensions are shown on the ArcGIS Pro backstage Licensing tab within the External Extensions list view.
- Add-in configurable extension state can be Enabled, Disabled, or Unavailable. Add-ins can execute their own proprietary licensing logic in response to a user request to enable the add-in.
- JIT loading still applies; the licensing tab can trigger activation of the module.

## Tool Specializations

Not all add-in commands are simple buttons.

Tool pattern:

- Tools are command plug-ins that remain checked while active and interact with the active pane. Only ONE tool can be ACTIVE at any one time within an ArcGIS Pro session.
- A Pane MUST be active in order for a tool to be active.
- Map tools and editing tools often depend on sketch geometry, pane context, and mouse/keyboard event overrides.
- Construction tools participate in editing categories through categoryRefID and appear in the editor construction palettes on the Create Features dockpane.
- Construction tools can surface persistent options through toolOptionsID and embeddable option controls.

Tray button pattern:

- Tray buttons live on the map or layout pane tray regions. They do NOT reside on the ribbon.
- Tray buttons support button, toggle, and popup-toggle styles.
- Popup tray buttons can host a small MVVM popup UI for settings or workflow options.

### Embeddable controls:

- Embeddable controls are reusable view/view-model components most commonly associated with map tools to provide additional tool-specific UI.
- Map tools can host overlay embeddable controls directly over the map view by setting OverlayControlID and related placement properties. The associated embeddable control is shown as long as its associated tool is THE ACTIVE TOOL.
  - When the Map tool is deactivated, its associated embeddable control (if it has one) is hidden.
- They are commonly used for tool options, overlay controls, or category-driven hosted UI tied to the active tool.
- Construction tools with options can also use this pattern together with the `esri_editing_tool_options` category to host additional UI for setting and configuring tool options.

## Panes, Pages, and ProWindow Summary

Add-ins can extend ArcGIS Pro beyond the ribbon using several window and pane models.

Panes, also referred to as view panes in practice:

- Panes are multi-instance application views.
- Each pane has DAML, a managed controller/view model, and a WPF view.
- Panes can specify default tabs and default tools.
- The active pane is a primary part of application context and influences which ribbon content is relevant.

Dock panes (most common):

- Dock panes are logical singletons and persist docking state across sessions.
- They support docking, grouping, floating, pinning, loading messages, help integration, optional drag-and-drop, and optional shared or custom undo/redo behavior.
- They are best treated as MVVM view models with minimal XAML code-behind.
- Initial placement is controlled declaratively through dock, dockWith, and related attributes.
- They are the MOST COMMONLY IMPLEMENTED UI EXTENSIBILITY PATTERN FOR REPRESENTING MULTIPLE-STEP WORKFLOWS or custom application options and/or customization settings.
- In general, if a user needs to implement custom UI within his or her add-in that is NOT suitable for the ribbon, dockpane is typically the PREFERRED choice.

Property sheets and property pages:

- Property sheets are purely declarative containers of pages.
- Property pages are active components with their own managed class and view.
- Pages load only when visited, can be conditional, and participate in validity, modified state, commit, cancel, and reset workflows.
- This pattern is appropriate when the workflow requires access to custom property editing UI rather than pane interaction or workflow implementations. They are NOT a replacement or substitute for dockpanes.

Backstage tabs:

- Backstage tabs are view-style surfaces hosted in the full-screen backstage experience.
- They combine a managed BackstageTab class with a view and can also participate in conditions.
- They are most commonly used to customize the Open and New experiences (typically as part of a Configuration) or for other custom project-level actions.

ProWindow:

- ProWindow is a modal or modeless dialog surface for custom add-in windows. It is styled to have the SAME appearance as other ArcGIS Pro application dialogs.
- It is the right fit when the user interaction is dialog-oriented rather than ribbon- or pane-oriented.
- It can also participate in framework drag-and-drop patterns through drop handlers exposed on the window view model.

### Toolbars and Pane-Hosted UI

Custom toolbars for panes:

- Starting at 3.7, add-ins can declare custom toolbars for custom panes and existing Pro panes.
- These toolbars are fully declarative and can host buttons, tools, menus, split buttons, and palettes much like ribbon tabs.
- Multiple toolbars can be stacked or docked to different sides of a pane.
- A toolbar can also expose an extended menu in burger-button form.
- Existing pane toolbars can be updated with DAML, though Pro-defined system toolbars cannot be deleted.

## Additional Considerations

### Security and Trust Requirements

Add-in loading is also governed by ArcGIS Pro security policy.

Supported security postures include:

- Load all add-ins
- Load only signed add-ins
- Load only signed add-ins that are signed by Esri

Design implication:

- Packaging and deployment are not sufficient by themselves; the add-in must also satisfy the active trust policy to load.

### Guidelines for Add-in Implementation Strategy

1. Keep the module as the coordination hub for subsystem state and behavior.
2. Prefer conditions and state for UI relevance instead of broad event wiring.
3. Leave `autoLoad="false"` unless you have a concrete need for eager module initialization.
4. Use custom tabs and groups deliberately; use `appearsOnAddInTab="true"` only as a convenience for debugging purposes, not a default architecture.
5. Package third-party dependencies intentionally and avoid Copy Local on Pro assemblies.
7. Use dependency declarations and load-context settings explicitly when multiple add-ins must interoperate.
8. Assume generated add-in projects whether by agents or templates are the CORRECT structural starting point
  - Customize from that scaffold rather than hand-building the add-in structure.

#### Relationship to Configurations in Implementation Terms

- Add-ins are THE STANDARD and PREFERRED pattern for extending ArcGIS Pro UI and workflows.
 - Configurations can also host add-in-style components but are intended to be used when startup customizations and/or role-based UI customization is REQUIRED.
- If you ONLY need ArcGIS Pro application UI customizations or workflow logic, an add-in is typically the CORRECT implementation pattern.

Use [Patterns Decision Guide](./patterns-decision-guide.md) for AUTHORITATIVE details.

### Authoritative References

- ProConcepts Framework (authoritative): https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Framework
- ProConcepts Advanced Topics (authoritative): https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Advanced-Topics
- ProGuide Build Your First Add-in (authoritative): https://github.com/esri/arcgis-pro-sdk/wiki/ProGuide-Build-Your-First-Add-in
- ArcGIS Pro SDK API Reference: https://doc.esri.com/en/arcgis-pro/latest/sdk/api-reference/
- ArcGIS Pro Registry Keys: https://github.com/esri/arcgis-pro-sdk/wiki/ArcGIS-Pro-Registry-Keys
- ArcGIS Pro SDK Community Samples: https://github.com/esri/arcgis-pro-sdk-community-samples 