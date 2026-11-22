
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
# ArcGIS Pro SDK Managed Configuration: Technical Implementation Details

## Scope

This document provides an overview of the ArcGIS Pro SDK Configuration and its available UI extensibility points, with emphasis on:

- Configuration project structure and runtime wiring
- The fact that a Configuration supports all Add-in extensibility capabilities 
- Configuration ADDS start-up experience customization and role-based UI customization
- The ConfigurationManager contract and callback override model
- Callback timing across ArcGIS Pro startup lifecycle states
- Safe and unsafe operations per lifecycle stage
- How Configuration and Add-in behaviors combine in one solution

This is an implementation guide, not a high-level pattern comparison. This document complements the Configuration description in [Patterns Summary](./patterns-summary.md) with additional details. For pattern comparison, refer to [Patterns Decision Guide](./patterns-decision-guide.md).

## Overview

ArcGIS Pro configurations are a higher-level customization than standard add-ins. They allow organizations to tailor branding, control application behavior, raise security levels, add admin well-known folders, and modify DAML at runtime. At the core of every configuration is the ConfigurationManager, which acts as the extensibility gateway for intercepting application startup and runtime behavior. Only one configuration can run per session.

## Core Implementation Model

Configurations are zip archives packaged as a .proConfigX extension with an internal structure identical to add-ins with the exception of a ConfigurationManager class. Building a managed configuration project generates the archive file, which is typically installed to the user's local configurations well-known folder. The minimum Configuration implementation consists of these parts, in no particular order of importance:

- ALL of the same components as an add-in per [Add-in Details](./addins-details.md):
	Config.daml, Module class, a .csproj or .vbproj file defining the Configuration, zero or more optional plugin classes, additional Resources
	
and

- The `ConfigurationManager` class deriving from `ArcGIS.Desktop.Framework.Contracts.ConfigurationManager`.
  - The Config.daml contains an additional `<Configuration>` section containing a child `<ConfigurationManager>` element that declares the Configuration.
  - There can only be ONE child `<ConfigurationManager>` element.
- `ConfigurationManager` is UNIQUE to the Configuration. It is not included as part of an Add-in implementation.

Important: A Configuration supports the same extension components and UI extensibility points as an Add-in, but can additionally customize startup experience and apply UI customizations at runtime to ArcGIS Pro daml based on the logged-on user's role.
  - These additional capabilities are provided through specific `ConfigurationManager` callbacks that can be overridden to provide custom behavior.

Critical interpretation rule:

- Everything described in [Add-in Details](./addins-details.md) regarding Add-in extensibility points is also applicable to Configurations.
- This document should be read as ADDITIVE to the Add-in implementation model, not as a narrower alternative model.
- The add-in capabilities may be mentioned here for purposes of providing a complete narrative, but [Add-in Details](./addins-details.md) should be accessed for specific details.

Fit boundary:

- A configuration customizes the ArcGIS Pro application; it does not create a standalone runtime outside of ArcGIS Pro.

### Special Considerations

Configurations act as singletons and are activated either via the command-line using /config:{Configuration_name} or specified within the Windows registry. Installation is handled via RegisterAddin.exe by double-clicking the configuration file.

- Unlike Add-ins, Configurations are a SINGLETON pattern meaning only ONE Configuration can be active per ArcGIS Pro session. 
  - This is in direct CONTRAST to add-ins where any number of add-ins can be loaded for the session. 
  - Unlike Add-ins, Configurations are not passively activated merely because a package is present in a scanned folder.
  - A Configuration must be explicitly selected through the `/config:Configuration_Name_Here` command-line option or a registry setting.
  - Starting an ArcGIS Pro session under a Configuration is commonly done via a desktop shortcut which specifies the `/config:` parameter set to the relevant Configuration to be invoked.
    - If the `/config:` parameter does NOT specify a full path to the Configuration (which is common), then Configuration well-known folders are scanned to locate the Configuration.
	- The user default location is `C:\Users\username\Documents\ArcGIS\AddIns\ArcGISPro\Configurations`. Other well-known Configuration folders can be specified via the `HKLM\Software\ESRI\ArcGISPro\Settings\Configuration Folders` and `HKCU\Software\ESRI\ArcGISPro\Settings\Configuration Folders` registry keys.
	
Note: To ALWAYS start ArcGIS Pro under a specific configuration, use the `HKLM\Software\ESRI\ArcGISPro\Settings\ConfigurationName` registry key. If this key is set then any `/config:` command line parameter will be ignored.

## DAML Wiring Requirements

For Configurations, same as Add-ins, the majority of the ArcGIS Pro UI customization model is declarative. The Configuration Config.daml also includes:

- Exactly one `<Configuration>` element -  the Configuration element "block" provides the configuration's runtime contract. It MUST contain:
	- A single, REQUIRED `<ConfigurationManager>` child element with `className` attribute pointing to the derived manager type's code behind implementation.
    - `ConfigurationManager` may also declare: 
		- Optional AddIns settings (securityLevel and AdditionalWellKnownFolder entries)
        - Optional configuration attributes such as blockAddInTab, blockCustomizeDialog, blockPerfMeter, checkForUpdatesAtStartup

Additional implementation notes from framework guidance:

- A configuration can expose ALL Add-in extensibility points (tabs, groups, controls, modules, panes, dock panes) in addition to startup customization.
- During startup, the Configuration has the FINAL opportunity to modify the combined DAML from ALL Pro extensions and add-ins (loaded for the session) via the `ConfigurationManager.OnUpdateDatabase(System.Xml.Linq.XDocument database)` callback. The entire Pro session DAML is available for modification before the Pro Application UI is realized. Configurations can, for example, make final modifications to the ArcGIS Pro combined DAML based on the role of the logged on user (e.g. to remove editing capabilities or unwanted tabs not permitted or relevant to the logged on user role).

## Configuration Extensibility Baseline

Managed Configurations inherit the full Add-in extensibility surface.

That includes the extensibility points summarized in [Add-in Details](./addins-details.md), including:

- Declarative ribbon customization through DAML in Config.daml
- Tabs, groups, tab groups, subgroups, toolbars, menus, palettes, split buttons, galleries, and the standard ribbon control families
- Buttons, tools, check boxes, combo boxes, edit boxes, spinners, label controls, dynamic menus, and custom controls
- Panes, dock panes, property sheets, property pages, backstage tabs, and ProWindow dialogs
- Component categories, embeddable controls, construction tools with options, tray buttons, overlay controls, and pane-hosted surfaces such as custom pane toolbars and TableControl-based dock panes
- Conditions, states, command search integration, command filters, drag-and-drop, and runtime plug-in wrappers

Design implication:

- If the question is "can a Configuration do the same UI or workflow extensibility work as an Add-in," the answer is YES.
- The differentiator is NOT loss of extensibility points; it is the ADDITION of startup customization and/or startup UI customization and/or role-based UI customizations.

Reading rule for agents and developers:

- Use the Add-ins details document as the baseline reference for available extensibility points.
- Use this Configuration details document for the additional rules, timing, and responsibilities introduced by ConfigurationManager and startup ownership.


## ConfigurationManager Class
Every configuration must implement a `ConfigurationManager` class that derives from the base contract class `ArcGIS.Desktop.Framework.Contracts.ConfigurationManager`.

### Instantiation Timing: 

Configurations are instantiated very early in the application startup sequence, loading before any core or standard modules.

### Rigorous Startup Constraints: 

Because many ConfigurationManager callbacks execute before the host WPF application is constructed or window and background threads are started, custom code inside these callbacks is subject to strict constraints. Failing to respect these thread and application states will crash ArcGIS Pro.

### Configuration Callbacks and Application States

- Implementation of ConfigurationManager callbacks are optional
- Developers only need to override the specific methods required to alter target aspects of ArcGIS Pro. 
- ConfigurationManager callbacks are executed in a strict chronological sequence across THREE main application states:
  1. Pre-initializing
  2. Initializing
  3. Ready

#### Pre-Initializing State

- ArcGIS Pro loads the active configuration very early, before normal extension flow is fully available.
- Callback restrictions are strict before the `Ready` state. Violating them can crash application startup.

-Constraints:

During the pre-initialization phase, the underlying WPF ArcGIS Pro application, UI thread, and QueuedTask are not yet accessible.
- No ArcGIS Pro API calls
- No QueuedTask.Run
- No ArcGIS Pro WPF styles or resource usage
- No event subscriptions
- No custom UI that depends on active Pro UI thread

-Callbacks (in order of execution):
OnShowSplashScreen: Override to provide a reference to a custom splash screen. Return null or do NOT implement the callback to retain the stock application splash screen.
 - Do not reference any ArcGIS Pro styles or resources in custom splash screens
 
RuntimeDamlFile: Gets a replacement Desktop Architecture Markup Language (DAML) file at runtime. Uncommon. Allows a Configuration to provide a full path to a different DAML file than its deployed Config.daml at runtime.

#### Initializing State

As initialization proceeds, record structures begin processing, and standard application settings are configured.

-Constraints:

- Treat API access as limited until Ready. Portal is safe to access on receiving OnApplicationInitializing.
- Do NOT assume maps, layers, projects, panes, layouts, etc. are initialized.
- Avoid early event subscriptions in ConfigurationManager callbacks.

If early event wiring is required:

- Use the Module class override of Initialize
- Set module autoLoad to true in DAML
- Subscribe within Initialize to framework events like ApplicationReadyEvent and ApplicationStartupEvent.

-Callbacks (in order of execution):

OnApplicationInitializing(CancelEventArgs cancelEventArgs): Marks the beginning of the `Initializing` state. Called when the application is beginning to initialize.
 - This marks the safe point to communicate with Portal. 
 - DAML records have not yet been fully processed. 
 - Setting `cancelEventArgs.Cancel` to `true` signals an error and triggers an application shutdown.

OnCreateDaml: Opportunity for the configuration to provide DAML at run-time. (Uncommon) Supports role-based customizations.

ApplicationName: Configurations can specify a replacement name for the application title bar.

OnUpdateDatabase: Provides Configurations with an opportunity to modify the final consolidated DAML declaration of the entire ArcGIS Pro UI. At this
juncture, all detected Add-in DAML has been merged with the ArcGIS Pro extension DAML; JIT-loaded Add-in modules need not have been instantiated.
 - The consolidated DAML is provided via the `XDocument` DAML database. 
 - Use standard XML processing techniques to add, delete, or update the ArcGIS Pro DAML. 
 - This is the PRIMARY opportunity for the Configuration to apply any necessary or relevant role-based UI customization (e.g. delete specific tabs from the ribbon)
 
Note: Use caution when editing this XML in the Configuration. It defines the ENTIRE ArcGIS Pro application UI. Use /dumpcombineddaml command line option to examine/write out the ArcGIS Pro consolidated DAML.

Icon: Configurations can specify a custom ImageSource for the application icon.
 - Typically utilizing a pack URI format. 
 Note: that while the title bar icon updates, the Windows taskbar icon will continue to display the default ArcGIS Pro icon.
 
OnValidateLicense: Configurations can execute custom logic during startup to evaluate user licensing constraints.

TitleBarText: Configurations can specify a replacement text to be displayed on the main application title bar.

OnShowStartPage: Configurations can specify a replacement start page.
 - Return a User Control to override the default startup view. 
	- Return null or do not override to retain the default startup view
 - Configurations are responsible for explicitly binding any supporting view model to their custom start view's data context as needed
 - This is the opportunity for the Configuration to customize the DEFAULT startup experience.
 - The start page logic MUST select an underlying ArcGIS Pro project to initiate the session
 - The application will automatically close the start page.

Note: The application window becomes visible here. OnShowStartPage is REQUESTED in Initializing, but the page is SHOWN in Ready

Options reset behavior: If a user changes or resets ribbon or Quick Access Toolbar customizations during a session, ArcGIS Pro can rebuild those
surfaces and invoke `OnApplicationInitializing`, `OnCreateDaml`, and `OnUpdateDatabase` again. Implement these callbacks so repeated invocation is
safe and produces the same intended UI state.

#### Ready State

The ready state begins when the start page is shown. The relevant ArcGIS Pro project, once selected by the start page logic, is opened to start the
session. Once the application has fully initialized and the user interface is rendered, the ready-state callbacks become active.

-Constraints:
 None
 
-Callbacks (in order of execution):

OnApplicationReady: Executed when ArcGIS Pro has completed its full initialization cycle and the application is in the Ready state. OnApplicationReady executes between the ApplicationReadyEvent and ApplicationStartupEvent publication.
 - On receipt of OnApplicationReady, it is safe to access available ArcGIS Pro APIs, styles, and resources, subject to normal thread-affinity rules and null application context.

OnCreateQuickAccessToolbar: Allows programmatic definition or modification of the Quick Access Toolbar. OnCreateQuickAccessToolbar is the supported way for a configuration to override QAT defaults as QAT is NOT customizable through DAML declarations.

OnShowAboutPage: Configurations can override this callback to specify replacement "About" backstage tab content through a UserControl.
 - OnShowAboutPage is called each time the About page backstage tab is accessed.

OnCanExecuteCommand / OnExecuteCommand: Configurations, via these callbacks, are given the opportunity to intercept command execution streams to filter, monitor, or block specific application commands.
 - Return `false` from `OnCanExecuteCommand` to disable the specified command. Returning `true` preserves the command's underlying enabled or disabled state; it does not force-enable the command.
  - This callback participates in the application's frequent command-state refresh cycle, so keep its processing lightweight.
  - Invoking database queries or website logic to determine command state is not recommended and could severely affect application performance.
  
 - Use OnExecuteCommand to inhibit execution of specific commands (e.g. because a user-role does NOT permit it or because of custom data protections, etc.)
 
#### Implementing Custom Backstage Workflows

Configurations that customize the startup experience should consider providing a complete user experience by updating related backstage tabs accordingly, namely:

 - New: Replace the "new" backstage tab with a custom tab that provides a "New" experience that matches the custom startup experience
  - It is common for Configurations to "hide" the underlying project selection
  - e.g. Session initiation may be initiated by selection of an area of interest, a work order, a custom task item, and so-on.
  - The "New" experience should mimic the start page behavior.
  - The core New backstage tab is identified by the DAML id `esri_core_newProjectBackStageTab`

  - Open: Replace the "open" backstage tab with a custom tab that provides an "Open" experience that matches the custom startup experience
  - e.g. Session initiation may be initiated by selection of an area of interest, a work order, a custom task item, and so-on.
   - The "Open" experience should mimic the start page behavior.
    - A project may be opened based on selection of an existing work order, area of interest, and so-on
	- Selection of an explicit project may be hidden from the user, same as the "New" experience.
  - The core Open  backstage tab is identified by the DAML id `esri_core_openProjectBackStageTab`
When deploying custom start or welcome pages via configurations, a complete user experience requires updating related backstage tabs:

Replacing Backstage Tabs: The default Open and New Project backstage tabs should be explicitly removed via standard DAML (<deleteTab ... />) and replaced with custom tabs built using the ArcGIS Pro Backstage Tab template.

Within `OnUpdateDatabase`, delete the following backstage tabs to remove the "stock" New and Open backstage tabs:

```cs
 var tabs_to_delete = new List<string>() {
  "esri_core_newProjectBackStageTab", "esri_core_openProjectBackStageTab"
 };

```
Example declaration of two replacement backstage tabs within the Configuration Config.daml.


```Xml
<backstage>
  <!-- Provide a replacement New tab -->
  <insertTab id="Demo2_Backstage_GeocodeNewTab" caption="New" 
						 className="Demo2.Backstage.GeocodeNewTabViewModel" 
						 insert="before" 
						 placeWith="esri_core_infoTab">
    <content className="Demo2.Backstage.GeocodeNewTabView" />
  </insertTab>
  <!-- Provide a replacement Open tab -->
  <insertTab id="Demo2_Backstage_GeocodeOpenTab" caption="Open" 
						 className="Demo2.Backstage.GeocodeOpenTabViewModel" 
						 insert="before" 
						 placeWith="esri_core_infoTab">
    <content className="Demo2.Backstage.GeocodeOpenTabView" />
  </insertTab>
	
</backstage>
```

## When to Use a Configuration

- A configuration can host Add-in-style extension artifacts directly.
- Add-ins remain the primary/recommended mechanism for standard UI extension.
- Configuration adds lifecycle callbacks and startup governance that Add-ins alone do not provide.
- In practice, treat Add-in components as feature units and ConfigurationManager as the startup and policy orchestration layer.

Even though a Configuration can host all primary ArcGIS Pro application extensibility patterns supported by an Add-in, Add-ins remain the primary/recommended mechanism for standard ArcGIS Pro application UI extension and custom workflow implementation.

Configurations, however, and NOT Add-ins, are intended to be used in the following scenarios:
- Control of the ArcGIS Pro startup behavior or customization of the ArcGIS Pro startup experience is REQUIRED.
- Role-based UI customization of ArcGIS Pro UI, based on the logged on user role, where role discovery occurs at application start up, is REQUIRED.
- A deeper application UI customization, such as streamlining the ribbon UI by deleting tabs or removing large groupings of core functionality, is required
 - e.g. to remove all editing capabilities
 - e.g. to provide only layout or mapping capabilities (because workflow requirements only need a subset of functionality be available)
 - e.g. to provide only custom tooling relevant to the implemented workflow
 
Remember: If the extensibility need is only for commands, panes, tools, or workflow UI then an add-in is the better fit.

Use [Patterns Decision Guide](./patterns-decision-guide.md) for AUTHORITATIVE details.

## Authoritative References

- ProConcepts Framework (authoritative): https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Framework
- ProGuide Configurations (authoritative): https://github.com/esri/arcgis-pro-sdk/wiki/ProGuide-Configurations
- ProConcepts Configurations: https://github.com/esri/arcgis-pro-sdk/wiki/ProConcepts-Configurations
- ArcGIS Pro SDK API Reference: https://doc.esri.com/en/arcgis-pro/latest/sdk/api-reference/
- ArcGIS Pro Registry Keys: https://github.com/esri/arcgis-pro-sdk/wiki/ArcGIS-Pro-Registry-Keys
- ArcGIS Pro SDK Community Samples: https://github.com/esri/arcgis-pro-sdk-community-samples
