---
name: "ArcGIS Pro SDK Item Generation"
description: "Use when creating, generating, or validating newly added ArcGIS Pro SDK Add-in or managed Configuration items such as buttons, tools, galleries, menus, dockpanes, panes, property pages, custom controls, custom items, project items, backstage tabs, embeddable controls, and map or layout tray buttons."
metadata:
  author: Esri, Inc.
  version: "1.0"
---

# ArcGIS Pro SDK Item Generation

Use these instructions to create complete ArcGIS Pro SDK item patterns and to validate whether an item has the required creation pattern in an Add-in or managed Configuration project.

## Authority and Scope

- Use the version-matched ArcGIS Pro SDK API reference and DAML schema as the authority for API signatures, attributes, component categories, conditions, and thread affinity.
- Use `arcgis-pro-sdk-best-practices.instructions.md` for workflow implementation, threading, async flow, error handling, and general managed C# quality.
- Use `arcgis-pro-sdk-example-resources.instructions.md` for the mandatory implementation confirmation gate and conformance checks on all generated C#, WPF/XAML, and DAML item content. When adapting an example or sample, treat it as evidence, not authority.
- This document is self-contained. Do not depend on a sample project, template project, or other temporary workspace artifact being present.
- These item patterns apply equally inside Add-in and managed Configuration projects. A Configuration retains its one `<Configuration>` block, `ConfigurationManager`, and `<PackageType>Configuration</PackageType>` while using the same item declarations as an Add-in.
- This document covers adding or creating new item content only. It does not provide instructions for updating existing DAML item content, and it does not provide instructions for deleting existing DAML item content. Route those operations to separate update/delete instructions when available.
- The DAML element name `<updateCategory>` does not change this scope. ArcGIS Pro requires that container when a newly created component is inserted into an existing framework category.
- Do not create a new project with these instructions. Route complete project creation to the ArcGIS Pro SDK project creator.
- Generate the minimum complete pattern for the requested item. Add optional behavior only when the prompt requires it.

## Required Generation Workflow

Before writing files:

1. Inspect the target `.csproj`, `Config.daml`, module class, default namespace, and nearby item conventions.
2. Determine the ArcGIS Pro version and matching target framework. Do not introduce APIs or DAML records from another SDK version.
3. Determine whether the project is an Add-in or Configuration. Preserve its existing outer scaffold.
4. Resolve the requested item from both formal and informal names. A selection tool is normally a `MapTool`. If "edit tool" or "sketch tool" could mean either a ribbon `MapTool` or a Create Features construction tool, ask whether it must use the current feature template and appear in Create Features before choosing.
5. Identify every file, DAML declaration, placement reference, category registration, and companion invocation control required by that item.
6. Present that file-and-wiring plan and obtain the explicit implementation confirmation required by `arcgis-pro-sdk-example-resources.instructions.md`. Do not edit implementation files before confirmation.
7. Reuse the project's ID, namespace, indentation, keytip, and naming conventions. Use the exact item images required below instead of inheriting project image conventions. Never replace unrelated DAML or project content.

After writing files, run the validation procedure at the end of this document.

## Human-Readable Comments Are Required

Generated code must help a developer maintain the item after generation.

- Add XML documentation to each generated framework item class explaining what the item does and where ArcGIS Pro hosts it.
- Comment non-obvious ID relationships next to the code or DAML that consumes the ID. Name the declaration that owns the ID.
- Explain why a companion button exists for a dockpane, pane, or property sheet.
- Explain required thread context around non-trivial ArcGIS Pro API work.
- For tools, explain the relationship among `categoryRefID`, `SketchType`, `SketchOutputMode`, and the intended workflow.
- Use `TODO` only for a concrete customization point. State what the developer must supply.
- Do not narrate obvious assignments or retain no-op overrides. The complete role-specific template import blocks required below are intentional.

## Shared DAML Rules

### Preserve the Registry Hierarchy

DAML placement is semantic and schema ordering can matter. Add records under their correct existing parent and preserve the order required by the version-matched `ArcGIS.Desktop.Framework.xsd`.

```xml
<ArcGIS defaultAssembly="AcmeAddIn.dll"
        defaultNamespace="AcmeAddIn"
        xmlns="http://schemas.esri.com/DADF/Registry">
  <modules>
    <insertModule id="AcmeAddIn_Module" className="Module1">
      <tabs />
      <groups />
      <controls />
      <palettes />
      <menus />
      <dockPanes />
      <galleries />
      <panes />
      <splitButtons />
    </insertModule>
  </modules>

  <!-- These are root sections, not children of insertModule. -->
  <backstage />
  <categories />
  <propertySheets />
</ArcGIS>
```

- Declare buttons, combo boxes, custom controls, and tools once under `<controls>`.
- Place a declared control by adding a typed `refID` reference to a group, menu, palette, split button, or backstage section as supported by that host.
- Declare module menus, palettes, galleries, dockpanes, panes, and split buttons in their corresponding module sections.
- Register category-based items under root `<categories>` using the exact ArcGIS Pro component category ID.
- Insert backstage content under root `<backstage>` and property sheets under root `<propertySheets>`.
- Do not duplicate a declaration merely because the item appears in more than one place. Reuse its `refID`.
- A local reference must resolve to exactly one local `id`. A reference beginning with `esri_` may target a built-in ArcGIS Pro item and must be verified for the target SDK version.
- Treat each item example below as an independent pattern. When adding it to a target project, merge new records into the project's existing sections and groups; do not combine examples literally or redeclare shared placeholder IDs such as `AcmeAddIn_Group`.

### IDs, Classes, and Namespaces

- Treat every DAML `id`, `refID`, category ID, condition ID, menu ID, sheet ID, page ID, tool GUID, and hard-coded C# lookup string as case-sensitive.
- Prefix new IDs consistently with the project's safe name, for example `AcmeAddIn_ParcelDockpane`.
- Generate a new unique GUID for each construction tool `<content guid="..."/>`. Do not reuse an example GUID.
- Supply a meaningful `keytip` on every tab, group, button, combo box, tool, custom control, button palette, gallery, and backstage tab declaration. The ArcGIS Pro 3.7 DAML schema requires it. Keep keytips unique within their keyboard-navigation scope.
- A DAML `className` in the `defaultNamespace` may use the short type name. A type in another namespace must use its fully qualified name.
- Apply that namespace rule to both implementation classes and `<content className="..."/>` views.

```xml
<ArcGIS defaultNamespace="AcmeAddIn">
  <button id="AcmeAddIn_RefreshButton" className="RefreshButton" keytip="RF" />
  <button id="AcmeAddIn_Admin_RefreshButton"
          className="AcmeAddIn.Admin.RefreshButton"
          keytip="AR" />
</ArcGIS>
```

### Ribbon Declaration and Placement

Ribbon controls are declarations under `<controls>` and typed references under a `<group>`.

```xml
<tabs>
  <tab id="AcmeAddIn_Tab" caption="Acme" keytip="AC">
    <group refID="AcmeAddIn_Group" />
  </tab>
</tabs>
<groups>
  <group id="AcmeAddIn_Group" caption="Tools" keytip="TL" appearsOnAddInTab="true">
    <button refID="AcmeAddIn_RefreshButton" size="large" />
  </group>
</groups>
<controls>
  <button id="AcmeAddIn_RefreshButton"
          caption="Refresh"
          className="RefreshButton"
          loadOnClick="true"
          keytip="RF"
              smallImage="GenericButtonRed16"
              largeImage="GenericButtonRed32">
    <tooltip heading="Refresh">Refresh the current result.<disabledText /></tooltip>
  </button>
</controls>
```

- Use a relevant condition when an item requires an active context, such as `esri_mapping_mapPane` or `esri_layouts_layoutPane`.
- Keep captions, tooltips, images, class names, and conditions synchronized with the generated behavior.
- A declaration registers an item. It does not necessarily make that item visible. Add the required placement reference or invocation path.

### Required Item Images

- Use the exact image keys shown in the matching item pattern, including companion controls. Ordinary Buttons use `GenericButtonRed16` for `smallImage` and `GenericButtonRed32` for `largeImage`.
- Preserve the role-specific images specified by other patterns, such as the dockpane's `BexDog16` and its companion show button's `GenericButtonPurple16` / `GenericButtonPurple32`. Do not replace all item images with the ordinary Button images.
- Never use `AddInDesktop16` or `AddInDesktop32` for any generated item or companion control. These are not item-image fallbacks; do not copy add-in/package branding or existing project image conventions into new items.
- When a pattern omits images, preserve that omission rather than inventing a fallback. Leave unrelated existing declarations and package branding unchanged.

## Required Base Classes and Using Directives

Use the exact direct base class shown here. Do not substitute a similarly named WPF type or invent a code-behind class for a declarative-only container.

| Item or companion role | Required direct base class |
| --- | --- |
| Backstage tab view model | `ArcGIS.Desktop.Framework.Contracts.BackstageTab` |
| Button, launcher, or actionable menu/palette/split child | `ArcGIS.Desktop.Framework.Contracts.Button` |
| ButtonPalette | Declarative only; its actionable children derive from `ArcGIS.Desktop.Framework.Contracts.Button` |
| Ribbon ComboBox | `ArcGIS.Desktop.Framework.Contracts.ComboBox`, not `System.Windows.Controls.ComboBox` |
| ConstructionTool | `ArcGIS.Desktop.Mapping.MapTool` |
| CustomControl view model | `ArcGIS.Desktop.Framework.Contracts.CustomControl` |
| CustomItem | `ArcGIS.Desktop.Core.CustomItemBase` |
| CustomProjectItem | `ArcGIS.Desktop.Core.CustomProjectItemBase` and `ArcGIS.Desktop.Core.CustomProjectItemContainer<TProjectItem>` |
| Dockpane view model | `ArcGIS.Desktop.Framework.Contracts.DockPane` |
| Drop handler | `ArcGIS.Desktop.Framework.DragDrop.DropHandlerBase` |
| Embeddable Control view model | `ArcGIS.Desktop.Framework.Controls.EmbeddableControl` |
| Gallery and Inline Gallery | `ArcGIS.Desktop.Framework.Contracts.Gallery` |
| LayoutTool | `ArcGIS.Desktop.Layouts.LayoutTool` |
| LayoutTrayButton | `ArcGIS.Desktop.Layouts.LayoutTrayButton` |
| MapTool, including selection and edit tools | `ArcGIS.Desktop.Mapping.MapTool` |
| MapTrayButton | `ArcGIS.Desktop.Mapping.MapTrayButton` |
| Menu | Declarative only; custom actionable children derive from `ArcGIS.Desktop.Framework.Contracts.Button` |
| Pane view model | `ArcGIS.Desktop.Framework.Contracts.ViewStatePane` |
| Property sheet | Declarative only |
| Property page view model | `ArcGIS.Desktop.Framework.Contracts.Page`, not `System.Windows.Controls.Page` |
| SplitButton | Declarative only; its actionable children derive from `ArcGIS.Desktop.Framework.Contracts.Button` |
| TableConstructionTool | `ArcGIS.Desktop.Mapping.MapTool` |
| DAML-hosted or tray-popup WPF view code-behind | `System.Windows.Controls.UserControl` |
| Tray popup view model | `ArcGIS.Desktop.Framework.Contracts.PropertyChangedBase` |

Every generated C# file must include the complete using block shown in the corresponding example below. Preserve the full block, including commonly used ArcGIS Pro namespaces that the initial class body does not yet consume. Do not reduce it to a minimal compiler-derived set. When one file contains multiple companion classes, use the complete union of the role-specific directives without duplicates.

Map tray source files use the full framework block plus `System.Windows.Controls`. Layout tray source files additionally use `ArcGIS.Core.Internal.CIM` after `ArcGIS.Core.Geometry`. Do not conflate these two template profiles.

## Shared WPF and Project Rules

Backstage tabs, custom controls, dockpanes, embeddable controls, panes, property pages, and popup tray buttons use WPF views.

### View and Code-Behind Pair

Use this pair for each generated `UserControl`, replacing all names consistently:

```xaml
<UserControl x:Class="AcmeAddIn.ParcelDockpaneView"
    xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
    xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
    xmlns:d="http://schemas.microsoft.com/expression/blend/2008"
    xmlns:local="clr-namespace:AcmeAddIn"
    xmlns:extensions="clr-namespace:ArcGIS.Desktop.Extensions;assembly=ArcGIS.Desktop.Extensions"
    mc:Ignorable="d"
    d:DataContext="{d:DesignInstance Type=local:ParcelDockpaneViewModel, IsDesignTimeCreatable=False}">
  <UserControl.Resources>
    <ResourceDictionary>
      <ResourceDictionary.MergedDictionaries>
        <!-- Make ArcGIS Pro theme resources available in the XAML designer. -->
        <extensions:DesignOnlyResourceDictionary
          Source="pack://application:,,,/ArcGIS.Desktop.Framework;component/Themes/Default.xaml" />
      </ResourceDictionary.MergedDictionaries>
    </ResourceDictionary>
  </UserControl.Resources>
  <Grid>
    <!-- Add workflow-specific controls and bind them to the framework view model. -->
  </Grid>
</UserControl>
```

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace AcmeAddIn;

/// <summary>Visual content hosted by the Parcel dockpane.</summary>
public partial class ParcelDockpaneView : UserControl
{
    public ParcelDockpaneView()
    {
        InitializeComponent();
    }
}
```

- The XAML `x:Class`, code-behind namespace/type, and DAML content `className` must identify the same view.
- Keep the view class `public` so WPF and the ArcGIS Pro framework can instantiate it.
- `d:DataContext` is design-time metadata. ArcGIS Pro supplies the runtime DataContext for DAML-hosted views.
- A tray popup is different: `ConstructPopupContent` creates its view and assigns the runtime DataContext explicitly.
- Bind only to public properties that exist on the runtime view model. Treat binding warnings as defects.

### XAML Build Items

- The project must set `<UseWPF>true</UseWPF>`.
- SDK-style projects normally have `EnableDefaultPageItems=true`; in that case, do not add duplicate `<Page Include="..."/>` entries.
- If default page inclusion is disabled, explicitly include every generated view and gallery resource dictionary as a `Page` item.
- Keep `Config.daml` and required packaged images in the project using its existing item conventions.

```powershell
dotnet msbuild .\AcmeAddIn.csproj -getProperty:EnableDefaultPageItems
dotnet msbuild .\AcmeAddIn.csproj -getItem:Page
```

## Item Patterns

### Backstage Tab

**Generate:** `Config.daml`, a `BackstageTab` view model, and the shared WPF view/code-behind pair.

Declare the tab with explicit `insertTab` under root `<backstage>`. `placeWith` names an existing backstage item and `insert` controls relative placement.

```xml
<backstage>
  <!-- This view model and view are paired by the two className attributes. -->
  <insertTab id="AcmeAddIn_SettingsBackstageTab"
             caption="Settings"
             className="SettingsBackstageTabViewModel"
             keytip="ST"
             insert="before"
             placeWith="esri_core_exitApplicationButton">
    <content className="SettingsBackstageTabView" />
  </insertTab>
</backstage>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Provides the Settings page in ArcGIS Pro backstage.</summary>
internal class SettingsBackstageTabViewModel : BackstageTab
{
    private string _heading = "Settings";

    public string Heading
    {
        get => _heading;
        set => SetProperty(ref _heading, value);
    }
}
```

- A backstage tab does not need a launcher button; ArcGIS Pro creates it from the root declaration.
- A button placed in backstage is a separate Button pattern and uses explicit `<insertButton refID="..."/>`.
- Validate both placement IDs and the view/view-model class links.

### Button

**Generate:** one control declaration and one class deriving from `Button`. Add one or more placement references appropriate to the prompt.

Use the complete `AcmeAddIn_RefreshButton` declaration and add-in-tab placement in **Ribbon Declaration and Placement** above as this pattern's declaration and initial host. The alternatives below replace or supplement that typed group reference; never emit only a local `refID` without its declaration.

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Refreshes the add-in's current result.</summary>
internal sealed class RefreshButton : Button
{
}
```

When the request specifies click behavior, implement `OnClick()` under `arcgis-pro-sdk-best-practices.instructions.md`. Do not emit a TODO-only or empty override.

Common placements reuse the same declared button:

```xml
<!-- Ribbon group -->
<button refID="AcmeAddIn_RefreshButton" size="large" />

<!-- Menu item -->
<button refID="AcmeAddIn_RefreshButton" separator="true" />

<!-- Root backstage placement requires insertButton. -->
<backstage>
  <insertButton refID="AcmeAddIn_RefreshButton"
                insert="before"
                placeWith="esri_core_exitApplicationButton"
                separator="true" />
</backstage>
```

- Use buttons as companion launchers when a dockpane, pane, or property sheet otherwise has no initial invocation path.
- Put every custom button declaration under `<controls>`, including buttons used only by menus, palettes, and split buttons.
- Use a fully qualified `className` when the class is outside the default namespace.
- Do not create XAML for an ordinary framework Button.

### ButtonPalette

**Generate:** ordinary child Button declarations/classes, a declarative `<buttonPalette>`, and a group reference. There is no `ButtonPalette` C# class.

```xml
<controls>
  <button id="AcmeAddIn_SymbolPalette_Red"
          caption="Red symbol"
          className="RedSymbolButton"
          loadOnClick="true"
          keytip="RD"
          smallImage="GenericButtonRed16"
          largeImage="GenericButtonRed32">
    <tooltip heading="Red symbol">Use the red symbol.<disabledText /></tooltip>
  </button>
  <button id="AcmeAddIn_SymbolPalette_Blue"
          caption="Blue symbol"
          className="BlueSymbolButton"
          loadOnClick="true"
          keytip="BL"
          smallImage="GenericButtonBlue16"
          largeImage="GenericButtonBlue32">
    <tooltip heading="Blue symbol">Use the blue symbol.<disabledText /></tooltip>
  </button>
</controls>
<palettes>
  <!-- The first child is the initially displayed command. -->
  <buttonPalette id="AcmeAddIn_SymbolPalette"
                 caption="Symbols"
                 keytip="SY"
                 dropDown="false"
                 menuStyle="true">
    <button refID="AcmeAddIn_SymbolPalette_Red" />
    <button refID="AcmeAddIn_SymbolPalette_Blue" />
  </buttonPalette>
</palettes>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <buttonPalette refID="AcmeAddIn_SymbolPalette" />
  </group>
</groups>
```

- Generate `RedSymbolButton` and `BlueSymbolButton` with the complete Button C# pattern above; validate each child `refID` and class.
- Do not add programmatic gallery items or a split-button class to this pattern.

### ComboBox

**Generate:** a `<comboBox>` control declaration, a group reference, and a class deriving from the ArcGIS Pro framework `ComboBox`, not the WPF control.

```xml
<controls>
  <comboBox id="AcmeAddIn_LayerComboBox"
            caption="Layer"
            className="LayerComboBox"
            keytip="LY"
            itemWidth="140"
            isEditable="false"
            isReadOnly="true"
            resizable="true"
            condition="esri_mapping_mapPane">
    <tooltip heading="Layer">Choose a layer.<disabledText /></tooltip>
  </comboBox>
</controls>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <comboBox refID="AcmeAddIn_LayerComboBox" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Lists the layers available to the ribbon workflow.</summary>
internal sealed class LayerComboBox : ComboBox
{
    public LayerComboBox()
    {
        // Populate framework ComboBoxItem instances, not WPF ComboBoxItem instances.
        Add(new ComboBoxItem("Layer 1"));
        SelectedItem = ItemCollection.FirstOrDefault();
        Enabled = ItemCollection.Count > 0;
    }
}
```

- If items depend on project state, refresh them at an appropriate lifecycle point rather than assuming constructor-time context exists.
- When the request specifies selection behavior, implement `OnSelectionChange()` under `arcgis-pro-sdk-best-practices.instructions.md`. Do not emit a TODO-only or empty override.

### ConstructionTool

**Generate:** a `<tool>` declaration under `<controls>` and a `MapTool` class. Do not add a ribbon reference unless the prompt explicitly asks for a second access point.

```xml
<!-- The editor discovers this tool in Create Features through categoryRefID. -->
<controls>
  <tool
    id="AcmeAddIn_PointConstructionTool"
    categoryRefID="esri_editing_construction_point"
    caption="Create point"
    className="PointConstructionTool"
    keytip="CP"
    loadOnClick="true"
    smallImage="GenericButtonRed16"
    largeImage="GenericButtonRed32">
    <tooltip heading="Create point">Create a point from the current feature template.<disabledText /></tooltip>
    <!-- Generate a unique GUID; feature templates use it to identify this tool. -->
    <content guid="GENERATE-A-NEW-GUID" />
  </tool>
</controls>
```

`GENERATE-A-NEW-GUID` is an instruction token, not valid output. Replace it with a newly generated GUID before writing or validating DAML. Select the category that matches the constructed item:

| Purpose | `categoryRefID` |
| --- | --- |
| Point | `esri_editing_construction_point` |
| Multipoint | `esri_editing_construction_multipoint` |
| Polyline | `esri_editing_construction_polyline` |
| Polygon | `esri_editing_construction_polygon` |
| Multipatch | `esri_editing_construction_multipatch` |
| Annotation | `esri_editing_construction_annotation` |
| Dimension | `esri_editing_construction_dimension` |
| Knowledge graph entity | `esri_editing_construction_knowledge_graph_entity` |
| Knowledge graph relationship | `esri_editing_construction_knowledge_graph_relationship` |
| Table row | `esri_editing_construction_table` |

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace AcmeAddIn;

/// <summary>Creates a point using the active feature template.</summary>
internal sealed class PointConstructionTool : MapTool
{
    public PointConstructionTool()
    {
        IsSketchTool = true;
        UseSnapping = true;
        SketchType = SketchGeometryType.Point;
        UsesCurrentTemplate = true;
    }

    protected override Task<bool> OnSketchCompleteAsync(Geometry geometry)
    {
        if (CurrentTemplate == null || geometry == null)
            return Task.FromResult(false);

        // The editor applies the current template's attributes to the new feature.
        var operation = new EditOperation
        {
            Name = $"Create {CurrentTemplate.Layer.Name}",
            SelectNewFeatures = true
        };
        operation.Create(CurrentTemplate, geometry);
        return operation.ExecuteAsync();
    }
}
```

- Match `categoryRefID`, `SketchType`, and the target template geometry. Verify specialized annotation, dimension, multipatch, and knowledge graph behavior against the target SDK.
- Keep `UsesCurrentTemplate=true` for template-driven construction.
- Do not confuse a construction tool with a general ribbon MapTool.

### CustomControl

**Generate:** a `<customControl>` declaration, group reference, `CustomControl` view model, and the shared WPF view/code-behind pair.

```xml
<controls>
  <customControl id="AcmeAddIn_StatusControl"
                 caption="Status"
                 className="StatusControlViewModel"
                 keytip="SS"
                 loadOnClick="true"
                 smallImage="GenericButtonPurple16"
                 largeImage="GenericButtonPurple32">
    <!-- ArcGIS Pro pairs this view with the declared view model. -->
    <content className="StatusControlView" />
    <tooltip heading="Status">Shows the current workflow status.<disabledText /></tooltip>
  </customControl>
</controls>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <customControl refID="AcmeAddIn_StatusControl" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Supplies compact status content for the ArcGIS Pro ribbon.</summary>
internal sealed class StatusControlViewModel : CustomControl
{
    private string _status = "Ready";

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }
}
```

- Keep its WPF layout compact enough for a ribbon slot.
- Do not treat this item as an embeddable map-view control.

### CustomItem

**Generate:** an `esri_customItems` category component, a `CustomItemBase` class, and the context menu/button files required by this complete pattern.

```xml
<categories>
  <updateCategory refID="esri_customItems">
    <insertComponent id="AcmeAddIn_ReportItem" className="ReportItem">
      <!-- Use a unique extension without a leading dot; it must not collide with a Pro or geoprocessing type. -->
      <content displayName="Report"
               fileExtension="acmereport"
               isContainer="false"
               keywords="report"
               contextMenuID="AcmeAddIn_ReportItem_ContextMenu">
        <filterFlags>
          <type id="File" />
        </filterFlags>
      </content>
    </insertComponent>
  </updateCategory>
</categories>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESRI.ArcGIS.ItemIndex;
using System.Windows;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Xml.Linq;

namespace AcmeAddIn;

/// <summary>Represents browsable .acmereport files in the ArcGIS Pro catalog.</summary>
internal class ReportItem : CustomItemBase
{
    protected ReportItem()
    {
    }

    protected ReportItem(ItemInfoValue itemInfo)
        : base(EnableCatalogBrowsing(itemInfo))
    {
    }

    private static ItemInfoValue EnableCatalogBrowsing(ItemInfoValue itemInfo)
    {
        itemInfo.browseDialogOnly = "FALSE";
        return itemInfo;
    }

    public override bool IsContainer => false;

    public override ImageSource LargeImage =>
        (ImageSource)Application.Current.Resources["BexDog32"];

    public override Task<ImageSource> SmallImage => Task.FromResult(
        (ImageSource)Application.Current.Resources["BexDog16"]);
}

/// <summary>Shows the selected custom report item's catalog name.</summary>
internal sealed class ShowReportNameButton : Button
{
    protected override void OnClick()
    {
        var report = Project.GetCatalogPane()?.SelectedItems
            .OfType<ReportItem>()
            .FirstOrDefault();
        if (report == null)
            return;

        ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
            $"Selected report: {report.Name}",
            "Report");
    }
}
```

```xml
<!-- Add these module-local records to the existing insertModule sections. -->
<controls>
  <button id="AcmeAddIn_ReportItem_ShowNameButton"
          caption="Show report name"
          className="ShowReportNameButton"
          keytip="RN"
          loadOnClick="true"
          smallImage="GenericButtonBlue16"
          largeImage="GenericButtonBlue32">
    <tooltip heading="Show report name">Show the selected report name.<disabledText /></tooltip>
  </button>
</controls>
<menus>
  <menu id="AcmeAddIn_ReportItem_ContextMenu" caption="Report">
    <button refID="AcmeAddIn_ReportItem_ShowNameButton" />
  </menu>
</menus>
```

- Keep the context-menu button declaration, `refID`, and `ShowReportNameButton` class synchronized.
- Keep DAML `isContainer` equal to C# `IsContainer`.
- If `IsContainer=true`, implement `Fetch()` and create child items derived from `CustomItemBase`.
- A CustomItem is browsed external content. Use CustomProjectItem when the item must persist in the `.aprx`.

### CustomProjectItem

**Generate:** the custom item registration, `CustomProjectItemBase` class, context menu/button, and the complete CustomProjectItemContainer pattern in the next section. A project item without its registered custom project item container is incomplete.

```xml
<categories>
  <updateCategory refID="esri_customItems">
    <!-- containerType links this item to the registered project container type. -->
    <insertComponent id="AcmeAddIn_WorkOrderItem"
                     className="WorkOrderItem"
                     containerType="WorkOrderItemContainer">
      <content displayName="Work order"
               fileExtension="acmeworkorder"
               isContainer="false"
               keywords="work order"
               contextMenuID="AcmeAddIn_WorkOrderItem_ContextMenu">
        <filterFlags>
          <type id="File" />
        </filterFlags>
      </content>
    </insertComponent>
  </updateCategory>
</categories>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESRI.ArcGIS.ItemIndex;
using System.Windows;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Xml.Linq;

namespace AcmeAddIn;

/// <summary>Represents a work order that is persisted in the current ArcGIS Pro project.</summary>
internal class WorkOrderItem : CustomProjectItemBase
{
    protected WorkOrderItem()
    {
    }

    protected WorkOrderItem(ItemInfoValue itemInfo)
        : base(EnableCatalogBrowsing(itemInfo))
    {
    }

    private static ItemInfoValue EnableCatalogBrowsing(ItemInfoValue itemInfo)
    {
        itemInfo.browseDialogOnly = "FALSE";
        return itemInfo;
    }

    public override bool IsContainer => false;

    public override ImageSource LargeImage =>
        (ImageSource)Application.Current.Resources["BexDog32"];

    public override Task<ImageSource> SmallImage => Task.FromResult(
        (ImageSource)Application.Current.Resources["BexDog16"]);

    public override ProjectItemInfo OnGetInfo() => new()
    {
        Name = Name,
        Path = Path,
        // This value must match the container's DAML content type.
        Type = WorkOrderItemContainer.ContainerTypeId
    };
}
```

The context-menu launcher that persists the browsed item must observe its queued work:

```xml
<!-- Add these module-local records to the existing insertModule sections. -->
<controls>
  <button id="AcmeAddIn_WorkOrderItem_AddToProjectButton"
          caption="Add to project"
          className="AddWorkOrderToProjectButton"
          keytip="AP"
          loadOnClick="true"
          smallImage="GenericButtonBlue16"
          largeImage="GenericButtonBlue32">
    <tooltip heading="Add to project">Persist the selected work order in this project.<disabledText /></tooltip>
  </button>
</controls>
<menus>
  <menu id="AcmeAddIn_WorkOrderItem_ContextMenu" caption="Work order">
    <button refID="AcmeAddIn_WorkOrderItem_AddToProjectButton" />
  </menu>
</menus>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESRI.ArcGIS.ItemIndex;
using System.Windows;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Xml.Linq;

namespace AcmeAddIn;

/// <summary>Adds the selected work order to the current ArcGIS Pro project.</summary>
internal sealed class AddWorkOrderToProjectButton : Button
{
    protected override async void OnClick()
    {
        var project = Project.Current;
        var catalogPane = Project.GetCatalogPane();
        var item = catalogPane?.SelectedItems.OfType<WorkOrderItem>().FirstOrDefault();
        if (item == null || project == null)
            return;

        try
        {
            // Project item mutation must run on the Main CIM Thread.
            await QueuedTask.Run(() => project.AddItem(item));
        }
        catch (Exception exception)
        {
            // This top-level UI handler must observe and report failures from async work.
            ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show(
                exception.Message,
                "Add work order to project");
        }
    }
}
```

- Apply the CustomItem extension, context menu, filter, image, and `IsContainer` checks as well.
- Do not abandon `QueuedTask.Run`; await it from the top-level button handler and handle expected failures on the UI thread.

#### CustomProjectItemContainer

**Generate:** a second category registration and a class deriving from `CustomProjectItemContainer<TProjectItem>`.

```xml
<categories>
  <updateCategory refID="esri_core_projectContainers">
    <insertComponent id="AcmeAddIn_WorkOrderItem_Container"
                     className="WorkOrderItemContainer"
                     insert="before"
                     placeWith="esri_geodatabase_folderConnectionContainer">
      <!-- type is the persistence contract used by OnGetInfo(). -->
      <content type="WorkOrderItemContainer"
               displayName="Work orders"
               contextMenu="AcmeAddIn_WorkOrderContainer_ContextMenu" />
    </insertComponent>
  </updateCategory>
</categories>

<!-- Add this module-local menu to the existing insertModule menus section. -->
<menus>
  <menu id="AcmeAddIn_WorkOrderContainer_ContextMenu" caption="Work orders">
    <button refID="esri_folderConnectionAddButton" />
  </menu>
</menus>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESRI.ArcGIS.ItemIndex;
using System.Windows;
using System.Windows.Media;
using System.Windows.Media.Imaging;

namespace AcmeAddIn;

/// <summary>Hosts persisted work-order items in the ArcGIS Pro catalog tree.</summary>
internal class WorkOrderItemContainer : CustomProjectItemContainer<WorkOrderItem>
{
    // Keep this value synchronized with DAML content type and WorkOrderItem.OnGetInfo().Type.
    internal const string ContainerTypeId = "WorkOrderItemContainer";

    public WorkOrderItemContainer()
        : base(ContainerTypeId)
    {
    }

    public override Item CreateItem(string name, string path, string containerType, string data)
    {
        var item = ItemFactory.Instance.Create(path);
        if (item is WorkOrderItem workOrder)
            Add(workOrder);

        return item;
    }
}
```

Keep all of these synchronized:

- Item DAML `containerType`.
- Container C# type and generic argument.
- Container DAML `className`.
- Container DAML `<content type>`.
- Container identifier passed to `base(...)`.
- Project item's `OnGetInfo().Type`.
- Both context menu IDs and declarations.
- Verify `esri_core_projectContainers`, `esri_geodatabase_folderConnectionContainer`, and `esri_folderConnectionAddButton` for the target SDK version before using the category, placement anchor, and container command.

### Dockpane

**Generate:** `<dockPane>`, a `DockPane` view model, the shared WPF view/code-behind pair, and an ordinary companion Button with a visible placement.

```xml
<dockPanes>
  <dockPane id="AcmeAddIn_ParcelDockpane"
            caption="Parcels"
            className="ParcelDockpaneViewModel"
            smallImage="BexDog16"
            dock="group"
            dockWith="esri_core_projectDockPane">
    <content className="ParcelDockpaneView" />
  </dockPane>
</dockPanes>

<controls>
  <!-- This ordinary button is the initial user-accessible path to the dockpane. -->
  <button id="AcmeAddIn_ShowParcelDockpaneButton"
          caption="Show parcels"
          className="ShowParcelDockpaneButton"
          keytip="SP"
          loadOnClick="true"
      smallImage="GenericButtonPurple16"
      largeImage="GenericButtonPurple32">
    <tooltip heading="Show parcels">Open the Parcels dockpane.<disabledText /></tooltip>
  </button>
</controls>

<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <button refID="AcmeAddIn_ShowParcelDockpaneButton" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Supplies state and commands for the Parcels dockpane.</summary>
internal class ParcelDockpaneViewModel : DockPane
{
    // This lookup ID must exactly match the dockPane declaration in Config.daml.
    private const string DockpaneId = "AcmeAddIn_ParcelDockpane";

    protected ParcelDockpaneViewModel()
    {
    }

    internal static void Show()
    {
        FrameworkApplication.DockPaneManager.Find(DockpaneId)?.Activate();
    }
}

/// <summary>Provides the initial user-accessible entry point for the Parcels dockpane.</summary>
internal sealed class ShowParcelDockpaneButton : Button
{
    protected override void OnClick() => ParcelDockpaneViewModel.Show();
}
```

- Declare and place the show button unless another explicit invocation path already exists.
- Registration alone does not initially show a dockpane.
- Use `BexDog16` for the dockpane `smallImage` and `GenericButtonPurple16` / `GenericButtonPurple32` for its companion show button.
- Verify `dockWith` exists for the target version and that the requested docking relationship is appropriate.

### Dockpane with Burger Button

**Generate:** the complete Dockpane pattern plus a context menu, its ordinary Button items, two view-model properties, and a `BurgerButton` in the dockpane header.

```xml
<controls>
  <button id="AcmeAddIn_ParcelDockpane_Refresh"
          caption="Refresh"
          className="RefreshParcelDockpaneButton"
          keytip="PR"
          loadOnClick="true" />
</controls>
<menus>
  <menu id="AcmeAddIn_ParcelDockpane_Menu"
        caption="Options"
        contextMenu="true">
    <button refID="AcmeAddIn_ParcelDockpane_Refresh" />
  </menu>
</menus>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Supplies state, commands, and a header menu for the Parcels dockpane.</summary>
internal class ParcelDockpaneViewModel : DockPane
{
    // These IDs must exactly match the dockPane and context menu declarations in Config.daml.
    private const string DockpaneId = "AcmeAddIn_ParcelDockpane";
    private const string MenuId = "AcmeAddIn_ParcelDockpane_Menu";
    private string _heading = "Parcels";

    protected ParcelDockpaneViewModel()
    {
    }

    public string BurgerButtonTooltip => "Options";

    public System.Windows.Controls.ContextMenu BurgerButtonMenu =>
        FrameworkApplication.CreateContextMenu(MenuId);

    public string Heading
    {
        get => _heading;
        set => SetProperty(ref _heading, value);
    }

    internal static void Show()
    {
        FrameworkApplication.DockPaneManager.Find(DockpaneId)?.Activate();
    }

    internal static void Refresh()
    {
        if (FrameworkApplication.DockPaneManager.Find(DockpaneId) is ParcelDockpaneViewModel viewModel)
            viewModel.Heading = $"Parcels refreshed at {DateTime.Now:T}";
    }
}

/// <summary>Provides the initial ribbon entry point for the Parcels dockpane.</summary>
internal sealed class ShowParcelDockpaneButton : Button
{
    protected override void OnClick() => ParcelDockpaneViewModel.Show();
}

/// <summary>Refreshes content through the Parcels dockpane burger menu.</summary>
internal sealed class RefreshParcelDockpaneButton : Button
{
    protected override void OnClick() => ParcelDockpaneViewModel.Refresh();
}
```

```xaml
<DockPanel Height="30" LastChildFill="True">
  <!-- PopupMenu is created from the DAML menu ID exposed by the view model. -->
  <controls:BurgerButton DockPanel.Dock="Right"
                         ToolTip="{Binding BurgerButtonTooltip}"
                         PopupMenu="{Binding BurgerButtonMenu}" />
  <TextBlock Text="{Binding Heading}"
             Style="{DynamicResource Esri_TextBlockDockPaneHeader}" />
</DockPanel>
```

- Add `xmlns:controls="clr-namespace:ArcGIS.Desktop.Framework.Controls;assembly=ArcGIS.Desktop.Framework"` to the view.
- Keep the show button, burger menu item button, dockpane ID, and menu ID distinct.
- The menu is declarative; each custom actionable menu item is a declared/code-backed Button.

### Pro Window

**Generate:** a regular `<button>` declaration and group placement, a `Button` class that creates and owns an ArcGIS Pro `ProWindow`, and the WPF window XAML/code-behind pair. A ProWindow is a top-level window, not a dockpane or pane.

```xml
<!-- Add these module-local records to the existing insertModule sections. -->
<controls>
  <button id="AcmeAddIn_ShowSettingsWindowButton"
          caption="Show settings"
          className="ShowSettingsWindowButton"
          keytip="SW"
          loadOnClick="true"
          smallImage="GenericButtonPurple16"
          largeImage="GenericButtonPurple32">
    <tooltip heading="Show settings">Open the settings window.<disabledText /></tooltip>
  </button>
</controls>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <button refID="AcmeAddIn_ShowSettingsWindowButton" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Opens one modeless settings window owned by the ArcGIS Pro main window.</summary>
internal sealed class ShowSettingsWindowButton : Button
{
    private SettingsProWindow _window;

    protected override void OnClick()
    {
        if (_window != null)
            return;

        _window = new SettingsProWindow
        {
            Owner = FrameworkApplication.Current.MainWindow
        };
        _window.Closed += (_, _) => _window = null;
        _window.Show();
    }
}
```

```xaml
<controls:ProWindow x:Class="AcmeAddIn.SettingsProWindow"
    xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
    xmlns:controls="clr-namespace:ArcGIS.Desktop.Framework.Controls;assembly=ArcGIS.Desktop.Framework"
    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
    xmlns:d="http://schemas.microsoft.com/expression/blend/2008"
    xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
    xmlns:extensions="clr-namespace:ArcGIS.Desktop.Extensions;assembly=ArcGIS.Desktop.Extensions"
    mc:Ignorable="d"
    Title="Settings"
    Height="300"
    Width="400"
    WindowStartupLocation="CenterOwner">
  <controls:ProWindow.Resources>
    <ResourceDictionary>
      <ResourceDictionary.MergedDictionaries>
        <extensions:DesignOnlyResourceDictionary
          Source="pack://application:,,,/ArcGIS.Desktop.Framework;component/Themes/Default.xaml" />
      </ResourceDictionary.MergedDictionaries>
    </ResourceDictionary>
  </controls:ProWindow.Resources>
  <Grid>
    <!-- Add the window's workflow-specific controls here. -->
  </Grid>
</controls:ProWindow>
```

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace AcmeAddIn;

/// <summary>Provides the Settings window content hosted by ArcGIS Pro.</summary>
public partial class SettingsProWindow : ArcGIS.Desktop.Framework.Controls.ProWindow
{
    public SettingsProWindow()
    {
        InitializeComponent();
    }
}
```

- Keep the button declaration, class name, and group `refID` synchronized. The ProWindow itself is instantiated by the button and has no DAML item declaration.
- Set `Owner` to `FrameworkApplication.Current.MainWindow` and center the window on its owner so it behaves as part of ArcGIS Pro.
- Retain the window instance while it is open to prevent duplicate modeless windows; clear the reference in `Closed` so the command can reopen it.
- Replace `Show()` with `ShowDialog()` only when the requested workflow requires a modal window.
- Use `ArcGIS.Desktop.Framework.Controls.ProWindow`, not `System.Windows.Window`, and include the ArcGIS Pro theme resources in the XAML.

### Drop Handler

**Generate:** a root-level `<dropHandlers>` registration and a class deriving from `DropHandlerBase`. Drop handlers do not use a ribbon declaration or placement; `dataTypes` determines the kinds of dropped data routed to the handler.

```xml
<!-- This registration belongs at the ArcGIS root, outside <modules>. -->
<dropHandlers>
  <insertHandler id="AcmeAddIn_DropHandler"
         className="DropHandler"
         dataTypes="*" />
</dropHandlers>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.DragDrop;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace AcmeAddIn;

/// <summary>Accepts dropped data and reports the values handled by this add-in.</summary>
internal sealed class DropHandler : DropHandlerBase
{
  public override void OnDragOver(DropInfo dropInfo)
  {
    dropInfo.Effects = dropInfo.Items.Any(item => item.Data != null)
      ? DragDropEffects.Copy
      : DragDropEffects.None;
  }

  public override void OnDrop(DropInfo dropInfo)
  {
    var droppedValues = dropInfo.Items
      .Select(item => item.Data?.ToString())
      .Where(value => !string.IsNullOrWhiteSpace(value))
      .ToArray();
    if (droppedValues.Length == 0)
      return;

    // Mark the drop handled only after this handler has processed its data.
    dropInfo.Handled = true;
    foreach (var value in droppedValues)
      System.Diagnostics.Debug.WriteLine($"Dropped data: {value}");
  }
}
```

- Keep the root `<insertHandler>` `id` unique and its `className` synchronized with the implementation class; do not place it inside `<insertModule>`.
- Use `dataTypes="*"` to receive all drop types or a pipe-delimited list of file extensions, such as `dataTypes=".xls|.xlsx"`, to narrow routing.
- Set `DropInfo.Effects` in `OnDragOver()` to advertise supported operations. Set `DropInfo.Handled` in `OnDrop()` only after the handler has actually processed the dropped data.
- Replace the debugger output with the requested workflow. Run ArcGIS Pro API operations on their required thread; do not assume a drop callback is on the Main CIM Thread.

### Embeddable Control

**Generate:** an `esri_embeddableControls` category component, an `EmbeddableControl` view model, and the shared WPF view/code-behind pair. Also wire a consuming MapTool when the prompt asks to display the control.

```xml
<categories>
  <updateCategory refID="esri_embeddableControls">
    <insertComponent id="AcmeAddIn_SketchOptionsControl"
                     className="SketchOptionsControlViewModel">
      <content className="SketchOptionsControlView" />
    </insertComponent>
  </updateCategory>
</categories>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Controls;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AcmeAddIn;

/// <summary>Provides options displayed over the map while the owning tool is active.</summary>
internal class SketchOptionsControlViewModel : EmbeddableControl
{
    // ArcGIS Pro supplies serialized options and whether the user may change them.
    public SketchOptionsControlViewModel(XElement options, bool canChangeOptions)
        : base(options, canChangeOptions)
    {
    }
}
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Hosts the sketch options embeddable control while this map tool is active.</summary>
internal class ParcelSketchTool : MapTool
{
    public ParcelSketchTool()
    {
        IsSketchTool = true;
        SketchType = SketchGeometryType.Rectangle;
        SketchOutputMode = SketchOutputMode.Map;

        // This ID resolves the component registered in esri_embeddableControls.
        OverlayControlID = "AcmeAddIn_SketchOptionsControl";
        OverlayControlCanResize = true;
        OverlayControlPositionRatio = new System.Windows.Point(0, 0);
    }
}
```

- Use `SketchTipID` instead of `OverlayControlID` only when the requested purpose is sketch-tip presentation.
- Registration without a consuming tool is valid but does not display the control by itself.
- Keep position-ratio coordinates in the supported range from 0 through 1.

### Gallery

**Generate:** a `<gallery>`, group reference, `Gallery` class, and XAML `ResourceDictionary` containing the referenced `DataTemplate`.

```xml
<galleries>
  <gallery id="AcmeAddIn_BasemapGallery"
           caption="Basemaps"
           className="BasemapGallery"
           keytip="BM"
           itemsInRow="3"
           dataTemplateFile="pack://application:,,,/AcmeAddIn;component/BasemapGalleryTemplate.xaml"
           templateID="BasemapGalleryItemTemplate"
           resizable="true"
           largeImage="GenericButtonOrange32">
    <tooltip heading="Basemaps">Choose a basemap.<disabledText /></tooltip>
  </gallery>
</galleries>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <gallery refID="AcmeAddIn_BasemapGallery" inline="false" size="large" />
  </group>
</groups>
```

```xaml
<ResourceDictionary
    xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml">
  <!-- This key must exactly match the DAML templateID. -->
  <DataTemplate x:Key="BasemapGalleryItemTemplate">
    <StackPanel Margin="5">
      <Image Source="{Binding Icon}" MaxWidth="32" MaxHeight="32" />
      <TextBlock Text="{Binding Text}" HorizontalAlignment="Center" />
    </StackPanel>
  </DataTemplate>
</ResourceDictionary>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media;

namespace AcmeAddIn;

/// <summary>Provides a dropdown collection of basemap choices.</summary>
internal class BasemapGallery : Gallery
{
    private bool _isInitialized;

    protected override void OnDropDownOpened()
    {
        if (_isInitialized)
            return;

        // Gallery items become WPF content and must be created on the UI thread.
        Add(new GalleryItem("Streets", null, "Streets"));
        _isInitialized = true;
    }
}
```

- Gallery content is programmatic. Do not put child Button references in the gallery declaration.
- Validate assembly name and path in `dataTemplateFile`, XAML `Page` inclusion, `templateID`/`x:Key`, and every bound `GalleryItem` property.
- When the request specifies item-selection behavior, implement `OnClick(GalleryItem)` under `arcgis-pro-sdk-best-practices.instructions.md`. Do not emit a TODO-only or empty override.

### Inline Gallery

**Generate:** the same declaration, resource dictionary, code file, and base class as Gallery, renamed consistently for the requested content. The defining difference is the group placement. Do not emit the placement below unless `AcmeAddIn_SymbolGallery` has the complete Gallery declaration/template contract above.

```xml
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <!-- inline=true expands the gallery directly on the ribbon. -->
    <gallery refID="AcmeAddIn_SymbolGallery" inline="true" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media;

namespace AcmeAddIn;

/// <summary>Provides symbol choices expanded directly on the ribbon.</summary>
internal class SymbolGallery : Gallery
{
    public SymbolGallery()
    {
        // Constructor population is appropriate only for context-independent UI-thread data.
        Add(new GalleryItem("Circle", null, "Circle"));
    }
}
```

- Use `itemsInRow` and a compact fixed-size template suitable for ribbon display.
- Keep the same template URI/key and UI-thread checks as a dropdown Gallery.
- Do not invent an `InlineGallery` base class; both variants derive from `Gallery`.

### LayoutTool

**Generate:** a `<tool>` declaration, a ribbon group reference, and a class deriving from `LayoutTool`.

```xml
<controls>
  <tool
    id="AcmeAddIn_SelectLayoutElementsTool"
    caption="Select elements"
    className="SelectLayoutElementsTool"
    keytip="SE"
    loadOnClick="true"
    condition="esri_layouts_layoutPane"
    smallImage="GenericButtonRed16"
    largeImage="GenericButtonRed32">
    <tooltip heading="Select elements">Sketch a box around layout elements.<disabledText /></tooltip>
  </tool>
</controls>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <tool refID="AcmeAddIn_SelectLayoutElementsTool" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Selects layout elements using a rectangular sketch.</summary>
internal sealed class SelectLayoutElementsTool : LayoutTool
{
    public SelectLayoutElementsTool()
    {
        SketchType = SketchGeometryType.Rectangle;
    }
}
```

- Use the layout-pane condition unless the requested tool has a verified broader context.
- Use `LayoutTool`, not `MapTool`, for layout canvas interaction.
- Implement only the lifecycle or sketch override required by the requested layout workflow under `arcgis-pro-sdk-best-practices.instructions.md`; do not emit a TODO-only or empty override.

### LayoutTrayButton

**Generate:** an `esri_layouts_LayoutTrayButtons` category component and a `LayoutTrayButton` class. Add popup WPF files only for `PopupToggleButton`.

```xml
<categories>
  <updateCategory refID="esri_layouts_LayoutTrayButtons">
    <insertComponent id="AcmeAddIn_LayoutGuidesTrayButton"
                     className="LayoutGuidesTrayButton">
      <content L_name="Layout guides"
               largeImage="GenericButtonGreen32"
               smallImage="GenericButtonGreen16"
               L_tooltipHeading="Layout guides"
               L_tooltip="Toggle layout guides." />
    </insertComponent>
  </updateCategory>
</categories>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Core.Internal.CIM;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace AcmeAddIn;

/// <summary>Toggles layout guides from the active layout view tray.</summary>
internal sealed class LayoutGuidesTrayButton : LayoutTrayButton
{
    protected override void Initialize()
    {
        base.Initialize();
        ButtonType = TrayButtonType.ToggleButton;
    }
}
```

- For `TrayButtonType.Button`, assign `ClickCommand = new RelayCommand(DoClick)` during `Initialize()`.
- For `TrayButtonType.ToggleButton`, override `OnButtonChecked()` only when implementing the requested toggle behavior; no popup files are required. Do not emit a TODO-only or empty override.
- For `TrayButtonType.PopupToggleButton`, follow the shared popup pattern under MapTrayButton.
- Do not add a ribbon group reference; category registration determines tray placement.

### MapTool

**Generate:** a `<tool>` control declaration, group reference, and a class deriving from `MapTool`.

```xml
<controls>
  <tool
    id="AcmeAddIn_SelectParcelsTool"
    caption="Select parcels"
    className="SelectParcelsTool"
    keytip="PS"
    loadOnClick="true"
    condition="esri_mapping_mapPane"
    smallImage="GenericButtonRed16"
    largeImage="GenericButtonRed32">
    <tooltip heading="Select parcels">Sketch a box to select parcels.<disabledText /></tooltip>
  </tool>
</controls>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <tool refID="AcmeAddIn_SelectParcelsTool" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Selects parcels intersecting a rectangular map sketch.</summary>
internal sealed class SelectParcelsTool : MapTool
{
    public SelectParcelsTool()
    {
        IsSketchTool = true;
        SketchType = SketchGeometryType.Rectangle;
        // Use Map for 2D map coordinates; verify Screen mode for the requested 3D workflow.
        SketchOutputMode = SketchOutputMode.Map;
    }

    protected override async Task<bool> OnSketchCompleteAsync(Geometry geometry)
    {
        var mapView = MapView.Active;
        if (mapView == null || geometry == null)
            return false;

        // Capture the view on the UI thread, then perform map selection on the MCT.
        await QueuedTask.Run(() => mapView.SelectFeatures(geometry));
        return true;
    }
}
```

- A feature selection tool is a MapTool structure plus the requested selection workflow implemented under `arcgis-pro-sdk-best-practices.instructions.md`.
- If “edit tool” or “sketch tool” does not reveal whether the tool belongs in Create Features and consumes the current feature template, ask before choosing between MapTool and ConstructionTool.
- Set `IsSketchTool=true` for left-click sketch interaction and choose the correct `SketchType` and output mode.
- Add editing flags such as `UsesCurrentTemplate` only when the requested workflow requires them.
- Do not add construction `categoryRefID`/GUID wiring unless this is a Create Features construction tool.

### MapTrayButton

**Generate:** an `esri_mapping_MapTrayButtons` category component and a `MapTrayButton` class. Choose exactly one tray-button mode.

```xml
<categories>
  <updateCategory refID="esri_mapping_MapTrayButtons">
    <insertComponent id="AcmeAddIn_MapOverlayTrayButton"
                     className="MapOverlayTrayButton">
      <content L_name="Map overlay"
               largeImage="GenericButtonRed32"
               smallImage="GenericButtonRed16"
               L_tooltipHeading="Map overlay"
               L_tooltip="Control the map overlay." />
    </insertComponent>
  </updateCategory>
</categories>
```

Minimum button mode:

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace AcmeAddIn;

/// <summary>Runs the overlay command from the active map view tray.</summary>
internal sealed class MapOverlayTrayButton : MapTrayButton
{
    protected override void Initialize()
    {
        base.Initialize();
        ButtonType = TrayButtonType.Button;
        ClickCommand = new RelayCommand(DoClick);
    }

    private void DoClick()
    {
        // Replace this demonstration with the requested UI-thread-safe command behavior.
        MessageBox.Show("Map overlay command invoked.", "Map overlay");
    }
}
```

When button-mode behavior is specified, replace the demonstration message with that implementation under `arcgis-pro-sdk-best-practices.instructions.md`. Do not leave `DoClick()` empty or TODO-only.

Popup-toggle mode replaces the minimum button class with a complete popup-capable `MapTrayButton` and adds a popup view model plus view/code-behind pair:

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace AcmeAddIn;

/// <summary>Hosts popup controls in the active map view tray.</summary>
internal class MapOverlayTrayButton : MapTrayButton
{
    private MapOverlayPopupViewModel _popupViewModel;
    private bool _isSubscribed;

    protected override void Initialize()
    {
        base.Initialize();
        ButtonType = TrayButtonType.PopupToggleButton;
    }

    protected override void OnButtonChecked()
    {
        if (_popupViewModel != null && _popupViewModel.IsChecked != IsChecked)
            _popupViewModel.IsChecked = IsChecked;
    }

    protected override ContentControl ConstructPopupContent()
    {
        _popupViewModel = new MapOverlayPopupViewModel
        {
            Heading = Name,
            IsChecked = IsChecked,
            CanPopupAutoClose = CanAutoClose
        };

        // Tray popups assign their runtime DataContext explicitly.
        return new MapOverlayPopupView { DataContext = _popupViewModel };
    }

    protected override void OnShowPopup()
    {
        base.OnShowPopup();
        if (_popupViewModel == null || _isSubscribed)
            return;

        _popupViewModel.PropertyChanged += PopupViewModelPropertyChanged;
        _isSubscribed = true;
    }

    protected override void OnHidePopup()
    {
        if (_popupViewModel != null && _isSubscribed)
        {
            _popupViewModel.PropertyChanged -= PopupViewModelPropertyChanged;
            _isSubscribed = false;
        }

        base.OnHidePopup();
    }

    private void PopupViewModelPropertyChanged(
        object sender,
        System.ComponentModel.PropertyChangedEventArgs eventArgs)
    {
        if (_popupViewModel == null)
            return;

        if (eventArgs.PropertyName == nameof(MapOverlayPopupViewModel.IsChecked))
            SetCheckedNoCallback(_popupViewModel.IsChecked);
        else if (eventArgs.PropertyName == nameof(MapOverlayPopupViewModel.CanPopupAutoClose))
            CanAutoClose = _popupViewModel.CanPopupAutoClose;
    }
}
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace AcmeAddIn;

/// <summary>Supplies bindable state for the map tray popup.</summary>
internal class MapOverlayPopupViewModel : PropertyChangedBase
{
    private string _heading = "Map overlay";
    private bool _isChecked;
    private bool _canPopupAutoClose;

    public string Heading
    {
        get => _heading;
        set => SetProperty(ref _heading, value);
    }

    public bool IsChecked
    {
        get => _isChecked;
        set => SetProperty(ref _isChecked, value);
    }

    public bool CanPopupAutoClose
    {
        get => _canPopupAutoClose;
        set => SetProperty(ref _canPopupAutoClose, value);
    }
}
```

```xaml
<UserControl x:Class="AcmeAddIn.MapOverlayPopupView"
    xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
    xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
    xmlns:d="http://schemas.microsoft.com/expression/blend/2008"
    xmlns:local="clr-namespace:AcmeAddIn"
    mc:Ignorable="d"
    d:DataContext="{d:DesignInstance Type=local:MapOverlayPopupViewModel, IsDesignTimeCreatable=False}">
  <StackPanel Margin="8" MinWidth="180">
    <CheckBox Content="Overlay enabled"
              IsChecked="{Binding IsChecked, Mode=TwoWay}" />
    <CheckBox Content="Close popup automatically"
              IsChecked="{Binding CanPopupAutoClose, Mode=TwoWay}" />
    <TextBlock Margin="0,8,0,0" Text="{Binding Heading}" />
  </StackPanel>
</UserControl>
```

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace AcmeAddIn;

/// <summary>Visual content hosted by the map tray popup.</summary>
public partial class MapOverlayPopupView : UserControl
{
    public MapOverlayPopupView()
    {
        InitializeComponent();
    }
}
```

- Synchronize popup `IsChecked` with `SetCheckedNoCallback` to avoid recursive callbacks.
- Subscribe to popup view-model changes in `OnShowPopup()` and unsubscribe in `OnHidePopup()`.
- Synchronize `CanPopupAutoClose` when the popup hosts controls that open nested popups.
- Every XAML binding must have a matching public view-model property.
- Do not add a ribbon group reference.

### Menu

**Generate:** a declarative `<menu>`, ordinary declarations/classes for custom actionable controls, and a placement or consumer reference. There is no static Menu class.

```xml
<controls>
  <button id="AcmeAddIn_ExportPdfButton"
          caption="Export PDF"
          className="ExportPdfButton"
          keytip="EP"
          loadOnClick="true"
          smallImage="GenericButtonGreen16"
          largeImage="GenericButtonGreen32">
    <tooltip heading="Export PDF">Export the active content to PDF.<disabledText /></tooltip>
  </button>
  <button id="AcmeAddIn_ExportImageButton"
          caption="Export image"
          className="ExportImageButton"
          keytip="EI"
          loadOnClick="true"
          smallImage="GenericButtonGreen16"
          largeImage="GenericButtonGreen32">
    <tooltip heading="Export image">Export the active content to an image.<disabledText /></tooltip>
  </button>
  <button id="AcmeAddIn_ExportPackageButton"
          caption="Export package"
          className="ExportPackageButton"
          keytip="EK"
          loadOnClick="true"
          smallImage="GenericButtonGreen16"
          largeImage="GenericButtonGreen32">
    <tooltip heading="Export package">Export the active content as a package.<disabledText /></tooltip>
  </button>
</controls>
<menus>
  <menu id="AcmeAddIn_AdvancedExportMenu" caption="Advanced export">
    <button refID="AcmeAddIn_ExportPackageButton" />
  </menu>
  <menu id="AcmeAddIn_ExportMenu"
        caption="Export"
        smallImage="GenericButtonGreen16"
        largeImage="GenericButtonGreen32">
    <button refID="AcmeAddIn_ExportPdfButton" />
    <button refID="AcmeAddIn_ExportImageButton" separator="true" />
    <!-- inline=false presents this nested menu as a side-pull menu. -->
    <menu refID="AcmeAddIn_AdvancedExportMenu" inline="false" />
  </menu>
</menus>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <menu refID="AcmeAddIn_ExportMenu" size="middle" />
  </group>
</groups>
```

Generate `ExportPdfButton`, `ExportImageButton`, and `ExportPackageButton` with the complete Button C# pattern above. Other valid consumers include:

- Dockpane burger buttons through `FrameworkApplication.CreateContextMenu(menuId)`; set `contextMenu="true"` for that menu.
- Custom item and custom project item `contextMenuID` attributes.
- Custom project container `contextMenu` attributes.
- Parent menus, palettes, and split buttons where supported by the schema.

Validate every custom child control declaration and class. A menu declaration by itself is only a container.

### Pane

**Generate:** a `<pane>`, `ViewStatePane` view model, shared WPF view/code-behind pair, and a companion Button that creates the pane.

```xml
<panes>
  <pane id="AcmeAddIn_WorkViewPane"
        caption="Work view"
        className="WorkViewPaneViewModel"
        smallImage="GenericButtonGreen16"
        defaultTab="esri_mapping_homeTab"
        defaultTool="esri_mapping_navigateTool">
    <content className="WorkViewPaneView" />
  </pane>
</panes>

<controls>
  <!-- This button creates the document-style pane for its first use. -->
  <button id="AcmeAddIn_OpenWorkViewPaneButton"
          caption="Open work view"
          className="OpenWorkViewPaneButton"
          keytip="OW"
          loadOnClick="true"
          smallImage="GenericButtonGreen16"
          largeImage="GenericButtonGreen32">
    <tooltip heading="Open work view">Create a Work view pane.<disabledText /></tooltip>
  </button>
</controls>

<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <button refID="AcmeAddIn_OpenWorkViewPaneButton" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Provides a persisted document-style view alongside map and layout panes.</summary>
internal sealed class WorkViewPaneViewModel : ViewStatePane
{
    // ViewType persists this DAML item identity in the project CIM.
    private const string PaneId = "AcmeAddIn_WorkViewPane";

    public WorkViewPaneViewModel(CIMView view)
        : base(view)
    {
    }

    internal static WorkViewPaneViewModel Create()
    {
        var view = new CIMGenericView { ViewType = PaneId };
        return FrameworkApplication.Panes.Create(PaneId, new object[] { view })
            as WorkViewPaneViewModel;
    }

    public override CIMView ViewState
    {
        get
        {
            _cimView.InstanceID = (int)InstanceID;
            return _cimView;
        }
    }
}

/// <summary>Creates the Work view pane from an accessible ribbon command.</summary>
internal sealed class OpenWorkViewPaneButton : Button
{
    protected override void OnClick()
    {
        WorkViewPaneViewModel.Create();
    }
}
```

- Keep the ordinary open button declaration, placement, and `Create()` call synchronized unless another explicit invocation path already exists.
- A Pane is a document-style persisted view. It is not a DockPane and is not found through `DockPaneManager`.
- Keep DAML pane ID, `CIMGenericView.ViewType`, and `Panes.Create` ID identical.

### Map Pane Impersonation

**Generate:** a `<pane>`, a `TOCMapPaneProviderPane` view model, the shared WPF view/code-behind pair, and a map-context companion Button. The pane uses a `CIMGenericView` to retain the URI of the map it impersonates.

```xml
<panes>
  <pane id="AcmeAddIn_MapPaneImpersonation"
        caption="Map pane impersonation"
        className="MapPaneImpersonationViewModel"
        smallImage="GenericButtonGreen16"
        defaultTab="esri_mapping_homeTab"
        defaultTool="esri_mapping_navigateTool">
    <content className="MapPaneImpersonationView" />
  </pane>
</panes>

<controls>
  <!-- The map-pane condition ensures there is an active map to impersonate. -->
  <button id="AcmeAddIn_OpenMapPaneImpersonationButton"
          caption="Open map pane impersonation"
          className="OpenMapPaneImpersonationButton"
          keytip="MI"
          loadOnClick="true"
          condition="esri_mapping_mapPane"
          smallImage="GenericButtonGreen16"
          largeImage="GenericButtonGreen32">
    <tooltip heading="Open map pane impersonation">Open a pane for the active map.<disabledText /></tooltip>
  </button>
</controls>

<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <button refID="AcmeAddIn_OpenMapPaneImpersonationButton" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Hosts a document pane whose content is the map that was active when it was created.</summary>
internal class MapPaneImpersonationViewModel : TOCMapPaneProviderPane
{
    // This ID must match the pane declaration and CIMGenericView.ViewType in Config.daml.
    private const string PaneId = "AcmeAddIn_MapPaneImpersonation";
    private const string MapUriProperty = "MAPURI";

    public MapPaneImpersonationViewModel(CIMView view)
        : base(view)
    {
        // Set this to true when the pane should dock beneath its impersonated map view.
        _dockUnderMapView = false;
    }

    internal static MapPaneImpersonationViewModel Create(MapView mapView)
    {
        if (mapView?.Map == null)
            return null;

        var view = new CIMGenericView
        {
            ViewType = PaneId,
            ViewProperties = new Dictionary<string, object>
            {
                // InitializeAsync reads this same property to restore the impersonated map.
                [MapUriProperty] = mapView.Map.URI
            }
        };

        var pane = FrameworkApplication.Panes.Create(PaneId, new object[] { view })
            as MapPaneImpersonationViewModel;
        if (pane != null)
            pane.Caption = $"Impersonate {mapView.Map.Name}";

        return pane;
    }

    public override CIMView ViewState
    {
        get
        {
            _cimView.InstanceID = (int)InstanceID;
            return _cimView;
        }
    }

    protected override async Task InitializeAsync()
    {
        var mapUri = ((CIMGenericView)_cimView).ViewProperties[MapUriProperty] as string;
        if (!string.IsNullOrEmpty(mapUri))
            await SetMapURI(mapUri);

        await base.InitializeAsync();
    }
}

/// <summary>Creates a map-pane impersonation view for the active map.</summary>
internal sealed class OpenMapPaneImpersonationButton : Button
{
    protected override void OnClick()
    {
        MapPaneImpersonationViewModel.Create(MapView.Active);
    }
}
```

```xaml
<UserControl x:Class="AcmeAddIn.MapPaneImpersonationView"
    xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
    xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
    xmlns:d="http://schemas.microsoft.com/expression/blend/2008"
    xmlns:local="clr-namespace:AcmeAddIn"
    xmlns:extensions="clr-namespace:ArcGIS.Desktop.Extensions;assembly=ArcGIS.Desktop.Extensions"
    mc:Ignorable="d"
    d:DataContext="{d:DesignInstance Type=local:MapPaneImpersonationViewModel, IsDesignTimeCreatable=False}">
  <UserControl.Resources>
    <ResourceDictionary>
      <ResourceDictionary.MergedDictionaries>
        <extensions:DesignOnlyResourceDictionary
          Source="pack://application:,,,/ArcGIS.Desktop.Framework;component/Themes/Default.xaml" />
      </ResourceDictionary.MergedDictionaries>
    </ResourceDictionary>
  </UserControl.Resources>
  <Grid>
    <StackPanel Orientation="Horizontal" HorizontalAlignment="Center" VerticalAlignment="Center">
      <TextBlock Text="Impersonating:" Style="{DynamicResource Esri_TextBlockRegular}" Margin="0,0,10,0" />
      <!-- MapURI is provided by TOCMapPaneProviderPane for the map assigned with SetMapURI. -->
      <TextBlock Text="{Binding MapURI, Mode=OneWay}" Style="{DynamicResource Esri_TextBlockRegular}" />
    </StackPanel>
  </Grid>
</UserControl>
```

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace AcmeAddIn;

/// <summary>Visual content hosted by the map-pane impersonation view.</summary>
public partial class MapPaneImpersonationView : UserControl
{
    public MapPaneImpersonationView()
    {
        InitializeComponent();
    }
}
```

- Keep the pane ID, `CIMGenericView.ViewType`, and `FrameworkApplication.Panes.Create` ID identical.
- Keep the `MAPURI` view property key synchronized between `Create()` and `InitializeAsync()`; restore the map with `SetMapURI` before completing initialization.
- The launcher requires `condition="esri_mapping_mapPane"` and must pass `MapView.Active`; a normal pane without a map context cannot identify which map to impersonate.
- This is a persisted document-style Pane, not a DockPane. The `TOCMapPaneProviderPane` base class supplies the map-pane content and the `MapURI` binding.

### Property Sheet and Property Page

**Generate:** a declarative property sheet, one or more nested pages, a `Page` view model and shared WPF view/code-behind pair for each page, plus one companion Button to open the sheet.

```xml
<propertySheets>
  <!-- The sheet is framework-created and has no C# class. -->
  <insertSheet id="AcmeAddIn_ParcelPropertySheet"
               caption="Parcel properties"
               hasGroups="true">
    <page id="AcmeAddIn_ParcelGeneralPage"
          caption="General"
          className="ParcelGeneralPageViewModel"
          group="Parcel">
      <content className="ParcelGeneralPageView" />
    </page>
  </insertSheet>
</propertySheets>

<controls>
  <!-- The property sheet is modal and needs an explicit command to open it. -->
  <button id="AcmeAddIn_ShowParcelPropertySheetButton"
          caption="Parcel properties"
          className="ShowParcelPropertySheetButton"
          keytip="PP"
          loadOnClick="true"
          smallImage="GenericButtonPurple16"
          largeImage="GenericButtonPurple32">
    <tooltip heading="Parcel properties">Open the Parcel property sheet.<disabledText /></tooltip>
  </button>
</controls>

<groups>
  <group id="AcmeAddIn_Group"
         caption="Tools"
         keytip="TL"
         appearsOnAddInTab="true">
    <button refID="AcmeAddIn_ShowParcelPropertySheetButton" size="large" />
  </group>
</groups>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AcmeAddIn;

/// <summary>Edits the general values hosted by the Parcel property sheet.</summary>
internal sealed class ParcelGeneralPageViewModel : Page
{
    public string ParcelName => Data[0] as string ?? string.Empty;
}

/// <summary>Opens the modal Parcel property sheet on its General page.</summary>
internal sealed class ShowParcelPropertySheetButton : Button
{
    private const string SheetId = "AcmeAddIn_ParcelPropertySheet";
    private const string PageId = "AcmeAddIn_ParcelGeneralPage";

    protected override void OnClick()
    {
        if (PropertySheet.IsVisible)
            return;

        // The array order is the data contract consumed by each Page view model.
        PropertySheet.ShowDialog(SheetId, PageId, new object[] { "Parcel 1" });
    }
}
```

- Nest each `<page>` inside its owning `<insertSheet>`.
- Keep sheet/page IDs synchronized with `ShowDialog` and keep `Data` indexes synchronized with the supplied array.
- For editable pages, set `IsModified` when a value changes and implement `CommitAsync()` to persist the requested workflow data. Do not generate an empty lifecycle override.
- Property sheets are modal. Do not show one from inside `QueuedTask.Run` or a row-event callback.

### SplitButton

**Generate:** ordinary child Button declarations/classes, a declarative `<splitButton>`, and a group reference. There is no SplitButton C# class.

```xml
<controls>
  <button id="AcmeAddIn_CreatePointButton"
          caption="Create point"
          className="CreatePointButton"
          keytip="PT"
          loadOnClick="true"
          smallImage="GenericButtonBlack16"
          largeImage="GenericButtonBlack32">
    <tooltip heading="Create point">Run the point creation command.<disabledText /></tooltip>
  </button>
  <button id="AcmeAddIn_CreateLineButton"
          caption="Create line"
          className="CreateLineButton"
          keytip="LN"
          loadOnClick="true"
          smallImage="GenericButtonBlack16"
          largeImage="GenericButtonBlack32">
    <tooltip heading="Create line">Run the line creation command.<disabledText /></tooltip>
  </button>
  <button id="AcmeAddIn_CreatePolygonButton"
          caption="Create polygon"
          className="CreatePolygonButton"
          keytip="PG"
          loadOnClick="true"
          smallImage="GenericButtonBlack16"
          largeImage="GenericButtonBlack32">
    <tooltip heading="Create polygon">Run the polygon creation command.<disabledText /></tooltip>
  </button>
</controls>
<splitButtons>
  <!-- The first child is initially active; the last-used command becomes the default. -->
  <splitButton id="AcmeAddIn_CreateSplitButton">
    <button refID="AcmeAddIn_CreatePointButton" />
    <button refID="AcmeAddIn_CreateLineButton" />
    <button refID="AcmeAddIn_CreatePolygonButton" />
  </splitButton>
</splitButtons>
<groups>
  <group id="AcmeAddIn_Group"
         caption="Create"
         keytip="CR"
         appearsOnAddInTab="true">
    <splitButton refID="AcmeAddIn_CreateSplitButton" />
  </group>
</groups>
```

- Generate `CreatePointButton`, `CreateLineButton`, and `CreatePolygonButton` with the complete Button C# pattern above.
- Use a SplitButton for one active command plus a single-column command list.
- Use ButtonPalette for palette presentation and Gallery for programmatically populated visual choices.

### TableConstructionTool

**Generate:** a construction `<tool>` registered only in `esri_editing_construction_table`, a unique content GUID, and a non-sketching `MapTool` class.

```xml
<controls>
  <tool
    id="AcmeAddIn_RowConstructionTool"
    categoryRefID="esri_editing_construction_table"
    caption="Create rows"
    className="RowConstructionTool"
    keytip="RW"
    loadOnClick="true"
    smallImage="GenericButtonRed16"
    largeImage="GenericButtonRed32">
    <tooltip heading="Create rows">Create rows from the current table template.<disabledText /></tooltip>
    <!-- Generate a unique identifier used by editing templates. -->
    <content guid="GENERATE-A-NEW-GUID" />
  </tool>
</controls>
```

```csharp
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.KnowledgeGraph;
using ArcGIS.Desktop.Layouts;
using ArcGIS.Desktop.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace AcmeAddIn;

/// <summary>Creates rows in the table associated with the active editing template.</summary>
internal sealed class RowConstructionTool : MapTool
{
    public RowConstructionTool()
    {
        // A table construction tool has no map-canvas sketch.
        IsSketchTool = false;
        SketchType = SketchGeometryType.None;
        UsesCurrentTemplate = true;
    }

    protected override Task<bool> OnSketchCompleteAsync(Geometry geometry)
    {
        // The Create button invokes this callback with null geometry for a non-sketching table tool.
        if (CurrentTemplate?.StandaloneTable == null)
            return Task.FromResult(false);

        var operation = new EditOperation
        {
            Name = $"Create {CurrentTemplate.StandaloneTable.Name}",
            SelectNewFeatures = false
        };

        // MapTool.CurrentTemplateRows is supplied by the Create Features row-count control.
        for (var index = 0; index < CurrentTemplateRows; index++)
            operation.Create(CurrentTemplate, null);

        return operation.ExecuteAsync();
    }
}
```

- Replace the `GENERATE-A-NEW-GUID` instruction token with a newly generated GUID before writing or validating DAML.
- Keep `IsSketchTool=false`, `SketchType=None`, and the table category together.
- Do not add a normal ribbon reference by default; Create Features discovers the tool through its category.

## Pattern Selection Guardrails

Use this table when a prompt uses informal terminology:

| User intent | Generate |
| --- | --- |
| Run one command from ribbon, menu, or backstage | Button plus the requested placement |
| Handle data dragged into ArcGIS Pro | Root `<dropHandlers>` registration and `DropHandlerBase` implementation |
| Select, identify, navigate, or sketch on a map | MapTool |
| Edit features with a map sketch | MapTool with editing workflow code |
| Create features from an editing template | ConstructionTool |
| Create standalone table rows from a template | TableConstructionTool |
| Interact with a layout canvas | LayoutTool |
| Show persistent side UI | Dockpane plus invocation button |
| Show a persisted document-style custom view | Pane plus creation button |
| Show modal multi-page settings | Property sheet plus property page(s) and invocation button |
| Show compact WPF content on the ribbon | CustomControl |
| Show tool-associated UI over a map | Embeddable Control plus consuming MapTool wiring |
| Show fixed declarative commands in a dropdown | Menu, ButtonPalette, or SplitButton according to interaction |
| Show programmatic visual choices | Gallery or Inline Gallery |
| Add a control to the bottom of a map/layout view | MapTrayButton or LayoutTrayButton |
| Browse an external proprietary file type | CustomItem |
| Persist a proprietary item in an `.aprx` | CustomProjectItem plus CustomProjectItemContainer |

## Validation Mode

After creating or adding item content, validate the newly generated files and DAML records in their target Add-in or Configuration. Do not merely confirm that they build: build a relationship graph from each new record to the existing scaffold it consumes and report concrete creation defects with file locations. Do not expand this procedure into instructions for updating or deleting pre-existing items.

### 1. Project and Version

- Confirm SDK references, target framework, WPF setting, imported ArcGIS Pro build targets, and project type agree with the installed/target ArcGIS Pro version.
- For a Configuration, confirm its `<Configuration>`/`ConfigurationManager` and `PackageType` remain intact.
- Query `EnableDefaultPageItems` and enumerate `Page` items. Detect missing or duplicate XAML items.

### 2. DAML Structure

- Parse `Config.daml` as XML and, when available, validate it against the version-matched framework XSD.
- Confirm every declaration is under the correct module or root section and in schema-valid order.
- Confirm every local `id` is unique.
- Resolve every local `refID`, `contextMenuID`, `contextMenu`, `containerType`, content `type`, `dockWith`, `placeWith`, default tab/tool, category, and condition.
- Verify built-in `esri_` IDs against the target SDK rather than assuming they exist.

### 3. DAML-to-Code

- Resolve every implementation `className` using `defaultNamespace`; require full qualification outside that namespace.
- Confirm each class derives from the item-specific framework base class and has the required constructor shape.
- Confirm hard-coded lookup IDs in dockpanes, panes, burger menus, property sheets, property pages, and embeddable-control consumers exactly match DAML.
- Confirm each construction tool has a unique GUID and matching category/sketch behavior.
- Confirm custom item/container persistence strings and generic types form one consistent contract.

### 4. WPF and Resources

- Match each DAML content view to XAML `x:Class` and public code-behind type.
- Confirm DAML-hosted views rely on framework DataContext wiring and tray popup views receive an explicit DataContext.
- Resolve every binding to a public property or command on the runtime view model.
- Match every gallery `dataTemplateFile` to a packaged XAML `Page` and every `templateID` to its `DataTemplate x:Key`.
- Verify every newly generated item and companion image matches its role in this document and that the image key or packaged image path exists. Reject `AddInDesktop16` and `AddInDesktop32` as item images. Project light/dark conventions must not override the required pattern images.

### 5. Visibility and Invocation

- Confirm ribbon items have typed group references unless category registration or another host supplies placement.
- Confirm dockpanes, panes, and property sheets have a reachable initial invocation path.
- Confirm backstage items use explicit root insert records.
- Confirm category-based construction and tray items are not incorrectly treated as ordinary ribbon controls.
- Confirm registered embeddable controls have a consumer when the requested behavior requires them to appear.

### 6. C# Quality and Runtime Safety

- Apply `arcgis-pro-sdk-best-practices.instructions.md` to workflow code after structural validation.
- Remove abandoned tasks, blocking waits, inappropriate MCT/UI access, no-op overrides, and unreachable template branches. Preserve the complete role-specific template import blocks required by these instructions.
- Treat WPF binding warnings and unresolved DAML lookups as runtime defects even when compilation succeeds.
- Require human-readable comments for non-obvious relationships and customization points.

### 7. Executable Checks

Run the narrowest available checks, then broaden:

```powershell
Write-Host "Inspect implicit WPF inclusion"
dotnet msbuild .\AcmeAddIn.csproj -getProperty:EnableDefaultPageItems
dotnet msbuild .\AcmeAddIn.csproj -getItem:Page

Write-Host "Compile C#, XAML, resources, and the ArcGIS Pro packaging target"
dotnet build .\AcmeAddIn.csproj -c Debug
```

When ArcGIS Pro is available, launch the Add-in or Configuration and exercise each generated placement, activation path, menu, binding, and persistence workflow. A successful build does not prove that a DAML reference resolves at runtime.

## Creation-Time Safeguards

- Add `condition="esri_layouts_layoutPane"` to a LayoutTool intended only for layout views.
- Await `QueuedTask.Run` when adding a custom project item; do not abandon the task.
- Define every property used by tray-popup XAML; never generate an unresolved sample binding.
- Include the complete role-specific using block prescribed by these instructions in every generated C# file.
- Generate a fresh construction-tool GUID rather than copying a sample value.
