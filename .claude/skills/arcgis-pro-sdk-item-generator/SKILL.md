---
name: arcgis-pro-sdk-item-generator
description: ArcGIS Pro SDK Item Generator. Use when adding a new framework item to an existing Add-in or Managed Configuration project, including buttons, tools, galleries, menus, dockpanes, panes, property pages, custom controls, custom items, project items, backstage tabs, embeddable controls, and map or layout tray buttons.
argument-hint: Provide the target Add-in or Managed Configuration project, item type, item name, placement, and requested behavior.
license: Apache-2.0
metadata:
  author: Esri, Inc.
  version: "1.0.0"
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

# ArcGIS Pro SDK Item Generator

## Purpose

Use this skill to add a new ArcGIS Pro SDK framework item to an existing Add-in or Managed Configuration project. Item templates are not supported in CoreHost or Plugin Datasource projects.

## Workflow

1. Check for the VS Code extension language model tool and delegate item generation when available using [VS Code Extension Language Model Tool Delegation](./references/03-vscode-extension-delegation.md). Validate generated item images before reporting delegated generation complete.
2. If delegation is unavailable, inspect the target and gather missing requirements using [Target and Required Inputs](./references/00-target-and-required-inputs.md). Stop if the target is not an Add-in or Managed Configuration project.
3. Resolve the item pattern, prepare the complete file-and-wiring plan, and obtain explicit implementation confirmation using [Item Plan and Confirmation](./references/01-item-plan-and-confirmation.md).
4. After confirmation, generate the item and run all structural and executable checks using [Generation and Verification](./references/02-generation-and-verification.md).

## Authority

The canonical item declarations, class patterns, required imports, WPF pairings, comments, and validation rules are defined in [Item Patterns](./references/04-item-patterns.md). Apply [ArcGIS Pro SDK Code Examples Guidance](../../instructions/arcgis-pro-sdk-example-resources.instructions.md) to all generated C#, XAML, and DAML. Apply [ArcGIS Pro SDK Best Practices](../../instructions/arcgis-pro-sdk-best-practices.instructions.md) whenever the item implements workflow behavior.

Do not reproduce or improvise an item pattern when the canonical item-pattern reference defines it.

## Required Item Images

Use the exact image keys specified for each item and companion role in [Item Patterns](./references/04-item-patterns.md). Ordinary Buttons use `smallImage="GenericButtonRed16"` and `largeImage="GenericButtonRed32"`; other roles retain their pattern-specific images. These rules override existing project image conventions and apply to both bundled and delegated generation. Never use `AddInDesktop16` or `AddInDesktop32` as an item image, including as a fallback. Do not copy add-in/package branding into item declarations. Validate every newly generated item image against its matching pattern before reporting completion; leave unrelated existing declarations and package branding unchanged.