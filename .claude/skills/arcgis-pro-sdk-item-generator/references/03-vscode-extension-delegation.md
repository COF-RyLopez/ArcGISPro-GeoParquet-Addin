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

# VS Code Extension Language Model Tool Delegation

## Objective

Prefer the ArcGIS Pro SDK for .NET VS Code extension's language model tool. Use the bundled item-generation workflow only when the tool is unavailable.

## Language Model Tool Contract

- Create-item tool reference: `#tool:createArcGISProItem`

Use VS Code's `#tool:<toolReferenceName>` syntax when referring to a language model tool in these instructions. The tool reference name is `createArcGISProItem`.

The tool owns target-project selection, project eligibility, item-type and item-name collection, implementation confirmation, file generation, and validation. Invoke it without arguments. Do not gather fallback workflow inputs before delegation.

VS Code makes contributed language model tools available when their extension is installed and enabled. Do not explicitly activate the extension, inspect installed extensions, or probe unrelated capabilities.

## Availability Check

Perform this check before the fallback target inspection:

1. Check whether the language model tool #tool:createArcGISProItem is available in the current tool set.
2. If the tool is unavailable, continue with the fallback workflow.
3. If the tool is available, proceed directly to delegated generation.

Do not ask the user to install the extension during this workflow. The bundled skill process remains the authoritative fallback behavior.

## Delegated Generation

1. Invoke #tool:createArcGISProItem without arguments.
2. Let the tool collect and validate its inputs, obtain any required confirmation, and generate the item.
3. After the tool completes successfully, check only its newly generated item and companion images against `./04-item-patterns.md`. Ordinary Buttons require `GenericButtonRed16` / `GenericButtonRed32`; other roles require their specified pattern images. Never accept `AddInDesktop16` or `AddInDesktop32` as an item image.
4. Correct image mismatches only in the newly generated item declarations or resource references and recheck them. This narrowly scoped image correction is not a second item-generation pass: do not run the fallback workflow, recreate files, change unrelated declarations, or change package branding. If the generated files cannot be inspected or corrected, report image validation as blocked rather than declaring generation complete.
5. Stop the skill workflow after image validation succeeds.

## Fallback Rules

Use the bundled skill workflow when:

- The language model tool #tool:createArcGISProItem is not available in the current tool set.
- VS Code reports that the tool is unavailable before execution begins.

Once tool execution begins, do not run the fallback workflow. If the user cancels, the tool fails, or partial files may exist, stop and report the outcome instead of risking duplicate or overlaid item files and DAML records.

## Handoff

Report whether item generation was delegated or the bundled fallback workflow was selected. The delegated tool owns its generation and validation result except for the mandatory post-generation image check above; `./02-generation-and-verification.md` applies to items created by the bundled workflow.