---
name: arcgis-pro-sdk-pattern-help
description: Provide grounded ArcGIS Pro SDK extensibility-pattern guidance, comparisons, workflow recommendations, limitations, and tradeoffs for Add-in, Configuration, Plugin Datasource, and CoreHost.
user-invocable: false
disable-model-invocation: false
license: Apache-2.0
metadata:
  author: Esri, Inc.
  version: "1.0.0"
---

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

# ArcGIS Pro SDK Pattern Help

## Purpose

Use this skill for ArcGIS Pro SDK .NET pattern-purpose questions involving Add-in, Configuration, Plugin Datasource, or CoreHost. Explain pattern fit and non-fit, comparisons, limitations, operational boundaries, and workflow-to-pattern recommendations.

## Authoritative Sources

Consult the required internal source before answering:

- General overview: [Patterns Summary](./references/patterns-summary.md)
- Comparison or workflow recommendation: [Patterns Decision Guide](./references/patterns-decision-guide.md), supplemented by the summary when useful
- Detailed Add-in question: [Add-in Details](./references/addins-details.md)
- Detailed Configuration question: [Configuration Details](./references/configurations-details.md)
- Detailed Plugin Datasource question: [Plugin Datasource Details](./references/plugin-datasource-details.md)
- Detailed CoreHost question: [CoreHost Details](./references/corehost-details.md)

For a detailed comparison, use the decision guide as the primary source and consult the relevant detail references for specific mechanics. Never cite or identify these internal files in user-facing output.

## Workflow

1. Determine whether the request concerns pattern purpose, fit, limitations, tradeoffs, or pattern selection.
2. Load the source or sources specified above.
3. Preserve these boundaries:
   - Add-in extends ArcGIS Pro UI and workflows.
   - Configuration provides deeper startup behavior and application UI customization.
   - Plugin Datasource integrates custom data formats and is read-only.
   - CoreHost supports standalone applications but is limited to geodatabase and geometry APIs.
   - Discuss patterns as complementary only for Add-in and Configuration, and only when context requires it.
4. For requests containing "more detail", "deeper", "drill down", "details", or equivalent wording, provide concrete mechanics, scope boundaries, and operational behavior from the applicable detail reference.
5. If the request asks for implementation code or an example, return control to the caller so the ArcGIS Pro SDK example-resource and best-practice instructions can govern the response.
6. Delegate requests for external resources or API and feature-domain guidance to `arcgis-pro-sdk-referral`.
7. Delegate explicit requests to create a new extensibility project to `arcgis-pro-sdk-project-creator`.

## Detail Requirements

- For detailed Add-in questions, cover relevant extensibility points such as buttons, tools, menus, galleries, panes, property pages, backstage elements, ProWindow, custom controls, embeddable controls, and DAML condition or state gating.
- For detailed Configuration questions, cover relevant `ConfigurationManager` lifecycle behavior, including `OnUpdateDatabase`, `OnCreateDaml`, `OnShowStartPage`, `OnCreateQuickAccessToolbar`, `OnCanExecuteCommand`, and `OnExecuteCommand`.
- State that Configuration supports the same ArcGIS Pro extensibility points as Add-in, plus deeper startup, start-page, and role-based application UI customization.

## Output

- Answer directly and concisely in Markdown.
- Bold pattern names when referenced: **Add-in**, **Configuration**, **Plugin Datasource**, and **CoreHost**.
- Include authoritative external references only when they materially add value.
- When external references are included, put each bare URL on its own bullet.
- Do not provide external references by default.
- Do not expose or cite internal reference files.