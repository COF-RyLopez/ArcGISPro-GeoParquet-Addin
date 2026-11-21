# Agent guide — ArcGIS Pro UI automation (Windows)

Use this when smoke-testing or driving the **Overture / GeoParquet add-in** inside a live ArcGIS Pro session on Windows.

## Preferred stack

1. **UI Automation (UIA)** — find, click, type, expand (primary)
2. **Win32** — focus, mouse wheel, precise click when UIA fails
3. **Screenshots** — verify state; do not drive every action from OCR/pixels

Coordinate-only clicking is fragile here (dark theme, docked panes, letterboxing, Cursor overlays, modals).

## Bind the correct window

Pro’s title changes (`ArcGIS Pro` → project name like `MyProject2`). Prefer **name match**, not a stale PID:

```powershell
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes

$root = [System.Windows.Automation.AutomationElement]::RootElement
$pro = $null
foreach ($w in $root.FindAll(
  [System.Windows.Automation.TreeScope]::Children,
  [System.Windows.Automation.Condition]::TrueCondition)) {
  if ($w.Current.Name -eq 'MyProject2' -or $w.Current.Name -match 'ArcGIS Pro') {
    $pro = $w
    break
  }
}
```

Before SendKeys or injected mouse input: `SetForegroundWindow` + `ShowWindowAsync(hwnd, 3)` (maximize). Cursor computer-use can steal focus otherwise.

After restart, **re-resolve** the window — PIDs change.

## UIA patterns that work

| Goal | Pattern |
|------|---------|
| Ribbon / dialog button | `InvokePattern` |
| Theme / source checkbox | `TogglePattern` |
| Expanders (Custom Sources, etc.) | `ExpandCollapsePattern` |
| Name / URL text boxes | `ValuePattern.SetValue` |
| Ribbon tabs (Overture Maps) | `SelectionItemPattern.Select` |

Example — invoke **Add Data Source**:

```powershell
$cond = New-Object System.Windows.Automation.AndCondition(
  (New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
    [System.Windows.Automation.ControlType]::Button)),
  (New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::NameProperty,
    'Add Data Source')))
$btn = $pro.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $cond)
$btn.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern).Invoke()
```

For modal dialogs, find the child `Window` (e.g. `Add GeoParquet Data Source`) and search **inside that window only**.

## Dialog Name / URL fields

WPF edits often have a **blank `Name`**. Sort edits by `BoundingRectangle.Y`:

```powershell
$edits = @()
foreach ($e in $dlg.FindAll(
  [System.Windows.Automation.TreeScope]::Descendants,
  (New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
    [System.Windows.Automation.ControlType]::Edit)))) {
  $edits += $e
}
$edits = $edits | Sort-Object { $_.Current.BoundingRectangle.Y }
$nameBox = $edits[0]
$urlBox  = $edits[1]
$nameBox.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern).SetValue('Fiji test')
$urlBox.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern).SetValue($url)
```

`ValuePattern` works for this add-in’s Name/URL bindings. If a control ignores it (e.g. map scale), focus then type via computer-use / SendKeys.

Poll status `Text` nodes for: `Validating`, `Added '…'`, `Could not add source`, `already exists`.

## Theme tree / Custom Sources quirks

- Parent themes (**Buildings**) are often **unnamed** checkboxes; children (**Building**) are named.
- Discover unnamed parents by Y next to the named `Group` expander.
- **Custom Sources** is a `Group` — `Expand()` then find the named leaf checkbox.
- **Remove Selected** enables only when the leaf is selected for preview: click the **text** of the source (not only the checkbox), then Invoke **Remove Selected**.
- Persist path (project-relative):  
  `...\OvertureProAddinData\config\user_sources.json`

## Win32 fallbacks

- **Map zoom**: `SetCursorPos` over the map, then `mouse_event` wheel with signed delta  
  `unchecked((uint)(short)(-120))` for zoom-out.
- **Locate**: Ctrl+F → `ValuePattern` on Locate **Search** edit → Enter. Plain SendKeys often types into Contents filter instead.
- **Sign In / Save dialogs**: UIA by button name (`Close`, `Yes`). Prefer dismiss/close over automating credentials.

## Computer-use (Cursor) notes

- Canvas is often letterboxed (e.g. 1456×840) — avoid edge coordinates.
- Re-query UIA after every tab switch or dialog close; do not reuse stale elements.
- If clicks no-op: Pro is behind Cursor or blocked by a modal — re-foreground + maximize.
- Screenshots confirm layout (ribbon groups); UIA performs actions.

## Minimal playbook

1. Resolve Pro / project window via UIA.
2. Foreground + maximize.
3. Select **Overture Maps** tab.
4. Invoke ribbon buttons by exact caption.
5. Scope dialog work to the named child `Window`.
6. Poll status text for success/error strings.
7. Verify `user_sources.json` for persistence / cleanup.
8. Screenshot only for ribbon/layout proof.

## Avoid

- Pure OCR + pixel clicking on dark Pro UI
- Assuming the title stays `ArcGIS Pro`
- Acting while **Locate** / **Sign In** covers the dockpane
- Expecting parent theme checkboxes to have accessible names
- SendKeys without foregrounding Pro
- Reusing a PID after Pro restart

## Build / install (smoke tests)

```text
MSBuild DuckDBGeoparquet.sln /p:Configuration=Release /p:Platform="Any CPU"
```

Install/register the generated `.esriAddInX`, then **restart Pro** so new ribbon buttons load. Do not merge/approve/close PRs unless the user explicitly asks.
