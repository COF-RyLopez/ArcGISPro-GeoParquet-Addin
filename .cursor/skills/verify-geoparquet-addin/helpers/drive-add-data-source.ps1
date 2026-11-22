# Drive "Add Data Source" ribbon → dialog → Add → prove user_sources.json.
param(
  [Parameter(Mandatory = $true)][string]$Name,
  [Parameter(Mandatory = $true)][string]$Url,
  [string]$EvidenceDir = $env:VERIFY_EVIDENCE_DIR
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'Resolve-ProWindow.ps1')
Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class VerifyWin {
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
}
"@

if (-not $EvidenceDir) {
  $EvidenceDir = Join-Path $env:TEMP ("verify-geoparquet-addin\" + (Get-Date -Format 'yyyyMMdd-HHmmss'))
  $env:VERIFY_EVIDENCE_DIR = $EvidenceDir
}
$out = Join-Path $EvidenceDir 'add-data-source'
New-Item -ItemType Directory -Force -Path $out | Out-Null
$log = Join-Path $out 'action.log'
function Log([string]$m) {
  $line = "$(Get-Date -Format o) $m"
  Add-Content -Path $log -Value $line
  Write-Host $line
}

$pro = Get-ProWindow
if (-not $pro) { throw 'Pro window not found' }
[VerifyWin]::ShowWindow([IntPtr]$pro.Current.NativeWindowHandle, 3) | Out-Null
[VerifyWin]::SetForegroundWindow([IntPtr]$pro.Current.NativeWindowHandle) | Out-Null
Log "window=$($pro.Current.Name)"

# Select Overture Maps tab
$tab = Find-Named $pro 'Overture Maps' ([System.Windows.Automation.ControlType]::TabItem)
if (-not $tab) { throw 'Overture Maps tab missing' }
try {
  $tab.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern).Select()
  Log 'selected Overture Maps tab'
} catch {
  Log "tab select via pattern failed: $_"
}
Start-Sleep 1

Invoke-NamedButton $pro 'Add Data Source'
Log 'invoked Add Data Source'
Start-Sleep 2

$root = [System.Windows.Automation.AutomationElement]::RootElement
$dlg = Find-Named $root 'Add GeoParquet Data Source' ([System.Windows.Automation.ControlType]::Window)
if (-not $dlg) {
  # Sometimes ProWindow is not ControlType.Window — search by name only
  $dlg = Find-Named $root 'Add GeoParquet Data Source'
}
if (-not $dlg) { throw 'Add GeoParquet Data Source dialog not found' }
Log 'dialog open'

$edits = @()
foreach ($e in $dlg.FindAll(
    [System.Windows.Automation.TreeScope]::Descendants,
    (New-Object System.Windows.Automation.PropertyCondition(
      [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
      [System.Windows.Automation.ControlType]::Edit)))) {
  $edits += $e
}
$edits = $edits | Sort-Object { $_.Current.BoundingRectangle.Y }
if ($edits.Count -lt 2) { throw "Expected Name+URL edits, found $($edits.Count)" }

$edits[0].GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern).SetValue($Name)
$edits[1].GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern).SetValue($Url)
Log "filled Name='$Name' Url='$Url'"

Invoke-NamedButton $dlg 'Add'
Log 'clicked Add'

# Poll status text
$status = ''
$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
  Start-Sleep 2
  foreach ($t in $dlg.FindAll(
      [System.Windows.Automation.TreeScope]::Descendants,
      (New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
        [System.Windows.Automation.ControlType]::Text)))) {
    $n = $t.Current.Name
    if ($n -and ($n -match "Added '|Could not add|Validating|already exists|Enter ")) {
      $status = $n
      break
    }
  }
  Log "status=$status"
  if ($status -match "^Added '") { break }
  if ($status -match 'Could not add|already exists|Enter ') { break }
}

$status | Set-Content (Join-Path $out 'status.txt')
if ($status -notmatch "^Added '") {
  Log "FAIL status='$status'"
  try { Invoke-NamedButton $dlg 'Close' } catch {}
  exit 1
}

# Side effect: user_sources.json
$storeCandidates = @(
  (Join-Path $env:USERPROFILE 'OneDrive - County of Fresno\Documents\ArcGIS\Projects\MyProject2\OvertureProAddinData\config\user_sources.json'),
  (Join-Path $env:USERPROFILE 'Documents\ArcGIS\Projects\MyProject2\OvertureProAddinData\config\user_sources.json')
)
$found = $false
foreach ($p in $storeCandidates) {
  if (-not (Test-Path $p)) { continue }
  Copy-Item $p (Join-Path $out 'user_sources.json') -Force
  $json = Get-Content $p -Raw
  if ($json -match [regex]::Escape($Name)) {
    Log "PROOF store contains name at $p"
    $found = $true
    break
  }
}
if (-not $found) {
  Log 'FAIL: Added status but name not found in known user_sources.json paths'
  try { Invoke-NamedButton $dlg 'Close' } catch {}
  exit 1
}

try { Invoke-NamedButton $dlg 'Close' } catch {}
Log 'PASS'
'PASS' | Set-Content (Join-Path $out 'result.txt')
exit 0
