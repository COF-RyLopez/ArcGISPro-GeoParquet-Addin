# Read-only health check: is this Pro instance worth driving?
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'Resolve-ProWindow.ps1')

$pro = Get-ProWindow
if (-not $pro) {
  Write-Host 'DOCTOR_FAIL: no ArcGIS Pro window'
  exit 1
}

$title = $pro.Current.Name
$procId = $pro.Current.ProcessId
Write-Host "DOCTOR: window='$title' pid=$procId"

$tab = Find-Named $pro 'Overture Maps' ([System.Windows.Automation.ControlType]::TabItem)
if (-not $tab) {
  Write-Host 'DOCTOR_FAIL: Overture Maps ribbon tab missing (add-in not loaded?)'
  exit 1
}
Write-Host 'DOCTOR: Overture Maps tab present'

if ($env:VERIFY_REQUIRE_PROJECT -eq '1' -and $title -eq 'ArcGIS Pro') {
  Write-Host 'DOCTOR_FAIL: still on start page; open a map project first'
  exit 1
}

$dll = Join-Path $env:LOCALAPPDATA 'ESRI\ArcGISPro\AssemblyCache\{9e44d8fc-6e1a-4513-9992-85c2de3e2d61}\DuckDBGeoparquet.dll'
if (Test-Path $dll) {
  $fi = Get-Item $dll
  Write-Host "DOCTOR: AssemblyCache dll $($fi.LastWriteTime.ToString('s')) ($([math]::Round($fi.Length/1KB)) KB)"
} else {
  Write-Host 'DOCTOR_WARN: AssemblyCache dll not found yet (first load after register)'
}

Write-Host 'DOCTOR_OK'
exit 0
