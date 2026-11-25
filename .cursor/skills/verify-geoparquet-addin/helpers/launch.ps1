# Build, register, and (re)start ArcGIS Pro for verification.
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..\..')).Path
Set-Location $repoRoot
Write-Host "LAUNCH: repo=$repoRoot"

$runId = Get-Date -Format 'yyyyMMdd-HHmmss'
if (-not $env:VERIFY_EVIDENCE_DIR) {
  $env:VERIFY_EVIDENCE_DIR = Join-Path $env:TEMP "verify-geoparquet-addin\$runId"
}
New-Item -ItemType Directory -Force -Path $env:VERIFY_EVIDENCE_DIR | Out-Null
Write-Host "LAUNCH: evidence=$env:VERIFY_EVIDENCE_DIR"

# Extensions must match DuckDbEngineVersion
$extSpatial = Join-Path $repoRoot 'Extensions\spatial.duckdb_extension'
$extHttpfs = Join-Path $repoRoot 'Extensions\httpfs.duckdb_extension'
if (-not (Test-Path $extSpatial) -or -not (Test-Path $extHttpfs)) {
  throw "Missing bundled DuckDB extensions under Extensions/. See Extensions/README.txt and build/DuckDbVersion.props."
}

$msbuild = $env:VERIFY_MSBUILD
if (-not $msbuild) {
  $msbuild = 'C:\Program Files\Microsoft Visual Studio\18\Community\MSBuild\Current\Bin\MSBuild.exe'
}
if (-not (Test-Path $msbuild)) {
  throw "MSBuild not found at $msbuild (set VERIFY_MSBUILD)"
}

$proj = Join-Path $repoRoot 'DuckDBGeoparquet.csproj'
& $msbuild $proj /p:Configuration=Release /p:Platform='Any CPU' /v:m /nologo
if ($LASTEXITCODE -ne 0) { throw "MSBuild failed: $LASTEXITCODE" }

$addin = Join-Path $repoRoot 'bin\Any CPU\Release\net10.0-windows\DuckDBGeoparquet.esriAddinX'
if (-not (Test-Path $addin)) { throw "Add-in missing: $addin" }

$cache = Join-Path $env:LOCALAPPDATA 'ESRI\ArcGISPro\AssemblyCache\{9e44d8fc-6e1a-4513-9992-85c2de3e2d61}'
if (Test-Path $cache) {
  Remove-Item $cache -Recurse -Force
  Write-Host "LAUNCH: cleared AssemblyCache"
}

$reg = 'C:\Program Files\ArcGIS\Pro\bin\RegisterAddIn.exe'
& $reg $addin /s
Write-Host "LAUNCH: RegisterAddIn exit=$LASTEXITCODE"

# If Pro already running, do not start a second instance — agent must restart manually or set VERIFY_RESTART_PRO=1
$existing = Get-Process ArcGISPro -ErrorAction SilentlyContinue
if ($existing -and $env:VERIFY_RESTART_PRO -ne '1') {
  Write-Host "LAUNCH: Pro already running (pids=$(($existing | ForEach-Object Id) -join ',')). Skipping Start-Process. Re-run doctor after you restart Pro if the add-in was stale."
  "SKIPPED_START" | Set-Content (Join-Path $env:VERIFY_EVIDENCE_DIR 'launch.log')
  exit 0
}

if ($existing -and $env:VERIFY_RESTART_PRO -eq '1') {
  $existing | Stop-Process -Force
  Start-Sleep 4
}

$proExe = 'C:\Program Files\ArcGIS\Pro\bin\ArcGISPro.exe'
$args = @()
if ($env:VERIFY_APRX -and (Test-Path $env:VERIFY_APRX)) {
  $args = @($env:VERIFY_APRX)
}
$p = Start-Process -FilePath $proExe -ArgumentList $args -PassThru
$env:VERIFY_PRO_PID = [string]$p.Id
"PID=$($p.Id)" | Set-Content (Join-Path $env:VERIFY_EVIDENCE_DIR 'launch.log')
Write-Host "LAUNCH: started Pro pid=$($p.Id) (VERIFY_PRO_PID set for optional cleanup kill)"

# Wait briefly for a window
$deadline = (Get-Date).AddMinutes(2)
. (Join-Path $PSScriptRoot 'Resolve-ProWindow.ps1')
while ((Get-Date) -lt $deadline) {
  Start-Sleep 3
  $w = Get-ProWindow
  if ($w) {
    Write-Host "LAUNCH: window='$($w.Current.Name)'"
    exit 0
  }
}
Write-Host 'LAUNCH_WARN: window not seen yet; continue with doctor after project opens'
exit 0
