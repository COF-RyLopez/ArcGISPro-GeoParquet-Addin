# Tear down verification scratch. Keeps evidence directory.
$ErrorActionPreference = 'Continue'
Write-Host "CLEANUP: evidence kept at $($env:VERIFY_EVIDENCE_DIR)"

# Remove verify-* custom sources from the active project's user_sources.json if we can find it.
$candidates = @(
  (Join-Path $env:USERPROFILE 'OneDrive - County of Fresno\Documents\ArcGIS\Projects\MyProject2\OvertureProAddinData\config\user_sources.json'),
  (Join-Path $env:USERPROFILE 'Documents\ArcGIS\Projects\MyProject2\OvertureProAddinData\config\user_sources.json')
)
if ($env:VERIFY_USER_SOURCES) { $candidates = @($env:VERIFY_USER_SOURCES) + $candidates }

foreach ($path in $candidates) {
  if (-not (Test-Path $path)) { continue }
  try {
    $raw = Get-Content $path -Raw
    $list = @($raw | ConvertFrom-Json)
    if ($list.Count -eq 0) { continue }
    $kept = @($list | Where-Object { $_.Name -notmatch '^verify-' })
    $removed = $list.Count - $kept.Count
    if ($removed -gt 0) {
      $json = if ($kept.Count -eq 0) { '[]' } else { $kept | ConvertTo-Json -Depth 6 }
      Set-Content -Path $path -Value $json -Encoding UTF8
      Write-Host "CLEANUP: removed $removed verify-* source(s) from $path"
    } else {
      Write-Host "CLEANUP: no verify-* sources in $path"
    }
  } catch {
    Write-Host "CLEANUP_WARN: could not edit $path : $_"
  }
}

if ($env:VERIFY_KILL_PRO -eq '1' -and $env:VERIFY_PRO_PID) {
  $proc = Get-Process -Id ([int]$env:VERIFY_PRO_PID) -ErrorAction SilentlyContinue
  if ($proc -and $proc.ProcessName -eq 'ArcGISPro') {
    Stop-Process -Id $proc.Id -Force
    Write-Host "CLEANUP: stopped VERIFY_PRO_PID=$($proc.Id)"
  } else {
    Write-Host 'CLEANUP: VERIFY_PRO_PID not a live ArcGISPro process; left alone'
  }
} else {
  Write-Host 'CLEANUP: left ArcGIS Pro running (set VERIFY_KILL_PRO=1 and VERIFY_PRO_PID to stop the launched instance)'
}

Write-Host 'CLEANUP_DONE'
exit 0
