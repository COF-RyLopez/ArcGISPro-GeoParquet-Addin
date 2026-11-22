# Copyright 2026 Esri
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# license: Apache-2.0
# metadata:
# author: Esri, Inc.
# version: "1.0.0"

[CmdletBinding()]
param(
  [object[]]$InstallationCandidates
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($null -eq $InstallationCandidates) {
  $registryPaths = @(
    'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\ArcGISPro'
    'HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\ArcGISPro'
    'HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\ArcGISPro'
    'HKCU:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\ArcGISPro'
  )
  $InstallationCandidates = @(
    foreach ($registryPath in $registryPaths) {
      if (-not (Test-Path -LiteralPath $registryPath)) {
        continue
      }
      $item = Get-ItemProperty -LiteralPath $registryPath
      [pscustomobject]@{
        RegistryPath    = $registryPath
        InstallLocation = [string]$item.InstallLocation
        DisplayVersion  = [string]$item.DisplayVersion
      }
    }
  )
}

foreach ($candidate in $InstallationCandidates) {
  $registryPath = [string]$candidate.RegistryPath
  if ([string]::IsNullOrWhiteSpace($registryPath)) {
    $registryPath = '<provided candidate>'
  }
  $installLocation = ([string]$candidate.InstallLocation).Trim()
  $displayVersion = ([string]$candidate.DisplayVersion).Trim()
  if ([string]::IsNullOrWhiteSpace($installLocation) -or
      [string]::IsNullOrWhiteSpace($displayVersion)) {
    Write-Warning "Ignoring incomplete ArcGIS Pro registry record: $registryPath"
    continue
  }

  $installLocation = [System.IO.Path]::GetFullPath($installLocation)
  $binPath = Join-Path $installLocation 'bin'
  $requiredFiles = @(
    (Join-Path $binPath 'ArcGISPro.exe')
    (Join-Path $binPath 'ArcGISPro.runtimeconfig.json')
    (Join-Path $binPath 'ArcGIS.Desktop.Framework.xsd')
  )
  $missingFiles = @($requiredFiles | Where-Object { -not (Test-Path -LiteralPath $_ -PathType Leaf) })
  if ($missingFiles.Count -gt 0) {
    Write-Warning "Ignoring invalid ArcGIS Pro installation '$installLocation'; missing: $($missingFiles -join ', ')"
    continue
  }

  [pscustomobject]@{
    RegistryPath     = $registryPath
    ProInstallDir    = $installLocation.TrimEnd('\', '/')
    ArcGISProVersion = $displayVersion
    BinPath          = $binPath
  }
  return
}

Write-Warning 'ArcGIS Pro was not detected in a valid HKLM or HKCU uninstall registry record.'
