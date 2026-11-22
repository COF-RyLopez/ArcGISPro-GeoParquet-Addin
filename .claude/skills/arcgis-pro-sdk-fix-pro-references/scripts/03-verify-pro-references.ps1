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
  [Parameter(Mandatory = $true)]
  [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
  [string]$ProjectPath,

  [Parameter(Mandatory = $true)]
  [string]$ProInstallDir,

  [ValidateScript({ Test-Path -LiteralPath $_ -PathType Container })]
  [string]$WorkspacePath,

  [string]$ExpectedTargetFramework,

  [switch]$ThrowOnFailure
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Read-XmlDocument {
  param([Parameter(Mandatory = $true)][string]$Path)

  $document = New-Object System.Xml.XmlDocument
  $document.PreserveWhitespace = $true
  $document.Load($Path)
  return $document
}

function ConvertFrom-JsonWithComments {
  param([Parameter(Mandatory = $true)][string]$Text)

  $builder = New-Object System.Text.StringBuilder
  $inString = $false
  $escaped = $false
  $inLineComment = $false
  $inBlockComment = $false
  for ($index = 0; $index -lt $Text.Length; $index++) {
    $character = $Text[$index]
    $next = if ($index + 1 -lt $Text.Length) { $Text[$index + 1] } else { [char]0 }
    if ($inLineComment) {
      if ($character -in @("`r", "`n")) {
        $inLineComment = $false
        [void]$builder.Append($character)
      }
      continue
    }
    if ($inBlockComment) {
      if ($character -eq '*' -and $next -eq '/') {
        $inBlockComment = $false
        $index++
      } elseif ($character -in @("`r", "`n")) {
        [void]$builder.Append($character)
      }
      continue
    }
    if ($inString) {
      [void]$builder.Append($character)
      if ($escaped) {
        $escaped = $false
      } elseif ($character -eq '\') {
        $escaped = $true
      } elseif ($character -eq '"') {
        $inString = $false
      }
      continue
    }
    if ($character -eq '"') {
      $inString = $true
      [void]$builder.Append($character)
    } elseif ($character -eq '/' -and $next -eq '/') {
      $inLineComment = $true
      $index++
    } elseif ($character -eq '/' -and $next -eq '*') {
      $inBlockComment = $true
      $index++
    } else {
      [void]$builder.Append($character)
    }
  }
  if ($inString -or $inBlockComment) {
    throw 'JSON contains an unterminated string or block comment.'
  }

  $withoutComments = $builder.ToString()
  [void]$builder.Clear()
  $inString = $false
  $escaped = $false
  for ($index = 0; $index -lt $withoutComments.Length; $index++) {
    $character = $withoutComments[$index]
    if ($inString) {
      [void]$builder.Append($character)
      if ($escaped) {
        $escaped = $false
      } elseif ($character -eq '\') {
        $escaped = $true
      } elseif ($character -eq '"') {
        $inString = $false
      }
      continue
    }
    if ($character -eq '"') {
      $inString = $true
      [void]$builder.Append($character)
      continue
    }
    if ($character -eq ',') {
      $lookAhead = $index + 1
      while ($lookAhead -lt $withoutComments.Length -and [char]::IsWhiteSpace($withoutComments[$lookAhead])) {
        $lookAhead++
      }
      if ($lookAhead -lt $withoutComments.Length -and $withoutComments[$lookAhead] -in @('}', ']')) {
        continue
      }
    }
    [void]$builder.Append($character)
  }
  return $builder.ToString() | ConvertFrom-Json
}

function Test-PathWithinRoot {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Root
  )

  $fullPath = [System.IO.Path]::GetFullPath($Path)
  $fullRoot = [System.IO.Path]::GetFullPath($Root).TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
  return $fullPath.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)
}

function Test-JsonNode {
  param(
    [Parameter(Mandatory = $true)]$Node,
    [Parameter(Mandatory = $true)][string]$JsonPath,
    [Parameter(Mandatory = $true)][string]$ExpectedExe,
    [Parameter(Mandatory = $true)][string]$ExpectedBin,
    [Parameter(Mandatory = $true)]
    [AllowEmptyCollection()]
    [System.Collections.Generic.List[string]]$Errors
  )

  if ($Node -is [System.Collections.IEnumerable] -and $Node -isnot [string] -and
      $Node -isnot [pscustomobject]) {
    foreach ($item in $Node) {
      Test-JsonNode -Node $item -JsonPath $JsonPath -ExpectedExe $ExpectedExe `
        -ExpectedBin $ExpectedBin -Errors $Errors
    }
    return
  }
  if ($Node -isnot [pscustomobject]) {
    return
  }

  $properties = @($Node.PSObject.Properties)
  $programProperty = $properties | Where-Object {
    $_.Name -in @('program', 'executablePath') -and
    $_.Value -is [string] -and $_.Value -match '(?i)ArcGISPro\.exe$'
  } | Select-Object -First 1
  if ($null -ne $programProperty) {
    if ([System.IO.Path]::GetFullPath([string]$programProperty.Value) -ine $ExpectedExe) {
      $Errors.Add("$JsonPath contains an ArcGISPro.exe path outside the selected installation: $($programProperty.Value)") | Out-Null
    }
    foreach ($workingProperty in $properties | Where-Object { $_.Name -in @('cwd', 'workingDirectory') }) {
      if ([System.IO.Path]::GetFullPath([string]$workingProperty.Value) -ine $ExpectedBin) {
        $Errors.Add("$JsonPath contains an ArcGIS Pro working directory outside the selected installation: $($workingProperty.Value)") | Out-Null
      }
    }
  }

  foreach ($property in $properties) {
    if ($property.Value -isnot [string] -and $null -ne $property.Value) {
      Test-JsonNode -Node $property.Value -JsonPath $JsonPath -ExpectedExe $ExpectedExe `
        -ExpectedBin $ExpectedBin -Errors $Errors
    }
  }
}

$ProjectPath = (Resolve-Path -LiteralPath $ProjectPath).Path
$ProInstallDir = [System.IO.Path]::GetFullPath($ProInstallDir).TrimEnd('\', '/')
$projectRoot = Split-Path -Parent $ProjectPath
$WorkspacePath = if ($WorkspacePath) {
  (Resolve-Path -LiteralPath $WorkspacePath).Path.TrimEnd('\', '/')
} else {
  $projectRoot
}
$workspacePrefix = $WorkspacePath + [System.IO.Path]::DirectorySeparatorChar
if ($projectRoot -ine $WorkspacePath -and
    -not $projectRoot.StartsWith($workspacePrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "ProjectPath must be located under WorkspacePath: $WorkspacePath"
}
$proBin = Join-Path $ProInstallDir 'bin'
$proExe = Join-Path $proBin 'ArcGISPro.exe'
$frameworkXsdPath = Join-Path $proBin 'ArcGIS.Desktop.Framework.xsd'
$desktopTargetsPath = Join-Path $proBin 'Esri.ProApp.SDK.Desktop.targets'
$errors = New-Object System.Collections.Generic.List[string]
$checkedFiles = New-Object System.Collections.Generic.List[string]
$arcGisReferenceCount = 0
$desktopImportFound = $false
$manifestPaths = @(
  @(
    (Join-Path $projectRoot 'Config.daml')
    (Join-Path $projectRoot 'Config.xml')
  ) | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf }
)

try {
  $projectDocument = Read-XmlDocument -Path $ProjectPath
  $checkedFiles.Add($ProjectPath) | Out-Null
  $referenceNodes = @($projectDocument.SelectNodes("//*[local-name()='Reference']"))
  foreach ($referenceNode in $referenceNodes) {
    $include = [string]$referenceNode.GetAttribute('Include')
    if ($include -notmatch '^(?i)(ArcGIS\.|ESRI\.ArcGIS\.)') {
      continue
    }
    $arcGisReferenceCount++
    $hintPathNode = $referenceNode.SelectSingleNode("*[local-name()='HintPath']")
    if ($null -eq $hintPathNode -or [string]::IsNullOrWhiteSpace($hintPathNode.InnerText)) {
      $errors.Add("ArcGIS reference '$include' has no HintPath.") | Out-Null
      continue
    }
    $hintPath = $hintPathNode.InnerText.Trim()
    if (-not (Test-PathWithinRoot -Path $hintPath -Root $proBin)) {
      $errors.Add("ArcGIS reference '$include' points outside the selected ArcGIS Pro bin folder: $hintPath") | Out-Null
    } elseif (-not (Test-Path -LiteralPath $hintPath -PathType Leaf)) {
      $errors.Add("ArcGIS reference '$include' points to a missing file: $hintPath") | Out-Null
    }
  }

  foreach ($importNode in @($projectDocument.SelectNodes("//*[local-name()='Import']"))) {
    $importProject = [string]$importNode.GetAttribute('Project')
    if ([System.IO.Path]::GetFileName($importProject) -ine 'Esri.ProApp.SDK.Desktop.targets') {
      continue
    }
    $desktopImportFound = $true
    if ([System.IO.Path]::GetFullPath($importProject) -ine $desktopTargetsPath) {
      $errors.Add("Desktop targets import does not point to the selected ArcGIS Pro installation: $importProject") | Out-Null
    } elseif (-not (Test-Path -LiteralPath $importProject -PathType Leaf)) {
      $errors.Add("Desktop targets import is missing: $importProject") | Out-Null
    }
  }
  if ($manifestPaths.Count -gt 0 -and -not $desktopImportFound) {
    $errors.Add('Project with an ArcGIS manifest is missing the Esri.ProApp.SDK.Desktop.targets import.') | Out-Null
  }
  if ($desktopImportFound -and $manifestPaths.Count -eq 0) {
    $errors.Add('Project imports Esri.ProApp.SDK.Desktop.targets but has no Config.daml or Config.xml.') | Out-Null
  }
  if ($arcGisReferenceCount -eq 0 -and -not $desktopImportFound -and $manifestPaths.Count -eq 0) {
    $errors.Add('Project contains no ArcGIS references, Desktop targets import, or ArcGIS manifest.') | Out-Null
  }

  if ($ExpectedTargetFramework) {
    $targetFrameworkNodes = @($projectDocument.SelectNodes("//*[local-name()='TargetFramework']"))
    if ($targetFrameworkNodes.Count -ne 1 -or $targetFrameworkNodes[0].InnerText.Trim() -ne $ExpectedTargetFramework) {
      $actual = @($targetFrameworkNodes | ForEach-Object { $_.InnerText.Trim() }) -join ', '
      $errors.Add("Expected TargetFramework '$ExpectedTargetFramework' but found '$actual'.") | Out-Null
    }
    $frameworkMatch = [regex]::Match($ExpectedTargetFramework, '^net(?<band>\d+\.\d+)-windows$')
    if (-not $frameworkMatch.Success) {
      $errors.Add("ExpectedTargetFramework is not a supported Windows TFM: $ExpectedTargetFramework") | Out-Null
    } else {
      $sdkBand = $frameworkMatch.Groups['band'].Value
      $installedSdks = @(& dotnet --list-sdks 2>&1 | ForEach-Object { $_.ToString() })
      if ($LASTEXITCODE -ne 0 -or
          -not ($installedSdks | Where-Object { $_ -match "^$([regex]::Escape($sdkBand))\." })) {
        $errors.Add("A compatible .NET SDK for '$ExpectedTargetFramework' is not installed.") | Out-Null
      }
    }
  }
} catch {
  $errors.Add("Project XML validation failed: $($_.Exception.Message)") | Out-Null
}

$xsiNamespace = 'http://www.w3.org/2001/XMLSchema-instance'
$registryNamespace = 'http://schemas.esri.com/DADF/Registry'
$expectedSchemaLocation = "$registryNamespace $([System.Uri]::new($frameworkXsdPath).AbsoluteUri)"
foreach ($manifestPath in $manifestPaths) {
  try {
    $manifestDocument = Read-XmlDocument -Path $manifestPath
    $checkedFiles.Add($manifestPath) | Out-Null
    $schemaLocation = $manifestDocument.DocumentElement.GetAttribute('schemaLocation', $xsiNamespace)
    if ($schemaLocation -cne $expectedSchemaLocation) {
      $errors.Add("Manifest schemaLocation is not the expected escaped ArcGIS Pro XSD URI: $manifestPath (expected '$expectedSchemaLocation')") | Out-Null
    }
  } catch {
    $errors.Add("Manifest validation failed for '$manifestPath': $($_.Exception.Message)") | Out-Null
  }
}

$jsonPaths = @(
  (Join-Path $WorkspacePath '.vscode\launch.json')
  (Join-Path $projectRoot '.vscode\launch.json')
  (Join-Path $projectRoot 'launch.json')
  (Join-Path $projectRoot 'Properties\launchSettings.json')
) | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -Unique
foreach ($jsonPath in $jsonPaths) {
  try {
    $json = ConvertFrom-JsonWithComments -Text (Get-Content -LiteralPath $jsonPath -Raw)
    $checkedFiles.Add($jsonPath) | Out-Null
    Test-JsonNode -Node $json -JsonPath $jsonPath -ExpectedExe $proExe -ExpectedBin $proBin -Errors $errors
  } catch {
    $errors.Add("JSON validation failed for '$jsonPath': $($_.Exception.Message)") | Out-Null
  }
}

$result = [pscustomobject]@{
  ProjectPath    = $ProjectPath
  WorkspacePath  = $WorkspacePath
  ProInstallDir  = $ProInstallDir
  IsValid        = $errors.Count -eq 0
  CheckedFiles   = @($checkedFiles)
  Errors         = @($errors)
}

if ($ThrowOnFailure -and -not $result.IsValid) {
  throw "ArcGIS Pro reference verification failed: $($errors -join ' | ')"
}
$result