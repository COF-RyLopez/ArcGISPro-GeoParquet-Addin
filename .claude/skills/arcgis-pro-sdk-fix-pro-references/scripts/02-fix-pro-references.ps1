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

  [Parameter(Mandatory = $true)]
  [string]$ArcGISProVersion,

  [switch]$UpdateTargetFramework,

  [ValidateScript({ Test-Path -LiteralPath $_ -PathType Container })]
  [string]$WorkspacePath,

  [string]$BackupDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-FileEncoding {
  param([Parameter(Mandatory = $true)][string]$Path)

  $stream = [System.IO.File]::OpenRead($Path)
  try {
    $reader = New-Object System.IO.StreamReader($stream, $true)
    try {
      [void]$reader.Peek()
      return $reader.CurrentEncoding
    } finally {
      $reader.Dispose()
    }
  } finally {
    $stream.Dispose()
  }
}

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

function Update-JsonNode {
  param(
    [Parameter(Mandatory = $true)]$Node,
    [Parameter(Mandatory = $true)][string]$ArcGISProExe,
    [Parameter(Mandatory = $true)][string]$ArcGISProBin,
    [string]$OldTargetFramework,
    [string]$NewTargetFramework,
    [Parameter(Mandatory = $true)][ref]$ChangeCount
  )

  if ($Node -is [System.Collections.IEnumerable] -and $Node -isnot [string] -and
      $Node -isnot [pscustomobject]) {
    foreach ($item in $Node) {
      Update-JsonNode -Node $item -ArcGISProExe $ArcGISProExe -ArcGISProBin $ArcGISProBin `
        -OldTargetFramework $OldTargetFramework -NewTargetFramework $NewTargetFramework `
        -ChangeCount $ChangeCount
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
  $isArcGISProProfile = $null -ne $programProperty

  foreach ($property in $properties) {
    if ($isArcGISProProfile -and $property.Name -in @('program', 'executablePath')) {
      if ($property.Value -ne $ArcGISProExe) {
        $property.Value = $ArcGISProExe
        $ChangeCount.Value++
      }
    } elseif ($isArcGISProProfile -and $property.Name -in @('cwd', 'workingDirectory')) {
      if ($property.Value -ne $ArcGISProBin) {
        $property.Value = $ArcGISProBin
        $ChangeCount.Value++
      }
    } elseif ($isArcGISProProfile -and $property.Value -is [string] -and $OldTargetFramework -and
              $property.Value.Contains($OldTargetFramework)) {
      $updatedValue = $property.Value.Replace($OldTargetFramework, $NewTargetFramework)
      if ($updatedValue -ne $property.Value) {
        $property.Value = $updatedValue
        $ChangeCount.Value++
      }
    }

    if ($property.Value -isnot [string] -and $null -ne $property.Value) {
      Update-JsonNode -Node $property.Value -ArcGISProExe $ArcGISProExe -ArcGISProBin $ArcGISProBin `
        -OldTargetFramework $OldTargetFramework -NewTargetFramework $NewTargetFramework `
        -ChangeCount $ChangeCount
    }
  }
}

$ProjectPath = (Resolve-Path -LiteralPath $ProjectPath).Path
if ([System.IO.Path]::GetExtension($ProjectPath) -ne '.csproj' -and [System.IO.Path]::GetExtension($ProjectPath) -ne '.vbproj') {
  throw "ProjectPath must identify a .csproj or .vbproj file: $ProjectPath"
}

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
$runtimeConfigPath = Join-Path $proBin 'ArcGISPro.runtimeconfig.json'
$frameworkXsdPath = Join-Path $proBin 'ArcGIS.Desktop.Framework.xsd'
$desktopTargetsPath = Join-Path $proBin 'Esri.ProApp.SDK.Desktop.targets'

$manifestPaths = @(
  @(
    (Join-Path $projectRoot 'Config.daml')
    (Join-Path $projectRoot 'Config.xml')
  ) | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf }
)

$requiredInstallFiles = @($proExe, $runtimeConfigPath, $frameworkXsdPath)
$missingInstallFiles = @($requiredInstallFiles | Where-Object { -not (Test-Path -LiteralPath $_ -PathType Leaf) })
if ($missingInstallFiles.Count -gt 0) {
  throw "ArcGIS Pro installation is incomplete; missing: $($missingInstallFiles -join ', ')"
}
if ([string]::IsNullOrWhiteSpace($ArcGISProVersion)) {
  throw 'ArcGISProVersion cannot be empty.'
}

if (-not $BackupDirectory) {
  $backupName = 'ArcGISProReferenceFix-{0}-{1}' -f (Get-Date -Format 'yyyyMMddHHmmss'), ([guid]::NewGuid().ToString('N'))
  $BackupDirectory = Join-Path ([System.IO.Path]::GetTempPath()) $backupName
}
$BackupDirectory = [System.IO.Path]::GetFullPath($BackupDirectory)
$backups = @{}
$updatedFiles = New-Object System.Collections.Generic.List[string]
$changes = New-Object System.Collections.Generic.List[string]

function Backup-File {
  param([Parameter(Mandatory = $true)][string]$Path)

  if ($backups.ContainsKey($Path)) {
    return
  }
  $relativePath = $Path.Substring($WorkspacePath.Length).TrimStart('\', '/')
  $backupPath = Join-Path $BackupDirectory $relativePath
  $backupParent = Split-Path -Parent $backupPath
  [void](New-Item -ItemType Directory -Path $backupParent -Force)
  Copy-Item -LiteralPath $Path -Destination $backupPath -Force
  $backups[$Path] = $backupPath
}

function Save-XmlIfChanged {
  param(
    [Parameter(Mandatory = $true)][System.Xml.XmlDocument]$Document,
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][int]$ChangeCount
  )

  if ($ChangeCount -eq 0) {
    return
  }
  Backup-File -Path $Path
  $encoding = Get-FileEncoding -Path $Path
  $settings = New-Object System.Xml.XmlWriterSettings
  $settings.Encoding = $encoding
  $settings.Indent = $false
  $settings.NewLineHandling = [System.Xml.NewLineHandling]::None
  $writer = [System.Xml.XmlWriter]::Create($Path, $settings)
  try {
    $Document.Save($writer)
  } finally {
    $writer.Dispose()
  }
  $updatedFiles.Add($Path) | Out-Null
}

$oldTargetFramework = $null
$newTargetFramework = $null

try {
  $projectDocument = Read-XmlDocument -Path $ProjectPath
  $projectChanges = 0
  $referenceNodes = @($projectDocument.SelectNodes("//*[local-name()='Reference']"))
  foreach ($referenceNode in $referenceNodes) {
    $include = [string]$referenceNode.GetAttribute('Include')
    if ($include -notmatch '^(?i)(ArcGIS\.|ESRI\.ArcGIS\.)') {
      continue
    }

    $hintPathNode = $referenceNode.SelectSingleNode("*[local-name()='HintPath']")
    if ($null -eq $hintPathNode -or [string]::IsNullOrWhiteSpace($hintPathNode.InnerText)) {
      continue
    }

    $currentHintPath = $hintPathNode.InnerText.Trim()
    $binSuffixMatch = [regex]::Match($currentHintPath, '(?i)(?:^|[\\/])bin[\\/](?<suffix>.+)$')
    if ($binSuffixMatch.Success) {
      $newHintPath = Join-Path $proBin ($binSuffixMatch.Groups['suffix'].Value -replace '/', '\')
    } else {
      $fileName = [System.IO.Path]::GetFileName($currentHintPath)
      $matchedFiles = @(Get-ChildItem -LiteralPath $proBin -Filter $fileName -File -Recurse)
      if ($matchedFiles.Count -ne 1) {
        throw "Cannot resolve a unique ArcGIS Pro DLL for reference '$include' from '$currentHintPath'."
      }
      $newHintPath = $matchedFiles[0].FullName
    }

    if (-not (Test-Path -LiteralPath $newHintPath -PathType Leaf)) {
      throw "Resolved HintPath does not exist for reference '$include': $newHintPath"
    }
    if ($hintPathNode.InnerText -ne $newHintPath) {
      $hintPathNode.InnerText = $newHintPath
      $projectChanges++
      $changes.Add("HintPath:$include") | Out-Null
    }
  }

  $desktopImportFound = $false
  $importNodes = @($projectDocument.SelectNodes("//*[local-name()='Import']"))
  foreach ($importNode in $importNodes) {
    $importProject = [string]$importNode.GetAttribute('Project')
    if ([System.IO.Path]::GetFileName($importProject) -ine 'Esri.ProApp.SDK.Desktop.targets') {
      continue
    }
    $desktopImportFound = $true
    if (-not (Test-Path -LiteralPath $desktopTargetsPath -PathType Leaf)) {
      throw "ArcGIS Pro Desktop targets file is missing: $desktopTargetsPath"
    }
    if ($importProject -ne $desktopTargetsPath) {
      [void]$importNode.SetAttribute('Project', $desktopTargetsPath)
      $projectChanges++
      $changes.Add('DesktopTargetsImport') | Out-Null
    }
    if ($importNode.HasAttribute('Condition')) {
      $condition = [string]$importNode.GetAttribute('Condition')
      $updatedCondition = [regex]::Replace(
        $condition,
        "(?i)Exists\('.*?Esri\.ProApp\.SDK\.Desktop\.targets'\)",
        "Exists('$desktopTargetsPath')"
      )
      if ($updatedCondition -ne $condition) {
        [void]$importNode.SetAttribute('Condition', $updatedCondition)
        $projectChanges++
        $changes.Add('DesktopTargetsCondition') | Out-Null
      }
    }
  }
  if ($manifestPaths.Count -gt 0 -and -not $desktopImportFound) {
    throw 'Projects with Config.daml or Config.xml require an Esri.ProApp.SDK.Desktop.targets import.'
  }

  if ($UpdateTargetFramework) {
    $targetFrameworkNodes = @($projectDocument.SelectNodes("//*[local-name()='TargetFramework']"))
    $targetFrameworksNodes = @($projectDocument.SelectNodes("//*[local-name()='TargetFrameworks']"))
    if ($targetFrameworksNodes.Count -gt 0 -or $targetFrameworkNodes.Count -ne 1) {
      throw 'Target framework update requires exactly one TargetFramework element and no TargetFrameworks element.'
    }
    $runtimeConfig = Get-Content -LiteralPath $runtimeConfigPath -Raw | ConvertFrom-Json
    $runtimeTfm = [string]$runtimeConfig.runtimeOptions.tfm
    if ($runtimeTfm -notmatch '^net\d+\.\d+$') {
      throw "ArcGISPro.runtimeconfig.json contains an unsupported TFM: $runtimeTfm"
    }
    $requiredSdkBand = $runtimeTfm.Substring(3)
    $installedSdks = @(& dotnet --list-sdks 2>&1 | ForEach-Object { $_.ToString() })
    if ($LASTEXITCODE -ne 0 -or
        -not ($installedSdks | Where-Object { $_ -match "^$([regex]::Escape($requiredSdkBand))\." })) {
      throw "A compatible .NET SDK for '$runtimeTfm' is not installed. Installed SDKs: $($installedSdks -join ', ')"
    }
    $oldTargetFramework = $targetFrameworkNodes[0].InnerText.Trim()
    $newTargetFramework = "$runtimeTfm-windows"
    if ($oldTargetFramework -ne $newTargetFramework) {
      $targetFrameworkNodes[0].InnerText = $newTargetFramework
      $projectChanges++
      $changes.Add("TargetFramework:$oldTargetFramework->$newTargetFramework") | Out-Null
    }
  }
  Save-XmlIfChanged -Document $projectDocument -Path $ProjectPath -ChangeCount $projectChanges

  $xsiNamespace = 'http://www.w3.org/2001/XMLSchema-instance'
  $registryNamespace = 'http://schemas.esri.com/DADF/Registry'
  $xsdUri = ([System.Uri]::new($frameworkXsdPath)).AbsoluteUri
  foreach ($manifestPath in $manifestPaths) {
    $manifestDocument = Read-XmlDocument -Path $manifestPath
    $manifestChanges = 0
    $root = $manifestDocument.DocumentElement
    $schemaLocation = $root.GetAttribute('schemaLocation', $xsiNamespace)
    $expectedSchemaLocation = "$registryNamespace $xsdUri"
    if ($schemaLocation -ne $expectedSchemaLocation) {
      [void]$root.SetAttribute('schemaLocation', $xsiNamespace, $expectedSchemaLocation)
      $manifestChanges++
      $changes.Add("SchemaLocation:$([System.IO.Path]::GetFileName($manifestPath))") | Out-Null
    }
    Save-XmlIfChanged -Document $manifestDocument -Path $manifestPath -ChangeCount $manifestChanges
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
    } catch {
      throw "Cannot safely update JSON file '$jsonPath': $($_.Exception.Message)"
    }
    $jsonChangeCount = 0
    Update-JsonNode -Node $json -ArcGISProExe $proExe -ArcGISProBin $proBin `
      -OldTargetFramework $oldTargetFramework -NewTargetFramework $newTargetFramework `
      -ChangeCount ([ref]$jsonChangeCount)
    if ($jsonChangeCount -gt 0) {
      Backup-File -Path $jsonPath
      $encoding = Get-FileEncoding -Path $jsonPath
      $updatedJson = $json | ConvertTo-Json -Depth 100
      [System.IO.File]::WriteAllText($jsonPath, "$updatedJson$([Environment]::NewLine)", $encoding)
      $updatedFiles.Add($jsonPath) | Out-Null
      $changes.Add("Json:$([System.IO.Path]::GetFileName($jsonPath)):$jsonChangeCount") | Out-Null
    }
  }
} catch {
  foreach ($originalPath in $backups.Keys) {
    Copy-Item -LiteralPath $backups[$originalPath] -Destination $originalPath -Force
  }
  throw "Reference repair failed; all modified files were restored. $($_.Exception.Message)"
}

[pscustomobject]@{
  ProjectPath          = $ProjectPath
  ProjectRoot          = $projectRoot
  WorkspacePath        = $WorkspacePath
  ProInstallDir        = $ProInstallDir
  ArcGISProVersion     = $ArcGISProVersion
  TargetFramework      = $newTargetFramework
  BackupDirectory      = if ($updatedFiles.Count -gt 0) { $BackupDirectory } else { $null }
  FilesUpdated         = $updatedFiles.Count
  UpdatedFiles         = @($updatedFiles)
  Changes              = @($changes)
}