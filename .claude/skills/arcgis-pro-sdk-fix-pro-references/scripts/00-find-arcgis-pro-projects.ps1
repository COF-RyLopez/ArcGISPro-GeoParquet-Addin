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

<#
.SYNOPSIS
Finds ArcGIS Pro SDK projects under a workspace root.

.DESCRIPTION
Returns only .csproj and .vbproj files containing an ArcGIS assembly Reference
or the Esri.ProApp.SDK.Desktop.targets import. Excludes generated and dependency
directories. Malformed project XML is a blocking error.

.EXAMPLE
& ./00-find-arcgis-pro-projects.ps1 -WorkspacePath C:\src\MyWorkspace
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateScript({ Test-Path -LiteralPath $_ -PathType Container })]
    [string]$WorkspacePath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$WorkspacePath = (Resolve-Path -LiteralPath $WorkspacePath).Path.TrimEnd('\', '/')
$workspaceSegments = @($WorkspacePath -split '[\\/]')
if ($workspaceSegments -contains '.github') {
    throw 'WorkspacePath cannot be inside the .github directory.'
}

$excludedDirectoryNames = @('bin', 'obj', '.git', 'node_modules', '.github')
$projectFiles = New-Object System.Collections.Generic.List[System.IO.FileInfo]
$directories = New-Object 'System.Collections.Generic.Stack[string]'
$directories.Push($WorkspacePath)
while ($directories.Count -gt 0) {
    $currentDirectory = $directories.Pop()
    foreach ($childDirectory in Get-ChildItem -LiteralPath $currentDirectory -Directory -Force) {
        if ($childDirectory.Name -in $excludedDirectoryNames) {
            continue
        }
        $directories.Push($childDirectory.FullName)
    }

    foreach ($projectFile in Get-ChildItem -LiteralPath $currentDirectory -File -Force) {
        if ($projectFile.Extension -in @('.csproj', '.vbproj')) {
            $projectFiles.Add($projectFile)
        }
    }
}

foreach ($projectFile in $projectFiles) {
    $document = New-Object System.Xml.XmlDocument
    try {
        $document.Load($projectFile.FullName)
    } catch {
        $caughtError = $_
        throw "Cannot inspect project XML '$($projectFile.FullName)': $($caughtError.Exception.Message)"
    }

    $evidence = New-Object System.Collections.Generic.List[string]
    foreach ($referenceNode in @($document.SelectNodes("//*[local-name()='Reference']"))) {
        $include = [string]$referenceNode.GetAttribute('Include')
        if ($include -match '^(?i)(ArcGIS\.|ESRI\.ArcGIS\.)') {
            $evidence.Add("Reference:$include") | Out-Null
        }
    }
    foreach ($importNode in @($document.SelectNodes("//*[local-name()='Import']"))) {
        $importProject = [string]$importNode.GetAttribute('Project')
        if ([System.IO.Path]::GetFileName($importProject) -ieq 'Esri.ProApp.SDK.Desktop.targets') {
            $evidence.Add('DesktopTargetsImport') | Out-Null
        }
    }

    if ($evidence.Count -gt 0) {
        [pscustomobject]@{
            ProjectPath = $projectFile.FullName
            Evidence    = @($evidence)
        }
    }
}