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
Builds a repaired ArcGIS Pro SDK project and returns a structured report.

.DESCRIPTION
Runs dotnet build for one .csproj or .vbproj. A nonzero exit code is a blocking
error and includes the final build output lines needed for diagnosis.

.EXAMPLE
& ./04-build-project.ps1 -ProjectPath C:\src\MyAddIn\MyAddIn.csproj
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
    [string]$ProjectPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectPath = (Resolve-Path -LiteralPath $ProjectPath).Path
$extension = [System.IO.Path]::GetExtension($ProjectPath)
if ($extension -notin @('.csproj', '.vbproj')) {
    throw "ProjectPath must identify a .csproj or .vbproj file: $ProjectPath"
}

if (-not $PSCmdlet.ShouldProcess($ProjectPath, 'Build project')) {
    return
}

$buildOutput = @(& dotnet build $ProjectPath --nologo 2>&1 | ForEach-Object { $_.ToString() })
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    $diagnostic = @($buildOutput | Select-Object -Last 20) -join [Environment]::NewLine
    throw "Project build failed with exit code $exitCode for '$ProjectPath'.$([Environment]::NewLine)$diagnostic"
}

[pscustomobject]@{
    ProjectPath = $ProjectPath
    Succeeded   = $true
    ExitCode    = $exitCode
    Output      = $buildOutput
}