#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Updates version in Config.daml from git tag
.DESCRIPTION
    This script updates the version attribute in Config.daml based on a git tag version.
    It uses XML manipulation to ensure proper handling of the DAML file.
.PARAMETER Version
    The version string to set (e.g., "0.1.2")
.PARAMETER ConfigPath
    Path to the Config.daml file (default: "Config.daml")
.EXAMPLE
    .\update-version.ps1 -Version "0.1.2" -ConfigPath "Config.daml"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$Version,

    [Parameter(Mandatory=$false)]
    [string]$ConfigPath = "Config.daml"
)

Write-Host "================================================"
Write-Host "  Version Update Script for Config.daml"
Write-Host "================================================"
Write-Host ""

# Validate inputs
if (-not (Test-Path $ConfigPath)) {
    Write-Error "Config.daml file not found at: $ConfigPath"
    exit 1
}

# Validate version format (basic semantic version check)
if ($Version -notmatch '^\d+\.\d+\.\d+$') {
    Write-Warning "Version format may be non-standard: $Version"
    Write-Warning "Expected format: X.Y.Z (e.g., 0.1.2)"
}

Write-Host "Current file: $ConfigPath"
Write-Host "Target version: $Version"
Write-Host ""

try {
    # Load the XML file
    [xml]$xml = Get-Content -Path $ConfigPath -Raw -Encoding UTF8

    # Store original version for comparison
    $originalVersion = $xml.ArcGIS.AddInInfo.version
    Write-Host "Original version: $originalVersion"

    # Update the version attribute
    $xml.ArcGIS.AddInInfo.SetAttribute("version", $Version)

    # Verify the update
    $newVersion = $xml.ArcGIS.AddInInfo.version
    if ($newVersion -ne $Version) {
        Write-Error "Version update verification failed. Expected: $Version, Got: $newVersion"
        exit 1
    }

    # Save the XML file with UTF-8 encoding and preserve formatting
    $xmlSettings = New-Object System.Xml.XmlWriterSettings
    $xmlSettings.Indent = $true
    $xmlSettings.IndentChars = "`t"
    $xmlSettings.NewLineChars = "`r`n"
    $xmlSettings.Encoding = [System.Text.UTF8Encoding]::new($true) # UTF-8 with BOM

    $writer = [System.Xml.XmlWriter]::Create($ConfigPath, $xmlSettings)
    $xml.Save($writer)
    $writer.Close()

    Write-Host ""
    Write-Host "✓ Successfully updated version: $originalVersion → $Version" -ForegroundColor Green
    Write-Host ""
    Write-Host "Changes made to: $ConfigPath"

    # Show the updated line for verification
    $updatedContent = Get-Content -Path $ConfigPath | Select-String -Pattern "version="
    Write-Host ""
    Write-Host "Updated line:" -ForegroundColor Cyan
    Write-Host "  $updatedContent"

    exit 0
}
catch {
    Write-Error "Failed to update Config.daml: $_"
    Write-Error $_.Exception.Message
    exit 1
}
