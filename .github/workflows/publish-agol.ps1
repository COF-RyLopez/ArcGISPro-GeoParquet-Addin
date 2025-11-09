#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Publishes ArcGIS Pro add-in to ArcGIS Online using Portal REST API
.DESCRIPTION
    This script uploads and updates an add-in item in ArcGIS Online using the Portal REST API.
    Supports both OAuth2 app client token and username/password authentication.
.PARAMETER AddInFile
    Path to the .esriAddInX file to publish
.PARAMETER PortalUrl
    ArcGIS Online portal URL (default: https://www.arcgis.com)
.PARAMETER ItemId
    The item ID of the existing add-in in AGOL (required for updates)
.PARAMETER AuthMethod
    Authentication method: 'token' or 'username' (default: 'token')
.PARAMETER ClientId
    OAuth2 app client ID (required if AuthMethod is 'token')
.PARAMETER ClientSecret
    OAuth2 app client secret (required if AuthMethod is 'token')
.PARAMETER Username
    Portal username (required if AuthMethod is 'username')
.PARAMETER Password
    Portal password (required if AuthMethod is 'username')
.PARAMETER Title
    Title for the add-in item (optional, uses filename if not provided)
.PARAMETER Description
    Description for the add-in item (optional)
.PARAMETER Tags
    Comma-separated tags for the add-in (optional)
.EXAMPLE
    .\publish-agol.ps1 -AddInFile "addin.esriAddInX" -ItemId "abc123" -ClientId "myclientid" -ClientSecret "mysecret"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$AddInFile,

    [Parameter(Mandatory=$false)]
    [string]$PortalUrl = "https://www.arcgis.com",

    [Parameter(Mandatory=$true)]
    [string]$ItemId,

    [Parameter(Mandatory=$false)]
    [ValidateSet('token', 'username')]
    [string]$AuthMethod = 'token',

    [Parameter(Mandatory=$false)]
    [string]$ClientId,

    [Parameter(Mandatory=$false)]
    [string]$ClientSecret,

    [Parameter(Mandatory=$false)]
    [string]$Username,

    [Parameter(Mandatory=$false)]
    [string]$Password,

    [Parameter(Mandatory=$false)]
    [string]$Title,

    [Parameter(Mandatory=$false)]
    [string]$Description,

    [Parameter(Mandatory=$false)]
    [string]$Tags = "ArcGIS Pro,Add-in,GeoParquet"
)

Write-Host "================================================"
Write-Host "  ArcGIS Online Publishing Script"
Write-Host "================================================"
Write-Host ""

# Validate inputs
if (-not (Test-Path $AddInFile)) {
    Write-Error "Add-in file not found at: $AddInFile"
    exit 1
}

if ($AuthMethod -eq 'token' -and (-not $ClientId -or -not $ClientSecret)) {
    Write-Error "ClientId and ClientSecret are required when using token authentication"
    exit 1
}

if ($AuthMethod -eq 'username' -and (-not $Username -or -not $Password)) {
    Write-Error "Username and Password are required when using username authentication"
    exit 1
}

# Set default title if not provided
if (-not $Title) {
    $Title = [System.IO.Path]::GetFileNameWithoutExtension($AddInFile)
}

Write-Host "Portal URL: $PortalUrl"
Write-Host "Item ID: $ItemId"
Write-Host "Add-in file: $AddInFile"
Write-Host "Authentication: $AuthMethod"
Write-Host ""

try {
    # Step 1: Get authentication token
    Write-Host "🔐 Authenticating with ArcGIS Online..."
    
    $token = $null
    if ($AuthMethod -eq 'token') {
        # OAuth2 app client authentication
        $tokenUrl = "$PortalUrl/sharing/rest/oauth2/token"
        $tokenBody = @{
            client_id = $ClientId
            client_secret = $ClientSecret
            grant_type = "client_credentials"
        } | ConvertTo-Json
        
        $tokenResponse = Invoke-RestMethod -Uri $tokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
        $token = $tokenResponse.access_token
        
        if (-not $token) {
            Write-Error "Failed to obtain access token"
            exit 1
        }
        Write-Host "✅ Authentication successful (OAuth2)"
    }
    else {
        # Username/password authentication
        $tokenUrl = "$PortalUrl/sharing/rest/generateToken"
        $tokenBody = @{
            username = $Username
            password = $Password
            referer = "https://www.arcgis.com"
            f = "json"
        }
        
        $tokenResponse = Invoke-RestMethod -Uri $tokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
        $token = $tokenResponse.token
        
        if (-not $token) {
            Write-Error "Failed to obtain access token: $($tokenResponse.error.message)"
            exit 1
        }
        Write-Host "✅ Authentication successful (Username/Password)"
    }

    # Step 2: Upload the add-in file using Portal REST API
    Write-Host ""
    Write-Host "📤 Uploading add-in file..."
    
    $uploadUrl = "$PortalUrl/sharing/rest/content/users/self/items/$ItemId/update"
    $fileName = [System.IO.Path]::GetFileName($AddInFile)
    
    # Portal REST API requires multipart/form-data with file and token
    $boundary = [System.Guid]::NewGuid().ToString()
    $fileBytes = [System.IO.File]::ReadAllBytes($AddInFile)
    
    # Build multipart form data manually
    $LF = "`r`n"
    $bodyParts = @()
    
    # Add token field
    $bodyParts += "--$boundary"
    $bodyParts += "Content-Disposition: form-data; name=`"token`""
    $bodyParts += ""
    $bodyParts += $token
    
    # Add file field
    $bodyParts += "--$boundary"
    $bodyParts += "Content-Disposition: form-data; name=`"file`"; filename=`"$fileName`""
    $bodyParts += "Content-Type: application/octet-stream"
    $bodyParts += ""
    
    # Combine text parts and binary file
    $textBody = $bodyParts -join $LF
    $textBytes = [System.Text.Encoding]::UTF8.GetBytes($textBody + $LF)
    $endBoundary = [System.Text.Encoding]::UTF8.GetBytes($LF + "--$boundary--" + $LF)
    
    # Combine all parts
    $bodyStream = New-Object System.IO.MemoryStream
    $bodyStream.Write($textBytes, 0, $textBytes.Length)
    $bodyStream.Write($fileBytes, 0, $fileBytes.Length)
    $bodyStream.Write($endBoundary, 0, $endBoundary.Length)
    $bodyBytes = $bodyStream.ToArray()
    $bodyStream.Close()
    
    $headers = @{
        "Authorization" = "Bearer $token"
    }
    
    try {
        $uploadResponse = Invoke-RestMethod -Uri $uploadUrl -Method Post -Body $bodyBytes -ContentType "multipart/form-data; boundary=$boundary" -Headers $headers
        
        if ($uploadResponse.error) {
            Write-Error "Upload failed: $($uploadResponse.error.message)"
            exit 1
        }
        
        Write-Host "✅ File uploaded successfully"
    }
    catch {
        $errorDetails = $_.ErrorDetails.Message | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($errorDetails -and $errorDetails.error) {
            Write-Error "Upload failed: $($errorDetails.error.message)"
        } else {
            Write-Error "Upload failed: $_"
        }
        exit 1
    }
    
    # Step 3: Update item metadata (if provided)
    if ($Description -or $Tags) {
        Write-Host ""
        Write-Host "📝 Updating item metadata..."
        
        $updateUrl = "$PortalUrl/sharing/rest/content/users/self/items/$ItemId/update"
        $updateParams = @{
            token = $token
            f = "json"
        }
        
        if ($Description) {
            $updateParams.description = $Description
        }
        
        if ($Tags) {
            $updateParams.tags = $Tags
        }
        
        $updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Post -Body $updateParams -ContentType "application/x-www-form-urlencoded"
        
        if ($updateResponse.error) {
            Write-Warning "Metadata update failed: $($updateResponse.error.message)"
        } else {
            Write-Host "✅ Metadata updated successfully"
        }
    }
    
    Write-Host ""
    Write-Host "✅ Successfully published to ArcGIS Online!" -ForegroundColor Green
    Write-Host "   Item URL: $PortalUrl/home/item.html?id=$ItemId"
    Write-Host ""
    
    exit 0
}
catch {
    Write-Error "Failed to publish to ArcGIS Online: $_"
    Write-Error $_.Exception.Message
    exit 1
}
