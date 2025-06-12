# Script to update DuckDBGeoparquet.csproj to use NuGet references instead of file references
param(
    [string]$ProjectFile = "DuckDBGeoparquet.csproj",
    [string]$NuGetVersion = "3.5.0.57366"
)

Write-Host "Updating $ProjectFile to use NuGet package Esri.ArcGISPro.Extensions30 version $NuGetVersion"

# Read the project file content
$content = Get-Content $ProjectFile -Raw

# Remove existing ArcGIS references and SDK import
$content = $content -replace '<Reference Include="ArcGIS\..*?</Reference>', ''
$content = $content -replace '<Reference Include="ESRI\..*?</Reference>', ''
$content = $content -replace '<Import Project="C:\\Program Files\\ArcGIS\\Pro\\bin\\Esri\.ProApp\.SDK\.Desktop\.targets".*?/>', ''

# Add reference to NuGet package if not already present
if ($content -notmatch 'Esri\.ArcGISPro\.Extensions30') {
    $packageRefSection = $content -match '<PackageReference Include.*?>'
    if ($packageRefSection) {
        $insertIdx = $content.IndexOf('</ItemGroup>', $content.IndexOf('<PackageReference')) 
        $nugetRef = "    <PackageReference Include=`"Esri.ArcGISPro.Extensions30`" Version=`"$NuGetVersion`" />"
        $content = $content.Insert($insertIdx, "$nugetRef`r`n")
    }
}

# Add HintPath for NuGet package assemblies 
$replacement = @"
</PropertyGroup>
  <ItemGroup>
    <Reference Include="ArcGIS.Core">
      <HintPath>packages\Esri.ArcGISPro.Extensions30.$NuGetVersion\lib\net8.0-windows7.0\ArcGIS.Core.dll</HintPath>
      <Private>True</Private>
    </Reference>
    <Reference Include="ArcGIS.Desktop.Framework">
      <HintPath>packages\Esri.ArcGISPro.Extensions30.$NuGetVersion\lib\net8.0-windows7.0\ArcGIS.Desktop.Framework.dll</HintPath>
      <Private>True</Private>
    </Reference>
    <Reference Include="ArcGIS.Desktop.Framework.Contracts">
      <HintPath>packages\Esri.ArcGISPro.Extensions30.$NuGetVersion\lib\net8.0-windows7.0\ArcGIS.Desktop.Framework.Contracts.dll</HintPath>
      <Private>True</Private>
    </Reference>
    <Reference Include="ArcGIS.Desktop.Framework.Threading.Tasks">
      <HintPath>packages\Esri.ArcGISPro.Extensions30.$NuGetVersion\lib\net8.0-windows7.0\ArcGIS.Desktop.Framework.Threading.Tasks.dll</HintPath>
      <Private>True</Private>
    </Reference>
  </ItemGroup>
"@
$content = $content -replace '</PropertyGroup>', $replacement

# Write updated content back to csproj
Set-Content -Path $ProjectFile -Value $content

Write-Host "Project file updated successfully"
