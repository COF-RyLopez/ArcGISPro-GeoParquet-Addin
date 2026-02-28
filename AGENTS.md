# AGENTS.md

## Cursor Cloud specific instructions

### Project Overview

This is an **ArcGIS Pro GeoParquet Add-in** — a C# (.NET 8.0) WPF desktop plugin that runs inside ArcGIS Pro on Windows. It uses DuckDB for in-memory geospatial data processing.

### Build on Linux

The project targets `net8.0-windows` with WPF. On Linux (Cloud Agent environment):

- **`dotnet restore`** works fully — all NuGet packages resolve via the `Esri.ArcGISPro.Extensions30` NuGet fallback (since ArcGIS Pro is not installed locally).
- **`dotnet build`** compiles the C# code and produces the DLL successfully, but the ArcGIS Pro SDK's post-build packaging step (`ConvertToRelativePath` in `Esri.ArcGISPro.Extensions30.targets`) fails because it uses `CodeTaskFactory`, a Windows-only MSBuild feature. This is expected — the DLL is still produced.
- **`dotnet build /t:Compile`** runs the compilation only (no packaging) and succeeds with exit code 0. Use this to verify code compiles without errors.

### Key Commands

| Action | Command |
|--------|---------|
| Restore packages | `dotnet restore DuckDBGeoparquet.sln` |
| Compile (clean exit) | `dotnet build DuckDBGeoparquet.csproj --configuration Debug /t:Compile` |
| Full build (DLL produced, packaging fails on Linux) | `dotnet build DuckDBGeoparquet.sln --configuration Debug` |

### Limitations

- **No tests**: The repository has no test project or test framework. The README mentions tests in contributing guidelines, but none exist.
- **No linter**: No code analysis tools (StyleCop, Roslyn analyzers, etc.) are configured.
- **Cannot run the application**: The add-in requires ArcGIS Pro 3.5+ on Windows. It cannot be launched or tested end-to-end on Linux.
- **Full build packaging**: `dotnet build` reports errors from the ArcGIS SDK packaging step on Linux, but the actual C# compilation succeeds and the DLL is produced in `bin/{Configuration}/net8.0-windows/`.

### .NET SDK

Requires .NET 8.0 SDK. Installed at `/usr/share/dotnet`. PATH and DOTNET_ROOT are configured in `~/.bashrc`.
