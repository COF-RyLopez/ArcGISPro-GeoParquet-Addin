import re
import os

filepath = 'Views/WizardDockpaneViewModel.cs'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

usings = []
idx = 0
while idx < len(lines):
    if lines[idx].strip() == 'namespace DuckDBGeoparquet.Views':
        break
    usings.append(lines[idx])
    idx += 1

usings = "".join(usings)
namespace_start = "namespace DuckDBGeoparquet.Views\n{\n    internal partial class WizardDockpaneViewModel : DockPane\n    {\n"
namespace_end = "    }\n}\n"

def extract_and_remove(lines, patterns):
    extracted = []
    new_lines = []
    in_block = False
    brace_count = 0
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check if we should start a block
        if not in_block:
            for p in patterns:
                if re.search(p, line):
                    in_block = True
                    brace_count = 0
                    extracted.append(line)
                    break
            if not in_block:
                new_lines.append(line)
        else:
            extracted.append(line)
            
        if in_block:
            brace_count += line.count('{')
            brace_count -= line.count('}')
            # Stop if block closes
            if brace_count == 0 and '{' in "".join(extracted[-5:]): # ensure we saw at least one brace
                in_block = False
                
        i += 1
        
    return extracted, new_lines

def write_partial(name, content):
    if not content: return
    with open(f'Views/WizardDockpaneViewModel.{name}.cs', 'w', encoding='utf-8') as f:
        f.write(usings + namespace_start + "".join(content) + namespace_end)

# Patterns for DataLoading
data_patterns = [
    r'private async Task<string> GetLatestRelease',
    r'private async Task StartHeartbeatAsync',
    r'private async Task PerformBulkDataReplacementAsync',
    r'private static FileDeleteResult TryDeleteParquetFileQuiet',
    r'private static bool IsLockWin32Error',
    r'private async Task RemoveLayersUsingFolderAsync',
    r'private async Task<Envelope> GetLoadExtentAsync',
    r'private async Task<bool> CheckAndReplaceExistingDataAsync',
    r'private async Task<bool> LoadAndCreateLayersForItemAsync',
    r'private async Task LoadOvertureDataAsync'
]

# Patterns for MfcWorkflow
mfc_patterns = [
    r'private string ResolveDataFolder',
    r'private bool ValidateDataFolder',
    r'private async Task CreateMfcAsync',
    r'private class ReleaseInfo'
]

# Patterns for Styling
styling_patterns = [
    r'private async Task ApplyMapStyleToExistingLayersAsync',
    r'private async Task RepairMapSymbologyAsync'
]

# Patterns for UIHelpers
ui_patterns = [
    r'private string LoadCachedLatestRelease',
    r'private void SaveCachedLatestRelease',
    r'private string GetLatestReleaseCacheFilePath',
    r'private void UpdatePathsFromRelease',
    r'private async Task RefreshLatestReleaseAsync',
    r'private void ApplyManualRelease',
    r'private void AddToLog',
    r'private void UpdateThemePreview',
    r'private void ShowThemeInfo',
    r'private void SetCustomExtent',
    r'private void OnExtentCreated',
    r'private void BrowseMfcLocation',
    r'private void BrowseDataLocation',
    r'private void BrowseCustomDataFolder',
    r'private void UpdateCustomExtentDisplay',
    r'internal static void Show',
    r'public bool IsThemeSelected',
    r'public void ToggleThemeSelection',
    r'private void CheckInitialThemeSelection',
    r'private void CleanupResources',
    r'private void OnThemeSelectionChanged',
    r'private void ResetState',
    r'private void ShowCreateMfcTab',
    r'private static string MakeFriendlyName',
    r'private void InitializeThemes',
    r'private List<SelectableThemeItem> GetSelectedLeafItems',
    r'private void OnLeafThemeSelectionChanged',
    r'private void ExecuteSelectAllInternal',
    r'private void UpdateIsSelectAllCheckedStatus',
    r'private List<SelectableThemeItem> GetAllLeafDataItems',
    r'private void RefreshCacheInfo',
    r'private void ClearCache'
]

data_ext, lines = extract_and_remove(lines, data_patterns)
write_partial('DataLoading', data_ext)

mfc_ext, lines = extract_and_remove(lines, mfc_patterns)
write_partial('MfcWorkflow', mfc_ext)

styling_ext, lines = extract_and_remove(lines, styling_patterns)
write_partial('Styling', styling_ext)

ui_ext, lines = extract_and_remove(lines, ui_patterns)
write_partial('UIHelpers', ui_ext)

# Write back the main file
with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("Done extracting methods!")
