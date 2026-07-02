import os

filepath = 'Views/WizardDockpaneViewModel.cs'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Remove lines 33 to 207 (inclusive) from the file
# In 0-indexed list, that's indices 32 to 206, so we slice up to 32 and from 207 onwards
new_lines = lines[:32] + lines[207:]

with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
