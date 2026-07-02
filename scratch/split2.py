import os

filepath = 'Views/WizardDockpaneViewModel.cs'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

usings = "".join(lines[0:30])
namespace_start = "namespace DuckDBGeoparquet.Views\n{\n    internal partial class WizardDockpaneViewModel : DockPane\n    {\n"
namespace_end = "    }\n}\n"

# Helper Methods: 916-1318 -> indices 915-1318
helpers_content = "".join(lines[915:1318])
# Commands: 902-914 -> indices 901-914
commands_content = "".join(lines[901:914])
# Properties: 349-799 -> indices 348-799
properties_content = "".join(lines[348:799])

with open('Views/WizardDockpaneViewModel.Helpers.cs', 'w', encoding='utf-8') as f:
    f.write(usings + "\n" + namespace_start + helpers_content + namespace_end)

with open('Views/WizardDockpaneViewModel.Commands.cs', 'w', encoding='utf-8') as f:
    f.write(usings + "\n" + namespace_start + commands_content + namespace_end)

with open('Views/WizardDockpaneViewModel.Properties.cs', 'w', encoding='utf-8') as f:
    f.write(usings + "\n" + namespace_start + properties_content + namespace_end)

# Build new file by removing the regions
new_lines = lines[:348] + lines[799:901] + lines[914:915] + lines[1318:]
with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("Split completed.")
