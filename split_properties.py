import os

filepath = 'Views/WizardDockpaneViewModel.cs'
with open(filepath, 'r') as f:
    lines = f.readlines()

usings = "".join(lines[0:32])
namespace_start = "    internal partial class WizardDockpaneViewModel\n    {\n"
properties_content = "".join(lines[348:799])
namespace_end = "    }\n}\n"

with open('Views/WizardDockpaneViewModel.Properties.cs', 'w') as f:
    f.write(usings + namespace_start + properties_content + namespace_end)

# Remove properties from original file
new_lines = lines[:348] + lines[799:]
with open(filepath, 'w') as f:
    f.writelines(new_lines)
