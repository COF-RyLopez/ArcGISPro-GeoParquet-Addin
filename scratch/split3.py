import os

filepath = 'Views/WizardDockpaneViewModel.cs'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Dynamic finding
def find_block(start_marker, end_marker):
    start_idx = -1
    end_idx = -1
    for i, line in enumerate(lines):
        if start_marker in line and start_idx == -1:
            start_idx = i
        elif end_marker in line and start_idx != -1 and end_idx == -1:
            end_idx = i
            break
    return start_idx, end_idx

prop_s, prop_e = find_block('#region Properties', '#endregion')
cmd_s, cmd_e = find_block('#region Commands', '#endregion')
hlp_s, hlp_e = find_block('#region Helper Methods', '#endregion')

# Also find SelectableThemeItem block:
sti_s, sti_e = find_block('public class SelectableThemeItem : INotifyPropertyChanged', '    internal partial class WizardDockpaneViewModel : DockPane')
if sti_s != -1:
    # We want to remove the preceding comments too
    sti_s -= 1
    sti_e -= 1 # leave the WizardDockpaneViewModel line intact

usings = "".join(lines[0:30])
namespace_start = "namespace DuckDBGeoparquet.Views\n{\n    internal partial class WizardDockpaneViewModel : DockPane\n    {\n"
namespace_end = "    }\n}\n"

with open('Views/WizardDockpaneViewModel.Properties.cs', 'w', encoding='utf-8') as f:
    f.write(usings + "\n" + namespace_start + "".join(lines[prop_s:prop_e+1]) + namespace_end)

with open('Views/WizardDockpaneViewModel.Commands.cs', 'w', encoding='utf-8') as f:
    f.write(usings + "\n" + namespace_start + "".join(lines[cmd_s:cmd_e+1]) + namespace_end)

with open('Views/WizardDockpaneViewModel.Helpers.cs', 'w', encoding='utf-8') as f:
    f.write(usings + "\n" + namespace_start + "".join(lines[hlp_s:hlp_e+1]) + namespace_end)

# Build new file by removing all these blocks (in reverse order to not mess up indices)
new_lines = lines[:]
def remove_range(s, e):
    global new_lines
    if s != -1 and e != -1:
        new_lines = new_lines[:s] + new_lines[e+1:]

remove_range(hlp_s, hlp_e)
remove_range(cmd_s, cmd_e)
remove_range(prop_s, prop_e)
remove_range(sti_s, sti_e)

with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("Split completed successfully.")
