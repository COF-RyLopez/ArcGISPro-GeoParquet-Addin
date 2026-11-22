# Shared UIA helper: resolve the ArcGIS Pro main window.
# Dot-source from other helpers. Never calls Toggle/Expand patterns.

Add-Type -AssemblyName UIAutomationClient -ErrorAction Stop
Add-Type -AssemblyName UIAutomationTypes -ErrorAction Stop

function Get-ProWindow {
  $root = [System.Windows.Automation.AutomationElement]::RootElement
  $preferred = $null
  $fallback = $null
  foreach ($w in $root.FindAll(
      [System.Windows.Automation.TreeScope]::Children,
      [System.Windows.Automation.Condition]::TrueCondition)) {
    $n = $w.Current.Name
    if ($n -eq 'MyProject2') { return $w }
    if ($n -and $n -ne 'ArcGIS Pro' -and $n -notmatch '^(Cursor|Visual Studio|ChatGPT|PowerShell)') {
      # Project windows often use the .aprx base name as title.
      if ($n -match 'Project|MyProject' -or (Get-Process -Id $w.Current.ProcessId -ErrorAction SilentlyContinue).ProcessName -eq 'ArcGISPro') {
        if ((Get-Process -Id $w.Current.ProcessId -ErrorAction SilentlyContinue).ProcessName -eq 'ArcGISPro') {
          if ($n -eq 'ArcGIS Pro') { $fallback = $w } else { $preferred = $w }
        }
      }
    }
    if ($n -eq 'ArcGIS Pro') {
      $proc = Get-Process -Id $w.Current.ProcessId -ErrorAction SilentlyContinue
      if ($proc -and $proc.ProcessName -eq 'ArcGISPro') { $fallback = $w }
    }
  }
  if ($preferred) { return $preferred }
  return $fallback
}

function Find-Named([System.Windows.Automation.AutomationElement]$root, [string]$name, $controlType = $null) {
  $nc = New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::NameProperty, $name)
  if ($null -ne $controlType) {
    $cc = New-Object System.Windows.Automation.PropertyCondition(
      [System.Windows.Automation.AutomationElement]::ControlTypeProperty, $controlType)
    return $root.FindFirst(
      [System.Windows.Automation.TreeScope]::Descendants,
      (New-Object System.Windows.Automation.AndCondition($cc, $nc)))
  }
  return $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $nc)
}

function Invoke-NamedButton([System.Windows.Automation.AutomationElement]$root, [string]$name) {
  $btn = Find-Named $root $name ([System.Windows.Automation.ControlType]::Button)
  if (-not $btn) { throw "Button not found: $name" }
  $btn.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern).Invoke()
}
