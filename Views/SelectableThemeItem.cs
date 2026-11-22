using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Tree node representing an Overture Maps theme or sub-type.
    /// Supports tri-state selection with parent/child propagation for the theme TreeView.
    /// </summary>
    public class SelectableThemeItem : INotifyPropertyChanged
    {
        private string _displayName;
        private bool? _isSelected; // Changed to nullable bool
        private string _actualType;
        private string _parentThemeForS3;
        private bool _isExpanded;
        private bool _isUpdatingSubItems = false; // Flag to prevent loops when parent updates children

        public SelectableThemeItem Parent { get; internal set; } // Property to hold the parent

        /// <summary>Set when this leaf is a user-saved custom GeoParquet source.</summary>
        public Models.UserDataSource UserSource { get; set; }

        public string DisplayName
        {
            get => _displayName;
            set
            {
                if (_displayName != value)
                {
                    _displayName = value;
                    OnPropertyChanged();
                }
            }
        }

        public bool? IsSelected // Changed to nullable bool
        {
            get => _isSelected;
            set // 'value' here is what the XAML CheckBox binding is trying to set
            {
                bool? determinedNewState = value; // Start with what the UI is suggesting

                if (IsExpandable) // Special handling for parent node clicks
                {
                    if (_isSelected == true && value == null)
                    {
                        determinedNewState = false;
                    }
                }

                if (_isSelected != determinedNewState)
                {
                    _isSelected = determinedNewState;

                    if (IsExpandable && _isSelected.HasValue && !_isUpdatingSubItems)
                    {
                        _isUpdatingSubItems = true;
                        foreach (var subItem in SubItems)
                        {
                            subItem.IsSelected = _isSelected;
                        }
                        _isUpdatingSubItems = false;
                    }

                    if (IsExpandable)
                    {
                        if (_isSelected == true)
                        {
                            IsExpanded = true;
                        }
                        else if (_isSelected == false)
                        {
                            IsExpanded = false;
                        }
                    }

                    OnPropertyChanged();
                    SelectionChanged?.Invoke(this, EventArgs.Empty);

                    Parent?.UpdateSelectionStateFromChildren();
                }
            }
        }

        public string ActualType
        {
            get => _actualType;
            set => _actualType = value;
        }

        public string ParentThemeForS3
        {
            get => _parentThemeForS3;
            private set => _parentThemeForS3 = value;
        }

        public ObservableCollection<SelectableThemeItem> SubItems { get; }
        public bool IsExpandable => SubItems.Any();
        public bool IsSelectable { get; } // True if it's a leaf node

        public bool IsExpanded
        {
            get => _isExpanded;
            set
            {
                if (_isExpanded != value && IsExpandable)
                {
                    _isExpanded = value;
                    OnPropertyChanged();
                }
            }
        }

        public event EventHandler SelectionChanged;
        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        public SelectableThemeItem(string displayName, string actualType, string parentThemeForS3, bool isLeafNode = true)
        {
            DisplayName = displayName;
            ActualType = actualType;
            ParentThemeForS3 = parentThemeForS3;
            SubItems = [];
            IsSelectable = isLeafNode;
            _isSelected = false;
            _isExpanded = false;
        }

        /// <summary>
        /// Finds the expandable parent node for a provider theme key ("osm",
        /// "sourcecoop", "custom"). Null when absent (e.g. no custom sources saved).
        /// </summary>
        public static SelectableThemeItem FindGroup(IEnumerable<SelectableThemeItem> themes, string themeKey)
        {
            if (themes == null || string.IsNullOrEmpty(themeKey)) return null;
            return themes.FirstOrDefault(t => t.IsExpandable && t.ParentThemeForS3 == themeKey);
        }

        /// <summary>
        /// Whether the Overture release header should be visible for the given
        /// selection: any selected Overture leaf (no UserSource) shows it, a
        /// selection of only preset/custom leaves hides it, and an empty
        /// selection keeps the current state.
        /// </summary>
        public static bool DecideOvertureReleaseVisible(IEnumerable<SelectableThemeItem> themes, bool current)
        {
            if (themes == null) return current;
            bool anyOther = false;
            foreach (var parent in themes)
            {
                var leaves = parent.IsSelectable ? new[] { parent } : (IEnumerable<SelectableThemeItem>)parent.SubItems;
                foreach (var leaf in leaves)
                {
                    if (leaf.IsSelected != true) continue;
                    if (leaf.UserSource == null) return true; // Overture leaf selected
                    anyOther = true;
                }
            }
            return anyOther ? false : current;
        }

        private bool _isVisible = true;

        /// <summary>
        /// Whether this top-level node is shown in the theme tree under the
        /// current provider filter (see DecideProviderVisible). Sub-items are
        /// always visible; their parent's visibility hides them.
        /// </summary>
        public bool IsVisible
        {
            get => _isVisible;
            set
            {
                if (_isVisible != value)
                {
                    _isVisible = value;
                    OnPropertyChanged();
                }
            }
        }

        /// <summary>Filter key meaning "Overture themes only" (the non-group top-level nodes).</summary>
        public const string OvertureFilterKey = "overture";

        /// <summary>
        /// Provider filter rule for a top-level tree node: null shows everything,
        /// "overture" shows only the Overture theme nodes, and a group key
        /// ("osm", "sourcecoop", "custom") shows only that provider's group.
        /// </summary>
        public static bool DecideProviderVisible(SelectableThemeItem item, string filterKey)
        {
            if (item == null) return false;
            if (string.IsNullOrEmpty(filterKey)) return true;
            // "custom" mirrors WizardDockpaneViewModel.CustomSourcesThemeKey; the VM
            // is not linked into the ArcGIS-free test build, so the literal lives here.
            bool isProviderGroup = item.ParentThemeForS3 == Services.PresetCatalog.OsmThemeKey
                || item.ParentThemeForS3 == Services.PresetCatalog.SourceCoopThemeKey
                || item.ParentThemeForS3 == "custom";
            return filterKey == OvertureFilterKey ? !isProviderGroup : item.ParentThemeForS3 == filterKey;
        }

        /// <summary>
        /// Folder-safe provider tag for a download's selected leaves:
        /// "Overture", "OSM", "SourceCoop", "Custom", or "Mixed" when the
        /// selection spans providers. Snapshot folders and Contents group
        /// layers are named from this (see DownloadSnapshot).
        /// </summary>
        public static string ComputeProviderTag(IEnumerable<SelectableThemeItem> selectedLeaves)
        {
            string tag = null;
            foreach (var leaf in selectedLeaves ?? Enumerable.Empty<SelectableThemeItem>())
            {
                string leafTag = leaf.UserSource == null ? "Overture"
                    : leaf.ParentThemeForS3 == Services.PresetCatalog.OsmThemeKey ? "OSM"
                    : leaf.ParentThemeForS3 == Services.PresetCatalog.SourceCoopThemeKey ? "SourceCoop"
                    : "Custom";
                if (tag == null) tag = leafTag;
                else if (tag != leafTag) return "Mixed";
            }
            return tag ?? "Overture";
        }

        internal void UpdateSelectionStateFromChildren()
        {
            if (!IsExpandable || _isUpdatingSubItems)
                return;

            bool? newSelectionState = CalculateSelectionStateFromChildren();

            if (_isSelected != newSelectionState)
            {
                _isSelected = newSelectionState;
                OnPropertyChanged(nameof(IsSelected));
            }
        }

        private bool? CalculateSelectionStateFromChildren()
        {
            if (!SubItems.Any())
                return false;

            bool allTrue = true;
            bool allFalse = true;

            foreach (var subItem in SubItems)
            {
                if (subItem.IsSelected != true) allTrue = false;
                if (subItem.IsSelected != false) allFalse = false;
            }

            if (allTrue) return true;
            if (allFalse) return false;
            return null;
        }
    }
}
