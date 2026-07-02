import os

content = """using System;
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
"""

with open('Views/SelectableThemeItem.cs', 'w', encoding='utf-8') as f:
    f.write(content)
