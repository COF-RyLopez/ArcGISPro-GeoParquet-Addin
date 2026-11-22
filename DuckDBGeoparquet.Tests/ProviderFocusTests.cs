using System.Collections.ObjectModel;
using DuckDBGeoparquet.Views;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    /// <summary>
    /// Tests for SelectableThemeItem.FindGroup, the ArcGIS-free core of the
    /// ribbon's per-provider FocusThemeGroup behavior: it must find a provider's
    /// parent node by theme key, and expanding it must not touch selection.
    /// </summary>
    public class ProviderFocusTests
    {
        private static SelectableThemeItem Group(string displayName, string themeKey, params string[] leaves)
        {
            var parent = new SelectableThemeItem(displayName, themeKey, themeKey, false);
            foreach (var leaf in leaves)
            {
                var child = new SelectableThemeItem(leaf, leaf, themeKey, true) { Parent = parent };
                parent.SubItems.Add(child);
            }
            return parent;
        }

        private static ObservableCollection<SelectableThemeItem> Tree() =>
            new()
            {
                Group("Buildings", "buildings", "building", "building_part"),
                Group("OpenStreetMap (LayerCake)", "osm", "OSM Buildings", "OSM Highways"),
                Group("Source Cooperative", "sourcecoop", "VIDA Buildings"),
                Group("Custom Sources", "custom", "My Source"),
            };

        [Theory]
        [InlineData("osm", "OpenStreetMap (LayerCake)")]
        [InlineData("sourcecoop", "Source Cooperative")]
        [InlineData("custom", "Custom Sources")]
        public void FindGroup_FindsProviderParentByThemeKey(string themeKey, string expectedDisplayName)
        {
            var found = SelectableThemeItem.FindGroup(Tree(), themeKey);
            Assert.NotNull(found);
            Assert.Equal(expectedDisplayName, found.DisplayName);
            Assert.True(found.IsExpandable);
        }

        [Fact]
        public void FindGroup_ReturnsNullWhenGroupAbsent()
        {
            var themes = Tree();
            themes.RemoveAt(3); // no custom sources saved
            Assert.Null(SelectableThemeItem.FindGroup(themes, "custom"));
        }

        [Fact]
        public void FindGroup_ReturnsNullForNullInputs()
        {
            Assert.Null(SelectableThemeItem.FindGroup(null, "osm"));
            Assert.Null(SelectableThemeItem.FindGroup(Tree(), null));
            Assert.Null(SelectableThemeItem.FindGroup(Tree(), ""));
        }

        private static SelectableThemeItem WithSource(SelectableThemeItem leaf)
        {
            leaf.UserSource = new DuckDBGeoparquet.Models.UserDataSource { Name = leaf.DisplayName, Url = "https://example.test/x.parquet" };
            return leaf;
        }

        private static ObservableCollection<SelectableThemeItem> MixedTree()
        {
            var tree = Tree();
            foreach (var parent in tree)
                if (parent.ParentThemeForS3 is "osm" or "sourcecoop" or "custom")
                    foreach (var leaf in parent.SubItems) WithSource(leaf);
            return tree;
        }

        [Fact]
        public void ReleaseVisible_WhenOvertureLeafSelected()
        {
            var tree = MixedTree();
            tree[0].SubItems[0].IsSelected = true; // Overture leaf
            tree[1].SubItems[0].IsSelected = true; // preset leaf too
            Assert.True(SelectableThemeItem.DecideOvertureReleaseVisible(tree, current: false));
        }

        [Fact]
        public void ReleaseHidden_WhenOnlyPresetOrCustomLeavesSelected()
        {
            var tree = MixedTree();
            tree[1].SubItems[0].IsSelected = true; // OSM preset
            tree[3].SubItems[0].IsSelected = true; // custom source
            Assert.False(SelectableThemeItem.DecideOvertureReleaseVisible(tree, current: true));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void ReleaseVisibility_EmptySelectionKeepsCurrentState(bool current)
        {
            Assert.Equal(current, SelectableThemeItem.DecideOvertureReleaseVisible(MixedTree(), current));
            Assert.Equal(current, SelectableThemeItem.DecideOvertureReleaseVisible(null, current));
        }

        [Fact]
        public void ReleaseVisible_WhenSelectedTopLevelLeafIsOverture()
        {
            var tree = MixedTree();
            // A parent that is itself a leaf (e.g. an Overture theme with no sub-types)
            var leafParent = new SelectableThemeItem("Addresses", "address", "addresses", true) { IsSelected = true };
            tree.Add(leafParent);
            Assert.True(SelectableThemeItem.DecideOvertureReleaseVisible(tree, current: false));
        }

        [Fact]
        public void ExpandingFoundGroup_DoesNotChangeSelection()
        {
            var themes = Tree();
            // Pre-select one leaf elsewhere to prove selection state is untouched
            themes[0].SubItems[0].IsSelected = true;

            var group = SelectableThemeItem.FindGroup(themes, "osm");
            group.IsExpanded = true;

            Assert.True(group.IsExpanded);
            Assert.Equal(false, group.IsSelected);
            Assert.All(group.SubItems, leaf => Assert.Equal(false, leaf.IsSelected));
            Assert.Equal(true, themes[0].SubItems[0].IsSelected);
            // And no other group was collapsed or expanded
            Assert.False(themes[0].IsExpanded);
            Assert.False(themes[2].IsExpanded);
        }
    }
}
