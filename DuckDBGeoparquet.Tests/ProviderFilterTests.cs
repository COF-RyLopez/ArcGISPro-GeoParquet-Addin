using DuckDBGeoparquet.Services;
using DuckDBGeoparquet.Views;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class ProviderFilterTests
    {
        private static SelectableThemeItem Node(string key) => new(key, key, key, false);

        [Fact]
        public void NullFilter_ShowsEverything()
        {
            Assert.True(SelectableThemeItem.DecideProviderVisible(Node("buildings"), null));
            Assert.True(SelectableThemeItem.DecideProviderVisible(Node(PresetCatalog.OsmThemeKey), null));
            Assert.True(SelectableThemeItem.DecideProviderVisible(Node("custom"), ""));
        }

        [Fact]
        public void OvertureFilter_HidesProviderGroupsOnly()
        {
            Assert.True(SelectableThemeItem.DecideProviderVisible(Node("buildings"), SelectableThemeItem.OvertureFilterKey));
            Assert.True(SelectableThemeItem.DecideProviderVisible(Node("transportation"), SelectableThemeItem.OvertureFilterKey));
            Assert.False(SelectableThemeItem.DecideProviderVisible(Node(PresetCatalog.OsmThemeKey), SelectableThemeItem.OvertureFilterKey));
            Assert.False(SelectableThemeItem.DecideProviderVisible(Node(PresetCatalog.SourceCoopThemeKey), SelectableThemeItem.OvertureFilterKey));
            Assert.False(SelectableThemeItem.DecideProviderVisible(Node("custom"), SelectableThemeItem.OvertureFilterKey));
        }

        [Fact]
        public void GroupFilter_ShowsOnlyThatGroup()
        {
            Assert.True(SelectableThemeItem.DecideProviderVisible(Node(PresetCatalog.OsmThemeKey), PresetCatalog.OsmThemeKey));
            Assert.False(SelectableThemeItem.DecideProviderVisible(Node("buildings"), PresetCatalog.OsmThemeKey));
            Assert.False(SelectableThemeItem.DecideProviderVisible(Node(PresetCatalog.SourceCoopThemeKey), PresetCatalog.OsmThemeKey));
            Assert.True(SelectableThemeItem.DecideProviderVisible(Node("custom"), "custom"));
            Assert.False(SelectableThemeItem.DecideProviderVisible(null, PresetCatalog.OsmThemeKey));
        }
    }
}
