using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GersifyRecordIdPolicyTests
    {
        [Fact]
        public void SelectPreferredIdField_PrefersGlobalIdOverObjectId()
        {
            string field = GersifyRecordIdPolicy.SelectPreferredIdField(["OBJECTID", "GlobalID", "NAME"]);

            Assert.Equal("GlobalID", field);
        }

        [Fact]
        public void SelectPreferredIdField_PrefersBusinessKeyPattern()
        {
            string field = GersifyRecordIdPolicy.SelectPreferredIdField(["Shape", "SITE_ID", "OBJECTID"]);

            Assert.Equal("SITE_ID", field);
        }

        [Fact]
        public void SelectPreferredIdField_DoesNotDefaultToObjectId()
        {
            string field = GersifyRecordIdPolicy.SelectPreferredIdField(["OBJECTID", "Shape"]);

            Assert.Null(field);
        }

        [Theory]
        [InlineData("OBJECTID", true)]
        [InlineData("objectid", true)]
        [InlineData("FID", true)]
        [InlineData("GlobalID", false)]
        [InlineData("SITE_ID", false)]
        public void IsUnstableArcGisRowId_DetectsRowPointers(string fieldName, bool expected)
        {
            Assert.Equal(expected, GersifyRecordIdPolicy.IsUnstableArcGisRowId(fieldName));
        }

        [Fact]
        public void ResolveOutputRelateField_UsesSourceRecordKeyWhenGeneratingStableIds()
        {
            string field = GersifyRecordIdPolicy.ResolveOutputRelateField("SITE_ID", generateStableLinkId: true);

            Assert.Equal(GersifyRecordIdPolicy.SourceRecordKeyFieldName, field);
        }

        [Fact]
        public void ResolveOutputRelateField_UsesRecordIdForBusinessKey()
        {
            string field = GersifyRecordIdPolicy.ResolveOutputRelateField("SITE_ID", generateStableLinkId: false);

            Assert.Equal("record_id", field);
        }

        [Fact]
        public void BuildIdFieldWarnings_WarnsWhenObjectIdSelectedWithoutGeneratedIds()
        {
            var warnings = GersifyRecordIdPolicy.BuildIdFieldWarnings("OBJECTID", generateStableLinkId: false);

            Assert.Contains(warnings, warning => warning.Contains("OBJECTID", System.StringComparison.OrdinalIgnoreCase));
        }
    }
}
