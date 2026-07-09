using DuckDBGeoparquet.Models;
using System.Collections.Generic;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class OvertureSchemaTests
    {
        [Fact]
        public void BuildCompatibilityReport_ReportsMissingUnknownAndFlattenedFields()
        {
            var columns = new List<string> { "id", "bbox", "geometry", "theme", "type", "version", "surprise_column" };
            var flattened = new List<string> { "names_primary", "cartography_min_zoom" };

            var report = OvertureSchema.BuildCompatibilityReport("division", columns, flattened);

            Assert.Equal(OvertureSchema.ReferenceVersion, report.ReferenceSchemaVersion);
            Assert.Equal("division", report.DatasetType);
            Assert.Contains("names", report.MissingExpectedColumns);
            Assert.Contains("cartography", report.MissingExpectedColumns);
            Assert.Contains("surprise_column", report.UnknownColumns);
            Assert.Contains("names_primary", report.FlattenedFields);
            Assert.Contains("cartography_min_zoom", report.FlattenedFields);
            Assert.True(report.HasFindings);
        }

        [Fact]
        public void BuildCompatibilityReport_ForUnknownType_StillReportsFlattenedFields()
        {
            var report = OvertureSchema.BuildCompatibilityReport(
                "custom_type",
                new List<string> { "id", "geometry" },
                new List<string> { "display_name" });

            Assert.Empty(report.MissingExpectedColumns);
            Assert.Empty(report.UnknownColumns);
            Assert.Contains("display_name", report.FlattenedFields);
        }
    }
}
