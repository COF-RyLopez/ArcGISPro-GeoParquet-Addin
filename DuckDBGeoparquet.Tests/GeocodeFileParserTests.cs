using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GeocodeFileParserTests
    {
        [Fact]
        public void BlankAddressRows_RetainTheirSourceIdentityAndDoNotShiftLaterRows()
        {
            var parsed = GeocodeFileParser.Parse(new[] { "id,address,city", "a,12 Main St,Fresno", "b,,Fresno", "c,14 Main St,Fresno" }, 100);
            Assert.Equal(3, parsed.Queries.Count);
            Assert.Equal("b", parsed.Queries[1].SourceId);
            Assert.Equal("", parsed.Queries[1].SourceQuery);
            Assert.Equal(3, parsed.Queries[1].SourceLine);
            Assert.Equal("b,,Fresno", parsed.Queries[1].RawInput);
            Assert.Equal(4, parsed.Queries[2].SourceLine);
            Assert.Equal("c", parsed.Queries[2].SourceId);
            Assert.Contains("Fresno", parsed.Queries[2].SourceQuery);
        }
        [Theory]
        [InlineData("12 Main St", "Apt 2")]
        [InlineData("12 County Road 2", "2")]
        public void SeparateUnit_CannotBeMatchedToBaseBuilding(string address, string unit)
        {
            var parsed = GeocodeFileParser.Parse(new[] { "address,unit,city", $"{address},{unit},Fresno" }, 100);
            string query = Assert.Single(parsed.Queries).SourceQuery;
            Assert.Contains(address + " " + unit, query);
            var decision = GeocodeAcceptance.Evaluate(query, new[] { new DuckDBGeoparquet.Models.GeocodeCandidate
                { DisplayLabel = address + ", Fresno", Longitude = -119, Latitude = 36 } });
            Assert.Null(decision.Match);
        }
        [Fact]
        public void QuotedMultilineField_IsOneRecordWithOriginalStartingLine()
        {
            var parsed = GeocodeFileParser.Parse(new[] { "id,address,city", "a,\"12 Main", "St\",Fresno", "b,14 Main St,Fresno" }, 100);
            Assert.Equal(2, parsed.Queries.Count);
            Assert.Equal(2, parsed.Queries[0].SourceLine);
            Assert.Equal(4, parsed.Queries[1].SourceLine);
            Assert.Contains("12 Main St", parsed.Queries[0].SourceQuery);
            Assert.Contains("\n", parsed.Queries[0].RawInput);
        }

        [Fact]
        public void Limits_IncludeUnmatchedRowsAndReportTruncation()
        {
            var parsed = GeocodeFileParser.Parse(new[] { "address", "", "12 Main St" }, 1);
            Assert.Single(parsed.Queries);
            Assert.True(parsed.WasTruncated);
            Assert.Empty(GeocodeFileParser.Parse(System.Array.Empty<string>(), 100).Queries);
        }
    }
}
