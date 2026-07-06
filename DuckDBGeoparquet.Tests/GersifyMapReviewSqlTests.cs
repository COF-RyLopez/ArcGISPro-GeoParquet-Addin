using DuckDBGeoparquet.Services;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class GersifyMapReviewSqlTests
    {
        [Fact]
        public void BuildUnmatchedWhereClause_FlagsNullOrBlankGersId()
        {
            string where = GersifyMapReviewSql.BuildUnmatchedWhereClause("gers_id");

            Assert.Contains("gers_id IS NULL", where);
            Assert.Contains("TRIM(gers_id) = ''", where);
        }

        [Fact]
        public void BuildWeakLinksWhereClause_UsesAcceptThresholdBand()
        {
            string where = GersifyMapReviewSql.BuildWeakLinksWhereClause("gers_id", "gers_match_score", 72, 8);

            Assert.Contains("gers_match_score >= 72", where);
            Assert.Contains("gers_match_score < 80", where);
            Assert.Contains("gers_id IS NOT NULL", where);
        }

        [Fact]
        public void BuildSourceKeysWhereClause_EscapesSingleQuotes()
        {
            string where = GersifyMapReviewSql.BuildSourceKeysWhereClause("site_id", ["A-1", "O'Brien"]);

            Assert.Contains("'A-1'", where);
            Assert.Contains("'O''Brien'", where);
        }

        [Fact]
        public void BuildWeakLinksWhereClause_UsesReviewFieldWhenPresent()
        {
            string where = GersifyMapReviewSql.BuildWeakLinksWhereClause(
                "gers_id",
                "gers_match_score",
                72,
                linkReviewField: "gers_link_review");

            Assert.Equal("gers_link_review = 'weak'", where);
        }

        [Fact]
        public void BuildUnmatchedWhereClause_UsesReviewFieldWhenPresent()
        {
            string where = GersifyMapReviewSql.BuildUnmatchedWhereClause("gers_id", "gers_link_review");
            Assert.Equal("gers_link_review = 'unmatched'", where);
        }
    }
}
