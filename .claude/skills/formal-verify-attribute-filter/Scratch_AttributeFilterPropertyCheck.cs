// Property check for the attribute-filter trust boundary. Lives in
// .claude/skills/formal-verify-attribute-filter/ and is copied into
// DuckDBGeoparquet.Tests/ by that skill's run.sh for the duration of a run.
//
// Property: every filter string ValidateAttributeFilter accepts, once wrapped
// by BuildAttributeFilterPredicate and embedded in a WHERE clause, is seen by
// the real DuckDB parser as ONE SELECT statement whose AST contains no
// subquery and no call to a file/env-reading function. The oracle is DuckDB's
// own parse (json_serialize_sql), so a mismatch between the C# scanner's idea
// of string boundaries and DuckDB's lexer cannot hide from it.
using DuckDB.NET.Data;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.Text.Json;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public sealed class Scratch_AttributeFilterPropertyCheck : IDisposable
    {
        private readonly DuckDBConnection _connection = new("DataSource=:memory:");

        public Scratch_AttributeFilterPropertyCheck()
        {
            _connection.Open();
            try { Sql("LOAD json"); }
            catch (DuckDBException) { Sql("INSTALL json; LOAD json"); }
        }

        private void Sql(string sql) { using var c = _connection.CreateCommand(); c.CommandText = sql; c.ExecuteNonQuery(); }

        // Functions that read files, environment, or settings. Mirrors the
        // function section of GeoParquetSql.AttributeFilterDeniedWords; if a
        // word is added there, add it here.
        private static readonly HashSet<string> DangerousFunctions = new(StringComparer.OrdinalIgnoreCase)
        {
            "read_parquet", "parquet_scan", "read_csv", "read_csv_auto",
            "read_json", "read_json_auto", "read_json_objects", "read_text",
            "read_blob", "read_xlsx", "st_read", "glob", "getenv", "sniff_csv",
            "parquet_metadata", "parquet_schema", "parquet_kv_metadata",
            "parquet_file_metadata", "database_size", "current_setting"
        };

        // Plausible filters a user would type (mostly accepted).
        private static readonly string[] Atoms =
        {
            "class = 'primary'", "1=1", "subtype = 'road'", "lower(id) = 'x'",
            "id IS NOT NULL", "len(id) > 3", "speed BETWEEN 1 AND 99",
            "name LIKE 'Main%'", "class IN ('a', 'b')", "\"class\" = 'x'",
            "height > 10.5", "class <> 'service'", "name = 'O''Brien'",
        };

        // Adversarial fragments: quote/lexer tricks, smuggled statements,
        // denied words in odd case, paren escapes, dangerous functions.
        private static readonly string[] Nasty =
        {
            ";", "--", "/*", "*/", "$", "$tag$", "E'\\''", "e'\\''", "'it''s'",
            "’", "“", "”", "select 1", "SeLeCt 1", "(SELECT 1)",
            "getenv('HOME')", "GETENV('HOME')", "gEtEnV('HOME') = 'x'",
            "read_parquet('/etc/passwd')", "read_csv('x')", "st_read('x')",
            "glob('*')", "parquet_metadata('x')", "current_setting('x')",
            "read_text('/etc/hosts')", "union all select 1",
            "); from t2; (", "1=1); from t2; (1=1", "1=1)) ; from t2; --",
            "\n", "\t", "\\", "''", "'", "\"", "(", ")",
            "x = E'\\'; getenv(''HOME''); --'",
            "x = '\\'; getenv(''HOME''); --'",
            "with q as (select 1) select * from q",
            "attach 'f' as d", "install httpfs", "pragma database_list",
            "WHERE class = 'x'", "where 1=1",
        };

        private static string Gen(Random r, int depth)
        {
            switch (r.Next(12))
            {
                case 0: case 1: case 2: case 3:
                    return Atoms[r.Next(Atoms.Length)];
                case 4: case 5: case 6:
                    return Nasty[r.Next(Nasty.Length)];
                case 7:
                    return depth > 2 ? Atoms[r.Next(Atoms.Length)] : "(" + Gen(r, depth + 1) + ")";
                case 8:
                    return depth > 2 ? Nasty[r.Next(Nasty.Length)] : Gen(r, depth + 1) + " AND " + Gen(r, depth + 1);
                case 9:
                    return depth > 2 ? Atoms[r.Next(Atoms.Length)] : Gen(r, depth + 1) + " OR " + Gen(r, depth + 1);
                case 10:
                    return depth > 2 ? Nasty[r.Next(Nasty.Length)] : Gen(r, depth + 1) + Nasty[r.Next(Nasty.Length)] + Gen(r, depth + 1);
                default:
                    return depth > 2 ? Atoms[r.Next(Atoms.Length)] : Gen(r, depth + 1) + " " + Gen(r, depth + 1);
            }
        }

        private string SerializeSql(string query)
        {
            using var c = _connection.CreateCommand();
            c.CommandText = "SELECT json_serialize_sql(?::VARCHAR)";
            c.Parameters.Add(new DuckDBParameter(query));
            return (string)c.ExecuteScalar();
        }

        /// <summary>Walks the parsed AST; returns a violation description or null.</summary>
        private static string FindViolation(JsonElement node)
        {
            if (node.ValueKind == JsonValueKind.Object)
            {
                if (node.TryGetProperty("type", out var t) && t.ValueKind == JsonValueKind.String &&
                    t.GetString() == "SUBQUERY")
                    return "AST contains a subquery";
                if (node.TryGetProperty("function_name", out var f) && f.ValueKind == JsonValueKind.String &&
                    DangerousFunctions.Contains(f.GetString()))
                    return $"AST calls dangerous function {f.GetString()}";
                foreach (var property in node.EnumerateObject())
                {
                    string v = FindViolation(property.Value);
                    if (v != null) return v;
                }
            }
            else if (node.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in node.EnumerateArray())
                {
                    string v = FindViolation(item);
                    if (v != null) return v;
                }
            }
            return null;
        }

        [Fact]
        public void AcceptedFilters_ParseAsOneSafeSelect()
        {
            int iterations = int.TryParse(Environment.GetEnvironmentVariable("FILTER_FUZZ_ITERATIONS"), out var n) ? n : 20000;
            int seed = int.TryParse(Environment.GetEnvironmentVariable("FILTER_FUZZ_SEED"), out var s) ? s : 20261121;
            var rng = new Random(seed);
            int accepted = 0, parsed = 0;

            for (int i = 0; i < iterations; i++)
            {
                string filter = Gen(rng, 0);
                if (GeoParquetSql.ValidateAttributeFilter(filter) != null) continue;
                accepted++;

                // Consistency: the backstop must agree with the validator.
                string predicate;
                try { predicate = GeoParquetSql.BuildAttributeFilterPredicate(filter); }
                catch (ArgumentException e)
                {
                    throw new Xunit.Sdk.XunitException(
                        $"Validator accepted but backstop threw.\nFilter: {filter}\nBackstop: {e.Message}");
                }
                if (predicate == null) continue;

                // Worst-case embedding: predicate at the very end of the query,
                // so an unbalanced-paren/comment trick has the most room.
                string embedded = "SELECT 1 WHERE " + predicate;
                using var doc = JsonDocument.Parse(SerializeSql(embedded));
                var root = doc.RootElement;

                // Accepted but unparseable = the query merely fails at run
                // time. Safe: nothing executes.
                if (root.GetProperty("error").GetBoolean()) continue;
                parsed++;

                var statements = root.GetProperty("statements");
                if (statements.GetArrayLength() != 1)
                    throw new Xunit.Sdk.XunitException(
                        $"Accepted filter smuggled extra statements ({statements.GetArrayLength()}).\nFilter: {filter}\nEmbedded: {embedded}");

                string violation = FindViolation(statements);
                if (violation != null)
                    throw new Xunit.Sdk.XunitException(
                        $"Accepted filter violates the property: {violation}.\nFilter: {filter}\nEmbedded: {embedded}");
            }

            // The run must genuinely exercise the accept side, or a generator
            // regression could silently turn the check into a no-op.
            Assert.True(accepted >= iterations / 20,
                $"Generator too hostile: only {accepted} of {iterations} inputs were accepted.");
            Assert.True(parsed >= accepted / 2,
                $"Too few accepted filters parsed ({parsed} of {accepted}); embedding may be broken.");
        }

        public void Dispose() => _connection.Dispose();
    }
}
