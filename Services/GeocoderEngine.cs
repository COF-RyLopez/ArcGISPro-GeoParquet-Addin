using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Address/place candidate search over locally downloaded Overture
    /// GeoParquet, backed by the shared DuckDB connection. Extracted from
    /// DataProcessor (Phase 2c stage 2); query logic is unchanged.
    /// </summary>
    public sealed class GeocoderEngine
    {
        private readonly DuckDBManager _duckDb;

        public GeocoderEngine(DuckDBManager duckDb)
        {
            _duckDb = duckDb ?? throw new ArgumentNullException(nameof(duckDb));
        }

        public async Task<List<GeocodeCandidate>> SearchAddressCandidatesAsync(string parquetGlobPath, string normalizedQuery, ExtentBounds extent = null, int maxResults = 25, CancellationToken cancellationToken = default)
        {
            return await SearchLocalCandidatesAsync(parquetGlobPath, normalizedQuery, "Address", extent, maxResults, 40, cancellationToken);
        }

        public async Task<List<GeocodeCandidate>> SearchPlaceCandidatesAsync(string parquetGlobPath, string normalizedQuery, ExtentBounds extent = null, int maxResults = 25, CancellationToken cancellationToken = default)
        {
            return await SearchLocalCandidatesAsync(parquetGlobPath, normalizedQuery, "Place", extent, maxResults, 0, cancellationToken);
        }

        public async Task<List<GeocodeCandidate>> SearchLocalCandidatesAsync(
            string parquetGlobPath,
            string normalizedQuery,
            string sourceType,
            ExtentBounds extent = null,
            int maxResults = 25,
            int sourceBias = 0,
            CancellationToken cancellationToken = default)
        {
            var candidates = new List<GeocodeCandidate>();
            if (string.IsNullOrWhiteSpace(parquetGlobPath) || string.IsNullOrWhiteSpace(normalizedQuery))
            {
                return candidates;
            }

            int safeLimit = Math.Clamp(maxResults, 1, 100);
            string escapedPath = GeoParquetSql.EscapeSqlLiteral(parquetGlobPath.Replace('\\', '/'));
            string escapedQuery = GeoParquetSql.EscapeSqlLiteral(normalizedQuery);
            string escapedSourceType = GeoParquetSql.EscapeSqlLiteral(sourceType ?? "Unknown");

            using var command = _duckDb.Connection.CreateCommand();

            // Read schema once so SQL only references columns that exist.
            command.CommandText = $@"
                CREATE OR REPLACE TABLE geocoder_schema_probe AS
                SELECT * FROM read_parquet('{escapedPath}', hive_partitioning=1) LIMIT 0;
            ";
            await command.ExecuteNonQueryAsync(cancellationToken);

            command.CommandText = "DESCRIBE geocoder_schema_probe";
            var availableColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using (var schemaReader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await schemaReader.ReadAsync(cancellationToken))
                {
                    if (!schemaReader.IsDBNull(0))
                    {
                        availableColumns.Add(schemaReader.GetString(0));
                    }
                }
            }

            if (!availableColumns.Contains("geometry"))
            {
                return candidates;
            }

            static string FirstExistingExpression(HashSet<string> columns, params string[] names)
            {
                foreach (var name in names)
                {
                    if (columns.Contains(name))
                    {
                        return $"CAST({name} AS VARCHAR)";
                    }
                }

                return "''";
            }

            string numberExpr = FirstExistingExpression(availableColumns, "number", "housenumber", "house_number");
            string streetExpr = FirstExistingExpression(availableColumns, "street", "street_name", "road", "name");
            string localityExpr = FirstExistingExpression(availableColumns, "city", "locality", "district", "admin2", "subregion");
            string regionExpr = FirstExistingExpression(availableColumns, "region", "state", "province", "admin1");
            string postalExpr = FirstExistingExpression(availableColumns, "postcode", "postal_code", "zip");
            string countryExpr = FirstExistingExpression(availableColumns, "country", "country_code");
            string placeNameExpr = FirstExistingExpression(availableColumns, "name", "name_primary", "names", "brand");
            string categoryExpr = FirstExistingExpression(availableColumns, "category", "class", "subtype", "kind");

            string tokenCondition = string.Join(" AND ", normalizedQuery
                .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(token => $"search_text LIKE '%{GeoParquetSql.EscapeSqlLiteral(token)}%'"));
            if (string.IsNullOrWhiteSpace(tokenCondition))
            {
                tokenCondition = "FALSE";
            }

            string extentFilter = string.Empty;
            if (extent != null)
            {
                string xMinStr = extent.XMin.ToString("G", CultureInfo.InvariantCulture);
                string yMinStr = extent.YMin.ToString("G", CultureInfo.InvariantCulture);
                string xMaxStr = extent.XMax.ToString("G", CultureInfo.InvariantCulture);
                string yMaxStr = extent.YMax.ToString("G", CultureInfo.InvariantCulture);

                if (availableColumns.Contains("bbox"))
                {
                    extentFilter = $@"
                        AND bbox.xmin <= {xMaxStr}
                        AND bbox.xmax >= {xMinStr}
                        AND bbox.ymin <= {yMaxStr}
                        AND bbox.ymax >= {yMinStr}";
                }
                else
                {
                    string extentPolygon = $"ST_GeomFromText('POLYGON(({xMinStr} {yMinStr}, {xMaxStr} {yMinStr}, {xMaxStr} {yMaxStr}, {xMinStr} {yMaxStr}, {xMinStr} {yMinStr}))')";
                    extentFilter = $@" AND ST_Intersects(geometry, {extentPolygon})";
                }
            }

            string searchSql = $@"
                WITH prepared AS (
                    SELECT
                        trim(concat_ws(', ',
                            nullif(trim(concat_ws(' ', nullif({numberExpr}, ''), nullif({streetExpr}, ''), nullif({placeNameExpr}, ''))), ''),
                            nullif({localityExpr}, ''),
                            nullif({regionExpr}, ''),
                            nullif({postalExpr}, ''),
                            nullif({countryExpr}, '')
                        )) AS display_label,
                        CAST(ST_X(geometry) AS DOUBLE) AS longitude,
                        CAST(ST_Y(geometry) AS DOUBLE) AS latitude,
                        lower(trim(concat_ws(' ',
                            nullif({numberExpr}, ''),
                            nullif({streetExpr}, ''),
                            nullif({placeNameExpr}, ''),
                            nullif({categoryExpr}, ''),
                            nullif({localityExpr}, ''),
                            nullif({regionExpr}, ''),
                            nullif({postalExpr}, ''),
                            nullif({countryExpr}, '')
                        ))) AS search_text
                    FROM read_parquet('{escapedPath}', hive_partitioning=1)
                    WHERE geometry IS NOT NULL
                    {extentFilter}
                ),
                ranked AS (
                    SELECT
                        display_label,
                        longitude,
                        latitude,
                        CASE
                            WHEN search_text = '{escapedQuery}' THEN 300 + {sourceBias}
                            WHEN search_text LIKE '{escapedQuery}%' THEN 220 + {sourceBias}
                            WHEN search_text LIKE '%{escapedQuery}%' THEN 170 + {sourceBias}
                            WHEN {tokenCondition} THEN 120 + {sourceBias}
                            ELSE 0
                        END AS score
                    FROM prepared
                    WHERE search_text LIKE '%{escapedQuery}%'
                       OR ({tokenCondition})
                )
                SELECT
                    display_label,
                    longitude,
                    latitude,
                    '{escapedSourceType}' AS source_type,
                    CASE
                        WHEN score >= 300 THEN 'exact'
                        WHEN score >= 200 THEN 'prefix'
                        WHEN score >= 150 THEN 'contains'
                        ELSE 'token'
                    END AS match_tier,
                    score
                FROM ranked
                WHERE score > 0
                  AND display_label IS NOT NULL
                  AND display_label <> ''
                ORDER BY score DESC, display_label
                LIMIT {safeLimit};";

            command.CommandText = searchSql;
            using var resultReader = await command.ExecuteReaderAsync(cancellationToken);
            while (await resultReader.ReadAsync(cancellationToken))
            {
                int score = resultReader.IsDBNull(5) ? 0 : Convert.ToInt32(resultReader.GetValue(5), CultureInfo.InvariantCulture);
                candidates.Add(new GeocodeCandidate
                {
                    DisplayLabel = resultReader.IsDBNull(0) ? string.Empty : resultReader.GetString(0),
                    Longitude = resultReader.IsDBNull(1) ? 0 : Convert.ToDouble(resultReader.GetValue(1), CultureInfo.InvariantCulture),
                    Latitude = resultReader.IsDBNull(2) ? 0 : Convert.ToDouble(resultReader.GetValue(2), CultureInfo.InvariantCulture),
                    SourceType = resultReader.IsDBNull(3) ? "Unknown" : resultReader.GetString(3),
                    MatchTier = resultReader.IsDBNull(4) ? "token" : resultReader.GetString(4),
                    Score = score,
                    ConfidenceTier = score >= 280 ? "High" : score >= 190 ? "Medium" : "Low"
                });
            }

            return candidates;
        }
    }
}
