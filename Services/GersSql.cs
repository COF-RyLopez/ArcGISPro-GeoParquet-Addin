using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace DuckDBGeoparquet.Services
{
    public static class GersSql
    {
        public const string UserInputTable = "gers_user_input";
        public const string CandidateTable = "gers_candidates";
        public const string AcceptedTable = "gers_accepted";
        public const string OutputTable = "gers_output";
        public const string TraceInputTable = "gers_trace_input";
        public const string TraceOutputTable = "gers_trace_sources";
        public const string PlacesReadParquetOptions = "hive_partitioning=1, union_by_name=1";
        public const string AddressesReadParquetOptions = "hive_partitioning=1, union_by_name=1";

        private static readonly string[] PlaceAddressColumns =
        [
            "address_freeform",
            "addresses",
            "address",
            "street",
            "full_address"
        ];

        private static readonly string[] AddressNumberColumns =
        [
            "number",
            "housenumber",
            "house_number"
        ];

        private static readonly string[] AddressStreetColumns =
        [
            "street",
            "street_name",
            "road",
            "name"
        ];

        public static string BuildCreateUserInputTableCommand(string csvPath)
        {
            string escapedPath = Escape(csvPath);
            return $@"
                CREATE OR REPLACE TABLE {UserInputTable} AS
                SELECT
                    CAST(record_id AS VARCHAR) AS record_id,
                    CAST(name AS VARCHAR) AS name,
                    CAST(address AS VARCHAR) AS address,
                    CAST(city AS VARCHAR) AS city,
                    CAST(state AS VARCHAR) AS state,
                    CAST(postcode AS VARCHAR) AS postcode,
                    CAST(longitude AS DOUBLE) AS longitude,
                    CAST(latitude AS DOUBLE) AS latitude
                FROM read_csv('{escapedPath}', header=true, auto_detect=true, ignore_errors=true)
                WHERE record_id IS NOT NULL
                  AND longitude IS NOT NULL
                  AND latitude IS NOT NULL;";
        }

        public static string BuildPlacesCandidateTablesCommand(
            string placesParquetGlob,
            GersifyOptions options,
            IReadOnlyCollection<string> availableColumns = null)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            string escapedPath = Escape(placesParquetGlob);
            int candidateLimit = Math.Clamp(options.ReviewCandidatesPerRecord, 1, 25);
            string maxDistance = Format(options.MaxDistanceMeters);
            string nameThreshold = Format(options.NameSimilarityThreshold);
            string addressThreshold = Format(options.AddressSimilarityThreshold);
            string scoreThreshold = Format(options.AcceptScoreThreshold);
            string proximityOnlyDistance = Format(Math.Min(
                options.MaxDistanceMeters,
                Math.Min(20, Math.Max(5, options.MaxDistanceMeters * 0.2))));
            string nearbyFallbackAcceptance = options.AllowNearbyOnlyMatches
                ? $@"
                        OR (
                            name_similarity IS NULL
                            AND address_similarity IS NULL
                            AND distance_m <= {proximityOnlyDistance}
                        )"
                : string.Empty;
            string bboxFilter = BuildOvertureBboxFilter(options.InputExtent, availableColumns);
            string placeNameExpr = BuildFirstExistingExpression(
                availableColumns,
                ("names", "CAST(names.primary AS VARCHAR)"),
                ("name", "CAST(name AS VARCHAR)"),
                ("name_primary", "CAST(name_primary AS VARCHAR)"),
                ("brand", "CAST(brand AS VARCHAR)"));
            string placeStreetAddressExpr = BuildFirstExistingExpression(
                availableColumns,
                ("address_freeform", "CAST(address_freeform AS VARCHAR)"),
                ("addresses", "(SELECT string_agg(CAST(a.freeform AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.freeform IS NOT NULL AND trim(CAST(a.freeform AS VARCHAR)) <> '')"),
                ("address", "CAST(address AS VARCHAR)"),
                ("street", "CAST(street AS VARCHAR)"),
                ("full_address", "CAST(full_address AS VARCHAR)"));
            string placeLocalityExpr = BuildFirstExistingExpression(
                availableColumns,
                ("address_locality", "CAST(address_locality AS VARCHAR)"),
                ("addresses", "(SELECT string_agg(DISTINCT CAST(a.locality AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.locality IS NOT NULL AND trim(CAST(a.locality AS VARCHAR)) <> '')"),
                ("locality", "CAST(locality AS VARCHAR)"),
                ("city", "CAST(city AS VARCHAR)"));
            string placeRegionExpr = BuildFirstExistingExpression(
                availableColumns,
                ("address_region", "CAST(address_region AS VARCHAR)"),
                ("addresses", "(SELECT string_agg(DISTINCT CAST(a.region AS VARCHAR), ' | ') FROM unnest(addresses) AS t(a) WHERE a.region IS NOT NULL AND trim(CAST(a.region AS VARCHAR)) <> '')"),
                ("region", "CAST(region AS VARCHAR)"),
                ("state", "CAST(state AS VARCHAR)"));
            string placePostcodeExpr = BuildFirstExistingExpression(
                availableColumns,
                ("address_postcode", "left(CAST(address_postcode AS VARCHAR), 5)"),
                ("addresses", "(SELECT string_agg(DISTINCT left(CAST(a.postcode AS VARCHAR), 5), ' | ') FROM unnest(addresses) AS t(a) WHERE a.postcode IS NOT NULL AND trim(CAST(a.postcode AS VARCHAR)) <> '')"),
                ("postcode", "left(CAST(postcode AS VARCHAR), 5)"),
                ("postal_code", "left(CAST(postal_code AS VARCHAR), 5)"),
                ("zip", "left(CAST(zip AS VARCHAR), 5)"));

            return $@"
                CREATE OR REPLACE TABLE {CandidateTable} AS
                WITH user_prepared AS (
                    SELECT
                        record_id,
                        coalesce(name, '') AS user_name,
                        coalesce(address, '') AS user_address,
                        coalesce(city, '') AS user_city,
                        coalesce(state, '') AS user_state,
                        coalesce(postcode, '') AS user_postcode,
                        longitude,
                        latitude,
                        NormalizeText(coalesce(name, '')) AS user_name_norm,
                        NormalizeText(coalesce(address, '')) AS user_street_norm,
                        NormalizeText(trim(concat_ws(' ', coalesce(address, ''), coalesce(city, ''), coalesce(state, ''), coalesce(postcode, '')))) AS user_address_norm
                    FROM {UserInputTable}
                ),
                overture_places AS (
                    SELECT
                        CAST(id AS VARCHAR) AS overture_id,
                        coalesce({placeNameExpr}, '') AS overture_name,
                        coalesce({placeStreetAddressExpr}, '') AS overture_address,
                        CAST(ST_X(geometry) AS DOUBLE) AS overture_longitude,
                        CAST(ST_Y(geometry) AS DOUBLE) AS overture_latitude,
                        NormalizeText(coalesce({placeNameExpr}, '')) AS overture_name_norm,
                        NormalizeText(coalesce({placeStreetAddressExpr}, '')) AS overture_street_norm,
                        NormalizeText(trim(concat_ws(' ', coalesce({placeStreetAddressExpr}, ''), coalesce({placeLocalityExpr}, ''), coalesce({placeRegionExpr}, ''), coalesce({placePostcodeExpr}, '')))) AS overture_address_norm
                    FROM read_parquet('{escapedPath}', {PlacesReadParquetOptions})
                    WHERE geometry IS NOT NULL
                    {bboxFilter}
                ),
                candidate_distances AS (
                    SELECT
                        u.*,
                        o.overture_id,
                        o.overture_name,
                        o.overture_address,
                        o.overture_longitude,
                        o.overture_latitude,
                        o.overture_name_norm,
                        o.overture_street_norm,
                        o.overture_address_norm,
                        sqrt(
                            pow((o.overture_longitude - u.longitude) * 111320.0 * cos(radians((o.overture_latitude + u.latitude) / 2.0)), 2) +
                            pow((o.overture_latitude - u.latitude) * 110540.0, 2)
                        ) AS distance_m
                    FROM user_prepared u
                    JOIN overture_places o
                      ON abs(o.overture_latitude - u.latitude) <= ({maxDistance} / 110540.0)
                     AND abs(o.overture_longitude - u.longitude) <= ({maxDistance} / greatest(111320.0 * abs(cos(radians(u.latitude))), 1.0))
                    WHERE (u.user_name_norm <> '' AND o.overture_name_norm <> '')
                       OR (u.user_street_norm <> '' AND o.overture_street_norm <> '')
                       OR (u.user_name_norm = '' AND u.user_street_norm <> '' AND o.overture_name_norm <> '')
                ),
                scored AS (
                    SELECT
                        record_id,
                        user_name,
                        user_address,
                        user_city,
                        user_state,
                        user_postcode,
                        longitude,
                        latitude,
                        overture_id AS gers_id,
                        overture_id,
                        overture_name,
                        overture_address,
                        overture_longitude,
                        overture_latitude,
                        distance_m,
                        CASE
                            WHEN user_name_norm = '' OR overture_name_norm = '' THEN NULL
                            ELSE jaro_winkler_similarity(user_name_norm, overture_name_norm)
                        END AS name_similarity,
                        CASE
                            WHEN user_street_norm = '' OR overture_street_norm = '' THEN NULL
                            ELSE greatest(
                                jaro_winkler_similarity(user_street_norm, overture_street_norm),
                                jaro_winkler_similarity(user_address_norm, overture_address_norm)
                            )
                        END AS address_similarity,
                        greatest(0.0, 1.0 - (distance_m / {maxDistance})) AS distance_similarity
                    FROM candidate_distances
                    WHERE distance_m <= {maxDistance}
                ),
                ranked AS (
                    SELECT
                        *,
                        CASE
                            WHEN name_similarity IS NOT NULL AND address_similarity IS NOT NULL THEN 'name_address'
                            WHEN name_similarity IS NOT NULL THEN 'name'
                            WHEN address_similarity IS NOT NULL THEN 'address'
                            ELSE 'nearby_only'
                        END AS match_strategy,
                        CASE
                            WHEN name_similarity IS NULL AND address_similarity IS NULL THEN distance_similarity * 100.0
                            WHEN name_similarity IS NULL THEN (address_similarity * 80.0) + (distance_similarity * 20.0)
                            WHEN address_similarity IS NULL THEN (name_similarity * 80.0) + (distance_similarity * 20.0)
                            ELSE (name_similarity * 60.0) + (address_similarity * 25.0) + (distance_similarity * 15.0)
                        END AS gers_match_score,
                        row_number() OVER (
                            PARTITION BY record_id
                            ORDER BY
                                CASE
                                    WHEN name_similarity IS NULL AND address_similarity IS NULL THEN distance_similarity * 100.0
                                    WHEN name_similarity IS NULL THEN (address_similarity * 80.0) + (distance_similarity * 20.0)
                                    WHEN address_similarity IS NULL THEN (name_similarity * 80.0) + (distance_similarity * 20.0)
                                    ELSE (name_similarity * 60.0) + (address_similarity * 25.0) + (distance_similarity * 15.0)
                                END DESC,
                                distance_m ASC,
                                overture_name ASC
                        ) AS candidate_rank
                    FROM scored
                )
                SELECT
                    *,
                    (
                    candidate_rank = 1
                    AND gers_match_score >= {scoreThreshold}
                    AND (
                        (
                            (name_similarity IS NULL OR name_similarity >= {nameThreshold})
                            AND (address_similarity IS NULL OR address_similarity >= {addressThreshold})
                            AND (name_similarity IS NOT NULL OR address_similarity IS NOT NULL)
                        )
                        {nearbyFallbackAcceptance}
                    )
                    ) AS accepted
                FROM ranked
                WHERE candidate_rank <= {candidateLimit};

                CREATE OR REPLACE TABLE {AcceptedTable} AS
                SELECT *
                FROM {CandidateTable}
                WHERE accepted = TRUE
                  AND candidate_rank = 1;

                CREATE OR REPLACE TABLE {OutputTable} AS
                SELECT
                    u.record_id,
                    u.name,
                    u.address,
                    u.city,
                    u.state,
                    u.postcode,
                    u.longitude,
                    u.latitude,
                    a.gers_id,
                    a.gers_match_score,
                    a.match_strategy AS gers_match_strategy,
                    a.distance_m AS gers_match_distance_m,
                    a.name_similarity AS gers_name_similarity,
                    a.address_similarity AS gers_address_similarity,
                    a.overture_address,
                    a.overture_name,
                    a.overture_id
                FROM {UserInputTable} u
                LEFT JOIN {AcceptedTable} a
                  ON u.record_id = a.record_id;";
        }

        public static string BuildAddressesCandidateTablesCommand(
            string addressParquetGlob,
            GersifyOptions options,
            IReadOnlyCollection<string> availableColumns = null)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            string escapedPath = Escape(addressParquetGlob);
            int candidateLimit = Math.Clamp(options.ReviewCandidatesPerRecord, 1, 25);
            string maxDistance = Format(options.MaxDistanceMeters);
            string addressThreshold = Format(options.AddressSimilarityThreshold);
            string scoreThreshold = Format(options.AcceptScoreThreshold);
            string bboxFilter = BuildOvertureBboxFilter(options.InputExtent, availableColumns);
            string numberExpr = BuildFirstExistingExpression(
                availableColumns,
                ("number", "CAST(number AS VARCHAR)"),
                ("housenumber", "CAST(housenumber AS VARCHAR)"),
                ("house_number", "CAST(house_number AS VARCHAR)"));
            string streetExpr = BuildFirstExistingExpression(
                availableColumns,
                ("street", "CAST(street AS VARCHAR)"),
                ("street_name", "CAST(street_name AS VARCHAR)"),
                ("road", "CAST(road AS VARCHAR)"),
                ("name", "CAST(name AS VARCHAR)"));
            string unitExpr = BuildFirstExistingExpression(
                availableColumns,
                ("unit", "CAST(unit AS VARCHAR)"),
                ("address_unit", "CAST(address_unit AS VARCHAR)"),
                ("suite", "CAST(suite AS VARCHAR)"));
            string localityExpr = BuildFirstExistingExpression(
                availableColumns,
                ("postal_city", "CAST(postal_city AS VARCHAR)"),
                ("city", "CAST(city AS VARCHAR)"),
                ("locality", "CAST(locality AS VARCHAR)"),
                ("district", "CAST(district AS VARCHAR)"));
            string regionExpr = BuildFirstExistingExpression(
                availableColumns,
                ("region", "CAST(region AS VARCHAR)"),
                ("state", "CAST(state AS VARCHAR)"),
                ("province", "CAST(province AS VARCHAR)"));
            string postcodeExpr = BuildFirstExistingExpression(
                availableColumns,
                ("postcode", "left(CAST(postcode AS VARCHAR), 5)"),
                ("postal_code", "left(CAST(postal_code AS VARCHAR), 5)"),
                ("zip", "left(CAST(zip AS VARCHAR), 5)"));
            string countryExpr = BuildFirstExistingExpression(
                availableColumns,
                ("country", "CAST(country AS VARCHAR)"),
                ("country_code", "CAST(country_code AS VARCHAR)"));

            return $@"
                CREATE OR REPLACE TABLE {CandidateTable} AS
                WITH user_prepared AS (
                    SELECT
                        record_id,
                        coalesce(name, '') AS user_name,
                        coalesce(address, '') AS user_address,
                        coalesce(city, '') AS user_city,
                        coalesce(state, '') AS user_state,
                        coalesce(postcode, '') AS user_postcode,
                        longitude,
                        latitude,
                        NormalizeText(coalesce(address, '')) AS user_street_norm,
                        NormalizeText(trim(concat_ws(' ', coalesce(address, ''), coalesce(city, ''), coalesce(state, ''), coalesce(postcode, '')))) AS user_address_norm,
                        regexp_extract(lower(coalesce(address, '')), '^\s*([0-9]+[a-z]?)', 1) AS user_number_norm,
                        left(regexp_replace(coalesce(postcode, ''), '[^0-9]', '', 'g'), 5) AS user_postcode_norm
                    FROM {UserInputTable}
                ),
                overture_addresses AS (
                    SELECT
                        CAST(id AS VARCHAR) AS overture_id,
                        trim(concat_ws(' ', nullif({numberExpr}, ''), nullif({streetExpr}, ''), nullif({unitExpr}, ''))) AS overture_address,
                        trim(concat_ws(' ', nullif({numberExpr}, ''), nullif({streetExpr}, ''))) AS overture_street_address,
                        {numberExpr} AS overture_number,
                        {postcodeExpr} AS overture_postcode,
                        CAST(ST_X(geometry) AS DOUBLE) AS overture_longitude,
                        CAST(ST_Y(geometry) AS DOUBLE) AS overture_latitude,
                        NormalizeText(trim(concat_ws(' ', nullif({numberExpr}, ''), nullif({streetExpr}, ''), nullif({unitExpr}, '')))) AS overture_street_norm,
                        NormalizeText(trim(concat_ws(' ', nullif({numberExpr}, ''), nullif({streetExpr}, ''), nullif({unitExpr}, ''), nullif({localityExpr}, ''), nullif({regionExpr}, ''), nullif({postcodeExpr}, ''), nullif({countryExpr}, '')))) AS overture_address_norm,
                        lower(trim({numberExpr})) AS overture_number_norm,
                        left(regexp_replace(coalesce({postcodeExpr}, ''), '[^0-9]', '', 'g'), 5) AS overture_postcode_norm
                    FROM read_parquet('{escapedPath}', {AddressesReadParquetOptions})
                    WHERE geometry IS NOT NULL
                    {bboxFilter}
                ),
                candidate_distances AS (
                    SELECT
                        u.*,
                        o.overture_id,
                        o.overture_address,
                        o.overture_street_address,
                        o.overture_number,
                        o.overture_postcode,
                        o.overture_longitude,
                        o.overture_latitude,
                        o.overture_street_norm,
                        o.overture_address_norm,
                        o.overture_number_norm,
                        o.overture_postcode_norm,
                        sqrt(
                            pow((o.overture_longitude - u.longitude) * 111320.0 * cos(radians((o.overture_latitude + u.latitude) / 2.0)), 2) +
                            pow((o.overture_latitude - u.latitude) * 110540.0, 2)
                        ) AS distance_m
                    FROM user_prepared u
                    JOIN overture_addresses o
                      ON abs(o.overture_latitude - u.latitude) <= ({maxDistance} / 110540.0)
                     AND abs(o.overture_longitude - u.longitude) <= ({maxDistance} / greatest(111320.0 * abs(cos(radians(u.latitude))), 1.0))
                    WHERE u.user_street_norm <> ''
                      AND o.overture_street_norm <> ''
                ),
                scored AS (
                    SELECT
                        record_id,
                        user_name,
                        user_address,
                        user_city,
                        user_state,
                        user_postcode,
                        longitude,
                        latitude,
                        overture_id AS gers_id,
                        overture_id,
                        overture_address AS overture_name,
                        overture_address,
                        overture_longitude,
                        overture_latitude,
                        distance_m,
                        CAST(NULL AS DOUBLE) AS name_similarity,
                        greatest(
                            jaro_winkler_similarity(user_street_norm, overture_street_norm),
                            jaro_winkler_similarity(user_address_norm, overture_address_norm)
                        ) AS address_similarity,
                        greatest(0.0, 1.0 - (distance_m / {maxDistance})) AS distance_similarity,
                        (
                            user_number_norm <> ''
                            AND overture_number_norm <> ''
                            AND user_number_norm = overture_number_norm
                        ) AS house_number_match,
                        (
                            user_postcode_norm = ''
                            OR overture_postcode_norm = ''
                            OR user_postcode_norm = overture_postcode_norm
                        ) AS postcode_compatible
                    FROM candidate_distances
                    WHERE distance_m <= {maxDistance}
                ),
                ranked AS (
                    SELECT
                        *,
                        CASE
                            WHEN house_number_match AND postcode_compatible AND address_similarity >= 0.995 THEN 'exact_address'
                            WHEN address_similarity IS NOT NULL THEN 'address'
                            ELSE 'nearby_only'
                        END AS match_strategy,
                        CASE
                            WHEN house_number_match AND postcode_compatible THEN (address_similarity * 90.0) + (distance_similarity * 10.0)
                            ELSE (address_similarity * 70.0) + (distance_similarity * 10.0)
                        END AS gers_match_score,
                        row_number() OVER (
                            PARTITION BY record_id
                            ORDER BY
                                CASE WHEN house_number_match AND postcode_compatible THEN 0 ELSE 1 END ASC,
                                CASE
                                    WHEN house_number_match AND postcode_compatible THEN (address_similarity * 90.0) + (distance_similarity * 10.0)
                                    ELSE (address_similarity * 70.0) + (distance_similarity * 10.0)
                                END DESC,
                                distance_m ASC,
                                overture_address ASC
                        ) AS candidate_rank
                    FROM scored
                )
                SELECT
                    *,
                    (
                        candidate_rank = 1
                        AND gers_match_score >= {scoreThreshold}
                        AND address_similarity >= {addressThreshold}
                        AND house_number_match
                        AND postcode_compatible
                    ) AS accepted
                FROM ranked
                WHERE candidate_rank <= {candidateLimit};

                CREATE OR REPLACE TABLE {AcceptedTable} AS
                SELECT *
                FROM {CandidateTable}
                WHERE accepted = TRUE
                  AND candidate_rank = 1;

                CREATE OR REPLACE TABLE {OutputTable} AS
                SELECT
                    u.record_id,
                    u.name,
                    u.address,
                    u.city,
                    u.state,
                    u.postcode,
                    u.longitude,
                    u.latitude,
                    a.gers_id,
                    a.gers_match_score,
                    a.match_strategy AS gers_match_strategy,
                    a.distance_m AS gers_match_distance_m,
                    a.name_similarity AS gers_name_similarity,
                    a.address_similarity AS gers_address_similarity,
                    a.overture_address,
                    a.overture_name,
                    a.overture_id
                FROM {UserInputTable} u
                LEFT JOIN {AcceptedTable} a
                  ON u.record_id = a.record_id;";
        }

        public static bool HasPlaceAddressColumns(IReadOnlyCollection<string> availableColumns)
        {
            if (availableColumns == null || availableColumns.Count == 0)
                return true;

            return PlaceAddressColumns.Any(column => availableColumns.Contains(column, StringComparer.OrdinalIgnoreCase));
        }

        public static bool HasAddressDatasetColumns(IReadOnlyCollection<string> availableColumns)
        {
            if (availableColumns == null || availableColumns.Count == 0)
                return true;

            return AddressNumberColumns.Any(column => availableColumns.Contains(column, StringComparer.OrdinalIgnoreCase)) &&
                   AddressStreetColumns.Any(column => availableColumns.Contains(column, StringComparer.OrdinalIgnoreCase));
        }

        public static string BuildCopyGersifyOutputsCommand(
            string outputCsvPath,
            string candidateCsvPath,
            string bridgeCsvPath,
            string datasetName,
            string theme = "places",
            string type = "place")
        {
            string escapedOutputPath = Escape(outputCsvPath);
            string escapedCandidatePath = Escape(candidateCsvPath);
            string escapedBridgePath = Escape(bridgeCsvPath);
            string escapedDataset = Escape(datasetName);
            string escapedTheme = Escape(theme);
            string escapedType = Escape(type);

            return $@"
                COPY {OutputTable} TO '{escapedOutputPath}' (FORMAT CSV, HEADER TRUE);
                COPY {CandidateTable} TO '{escapedCandidatePath}' (FORMAT CSV, HEADER TRUE);
                COPY (
                    SELECT
                        gers_id AS id,
                        record_id,
                        CAST(current_timestamp AS VARCHAR) AS update_time,
                        '{escapedDataset}' AS dataset,
                        '{escapedTheme}' AS theme,
                        '{escapedType}' AS type
                    FROM {AcceptedTable}
                    ORDER BY record_id
                ) TO '{escapedBridgePath}' (FORMAT CSV, HEADER TRUE);";
        }

        public static string BuildCreateTraceInputCommand(string csvPath)
        {
            string escapedPath = Escape(csvPath);
            return $@"
                CREATE OR REPLACE TABLE {TraceInputTable} AS
                SELECT DISTINCT CAST(gers_id AS VARCHAR) AS gers_id
                FROM read_csv('{escapedPath}', header=true, auto_detect=true, ignore_errors=true)
                WHERE gers_id IS NOT NULL
                  AND trim(CAST(gers_id AS VARCHAR)) <> '';";
        }

        public static string BuildTraceSourcesCommand(TraceSourcesOptions options, string outputCsvPath)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            string bridgeGlob = Escape(BuildBridgeGlob(options.BridgeRoot, options.Release));
            string outputPath = Escape(outputCsvPath);
            string theme = Escape(options.Theme);
            string type = Escape(options.Type);
            int maxRows = Math.Clamp(options.MaxRows, 1, 1000000);

            return $@"
                CREATE OR REPLACE TABLE {TraceOutputTable} AS
                WITH bridge AS (
                    SELECT
                        CAST(id AS VARCHAR) AS gers_id,
                        CAST(record_id AS VARCHAR) AS record_id,
                        CAST(update_time AS VARCHAR) AS update_time,
                        CAST(dataset AS VARCHAR) AS dataset,
                        CAST(theme AS VARCHAR) AS theme,
                        CAST(type AS VARCHAR) AS type
                    FROM read_parquet('{bridgeGlob}', hive_partitioning=1)
                    WHERE theme = '{theme}'
                      AND type = '{type}'
                )
                SELECT
                    i.gers_id,
                    b.dataset,
                    b.record_id,
                    b.update_time,
                    b.theme,
                    b.type,
                    BuildOsmUrl(b.dataset, b.record_id) AS source_url
                FROM {TraceInputTable} i
                JOIN bridge b
                  ON i.gers_id = b.gers_id
                ORDER BY i.gers_id, b.dataset, b.record_id
                LIMIT {maxRows};

                COPY {TraceOutputTable} TO '{outputPath}' (FORMAT CSV, HEADER TRUE);";
        }

        public static string BuildBridgeGlob(string bridgeRoot, string release)
        {
            string root = string.IsNullOrWhiteSpace(bridgeRoot)
                ? "https://overturemapswestus2.blob.core.windows.net/bridgefiles"
                : bridgeRoot.Trim().TrimEnd('/', '\\');
            string safeRelease = string.IsNullOrWhiteSpace(release)
                ? GersifyOptions.DefaultReleaseVersion
                : release.Trim();

            return $"{root}/{safeRelease}/**/*.parquet";
        }

        private static string BuildOvertureBboxFilter(ExtentBounds extent, IReadOnlyCollection<string> availableColumns)
        {
            if (extent == null)
                return string.Empty;

            if (availableColumns != null && !availableColumns.Contains("bbox", StringComparer.OrdinalIgnoreCase))
                return string.Empty;

            return $@"
                      AND bbox.xmin <= {Format(extent.XMax)}
                      AND bbox.xmax >= {Format(extent.XMin)}
                      AND bbox.ymin <= {Format(extent.YMax)}
                      AND bbox.ymax >= {Format(extent.YMin)}";
        }

        private static string BuildFirstExistingExpression(
            IReadOnlyCollection<string> availableColumns,
            params (string Column, string Expression)[] expressions)
        {
            if (availableColumns == null || availableColumns.Count == 0)
            {
                return expressions.Length == 0 ? "''" : expressions[0].Expression;
            }

            foreach (var (column, expression) in expressions)
            {
                if (availableColumns.Contains(column, StringComparer.OrdinalIgnoreCase))
                {
                    return expression;
                }
            }

            return "''";
        }

        public static string BuildUtilityFunctionsCommand() => @"
                CREATE OR REPLACE MACRO NormalizeText(value) AS
                    regexp_replace(lower(trim(CAST(coalesce(value, '') AS VARCHAR))), '[^a-z0-9]+', ' ', 'g');

                CREATE OR REPLACE MACRO BuildOsmUrl(dataset, record_id) AS
                    CASE
                        WHEN dataset IS NULL OR record_id IS NULL OR lower(CAST(dataset AS VARCHAR)) NOT LIKE '%openstreetmap%' THEN NULL
                        WHEN regexp_matches(CAST(record_id AS VARCHAR), '^n[0-9]+(@.*)?$')
                            THEN 'https://www.openstreetmap.org/node/' || regexp_extract(CAST(record_id AS VARCHAR), '^n([0-9]+)', 1)
                        WHEN regexp_matches(CAST(record_id AS VARCHAR), '^w[0-9]+(@.*)?$')
                            THEN 'https://www.openstreetmap.org/way/' || regexp_extract(CAST(record_id AS VARCHAR), '^w([0-9]+)', 1)
                        WHEN regexp_matches(CAST(record_id AS VARCHAR), '^r[0-9]+(@.*)?$')
                            THEN 'https://www.openstreetmap.org/relation/' || regexp_extract(CAST(record_id AS VARCHAR), '^r([0-9]+)', 1)
                        ELSE NULL
                    END;";

        private static string Escape(string value) =>
            GeoParquetSql.EscapeSqlLiteral(value?.Replace('\\', '/') ?? string.Empty);

        private static string Format(double value) =>
            value.ToString("G", CultureInfo.InvariantCulture);
    }
}
