using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public sealed class GersifyService : IDisposable
    {
        private readonly DuckDBManager _duckDb;
        private bool _isDisposed;

        public GersifyService()
        {
            _duckDb = new DuckDBManager();
        }

        public async Task<GersifyResult> GersifyPlacesAsync(
            GersifyOptions options,
            IProgress<string> progress = null,
            CancellationToken cancellationToken = default)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.InputCsvPath) || !File.Exists(options.InputCsvPath))
                throw new FileNotFoundException("Input CSV was not found.", options.InputCsvPath);
            if (string.IsNullOrWhiteSpace(options.OvertureReleaseFolder) || !Directory.Exists(options.OvertureReleaseFolder))
                throw new DirectoryNotFoundException($"Overture release folder was not found: {options.OvertureReleaseFolder}");
            if (string.IsNullOrWhiteSpace(options.OutputFolder))
                throw new ArgumentException("Output folder is required.", nameof(options));

            Directory.CreateDirectory(options.OutputFolder);

            string placesGlob = ResolvePlacesParquetGlob(options.OvertureReleaseFolder);
            if (string.IsNullOrWhiteSpace(placesGlob))
            {
                throw new FileNotFoundException(
                    "Could not find downloaded Overture Places GeoParquet files. Load Places first or choose a release folder containing a 'place' subfolder.");
            }

            string suffix = GersifyOptions.BuildTimestampSuffix();
            string outputCsvPath = Path.Combine(options.OutputFolder, $"gersified_points_{suffix}.csv");
            string candidatesCsvPath = Path.Combine(options.OutputFolder, $"gersify_candidates_{suffix}.csv");
            string bridgeCsvPath = Path.Combine(options.OutputFolder, $"gers_bridge_{suffix}.csv");

            await _duckDb.InitializeAsync(cancellationToken);
            await ExecuteNonQueryAsync(GersSql.BuildUtilityFunctionsCommand(), cancellationToken);

            progress?.Report("Reading input feature export...");
            await ExecuteNonQueryAsync(GersSql.BuildCreateUserInputTableCommand(options.InputCsvPath), cancellationToken);

            int inputCount = await ExecuteCountAsync(GersSql.UserInputTable, cancellationToken);
            if (inputCount == 0)
            {
                throw new InvalidOperationException("No valid point features were exported from the selected layer.");
            }

            progress?.Report($"Matching {inputCount:N0} point feature(s) to Overture Places...");
            var placeColumns = await ReadParquetColumnsAsync(placesGlob, cancellationToken);
            await ExecuteNonQueryAsync(GersSql.BuildPlacesCandidateTablesCommand(placesGlob, options, placeColumns), cancellationToken);

            progress?.Report("Writing GERSify outputs...");
            await ExecuteNonQueryAsync(
                GersSql.BuildCopyGersifyOutputsCommand(
                    outputCsvPath,
                    candidatesCsvPath,
                    bridgeCsvPath,
                    string.IsNullOrWhiteSpace(options.DatasetName) ? "user_data" : options.DatasetName),
                cancellationToken);

            int candidateCount = await ExecuteCountAsync(GersSql.CandidateTable, cancellationToken);
            int acceptedCount = await ExecuteCountAsync(GersSql.AcceptedTable, cancellationToken);
            var acceptedMatches = await ReadAcceptedMatchesAsync(cancellationToken);

            return new GersifyResult
            {
                InputCount = inputCount,
                CandidateCount = candidateCount,
                AcceptedCount = acceptedCount,
                OutputCsvPath = outputCsvPath,
                CandidateCsvPath = candidatesCsvPath,
                BridgeCsvPath = bridgeCsvPath,
                AcceptedMatches = acceptedMatches
            };
        }

        private async Task<IReadOnlyCollection<string>> ReadParquetColumnsAsync(string parquetGlob, CancellationToken cancellationToken)
        {
            string escapedPath = GeoParquetSql.EscapeSqlLiteral(parquetGlob.Replace('\\', '/'));
            await ExecuteNonQueryAsync($@"
                CREATE OR REPLACE TABLE gers_place_schema_probe AS
                SELECT * FROM read_parquet('{escapedPath}', hive_partitioning=1) LIMIT 0;", cancellationToken);

            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = "DESCRIBE gers_place_schema_probe";
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(0))
                {
                    columns.Add(reader.GetString(0));
                }
            }

            return columns;
        }

        private async Task<List<GersMatchCandidate>> ReadAcceptedMatchesAsync(CancellationToken cancellationToken)
        {
            var matches = new List<GersMatchCandidate>();
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = $@"
                SELECT
                    record_id,
                    user_name,
                    user_address,
                    longitude,
                    latitude,
                    gers_id,
                    overture_id,
                    overture_name,
                    distance_m,
                    name_similarity,
                    address_similarity,
                    gers_match_score,
                    candidate_rank,
                    accepted
                FROM {GersSql.AcceptedTable}
                ORDER BY record_id;";

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                matches.Add(new GersMatchCandidate
                {
                    RecordId = ReadString(reader, 0),
                    UserName = ReadString(reader, 1),
                    UserAddress = ReadString(reader, 2),
                    Longitude = ReadDouble(reader, 3),
                    Latitude = ReadDouble(reader, 4),
                    GersId = ReadString(reader, 5),
                    OvertureId = ReadString(reader, 6),
                    OvertureName = ReadString(reader, 7),
                    DistanceMeters = ReadDouble(reader, 8),
                    NameSimilarity = ReadDouble(reader, 9),
                    AddressSimilarity = reader.IsDBNull(10) ? null : ReadDouble(reader, 10),
                    MatchScore = ReadDouble(reader, 11),
                    CandidateRank = reader.IsDBNull(12) ? 0 : Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture),
                    Accepted = !reader.IsDBNull(13) && Convert.ToBoolean(reader.GetValue(13), CultureInfo.InvariantCulture)
                });
            }

            return matches;
        }

        private static string ResolvePlacesParquetGlob(string releaseFolder)
        {
            if (string.IsNullOrWhiteSpace(releaseFolder) || !Directory.Exists(releaseFolder))
                return null;

            string[] candidateDirectories =
            [
                Path.Combine(releaseFolder, "place"),
                Path.Combine(releaseFolder, "places", "place"),
                Path.Combine(releaseFolder, "theme=places", "type=place")
            ];

            foreach (string directory in candidateDirectories)
            {
                if (Directory.Exists(directory) &&
                    Directory.EnumerateFiles(directory, "*.parquet", SearchOption.TopDirectoryOnly).Any())
                {
                    return Path.Combine(directory, "*.parquet").Replace('\\', '/');
                }
            }

            var nestedPlaceDirectory = new DirectoryInfo(releaseFolder)
                .EnumerateDirectories("*", SearchOption.AllDirectories)
                .FirstOrDefault(directory =>
                    string.Equals(directory.Name, "place", StringComparison.OrdinalIgnoreCase) &&
                    directory.EnumerateFiles("*.parquet", SearchOption.TopDirectoryOnly).Any());

            return nestedPlaceDirectory == null
                ? null
                : Path.Combine(nestedPlaceDirectory.FullName, "*.parquet").Replace('\\', '/');
        }

        private async Task ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken)
        {
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task<int> ExecuteCountAsync(string tableName, CancellationToken cancellationToken)
        {
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = $"SELECT COUNT(*) FROM {tableName}";
            object value = await command.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        private static string ReadString(System.Data.Common.DbDataReader reader, int index) =>
            reader.IsDBNull(index) ? string.Empty : Convert.ToString(reader.GetValue(index), CultureInfo.InvariantCulture) ?? string.Empty;

        private static double ReadDouble(System.Data.Common.DbDataReader reader, int index) =>
            reader.IsDBNull(index) ? 0 : Convert.ToDouble(reader.GetValue(index), CultureInfo.InvariantCulture);

        public void Dispose()
        {
            if (_isDisposed) return;
            _duckDb.Dispose();
            _isDisposed = true;
        }
    }
}
