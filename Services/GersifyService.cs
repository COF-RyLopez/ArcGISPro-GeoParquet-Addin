using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
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

        public Task<GersifyResult> GersifyPlacesAsync(
            GersifyOptions options,
            IProgress<string> progress = null,
            CancellationToken cancellationToken = default)
        {
            if (options != null)
                options.TargetType = GersifyTargetType.Places;

            return GersifyAsync(options, progress, cancellationToken);
        }

        public async Task<GersifyResult> GersifyAsync(
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

            string targetGlob = ResolveTargetParquetGlob(options.OvertureReleaseFolder, options.TargetType);
            if (string.IsNullOrWhiteSpace(targetGlob))
            {
                throw new FileNotFoundException(
                    $"Could not find downloaded {options.TargetLabel} GeoParquet files. Load {options.TargetLabel} first or choose a release folder containing a '{options.TargetDatasetType}' subfolder.");
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
            var inputSignalCounts = await ReadInputSignalCountsAsync(cancellationToken);

            progress?.Report($"Checking selected {options.TargetLabel} files...");
            string readOptions = GetReadParquetOptions(options.TargetType);
            var targetColumns = await ReadParquetColumnsAsync(targetGlob, readOptions, cancellationToken);
            if (options.TargetType == GersifyTargetType.Places &&
                inputSignalCounts.NameCount == 0 &&
                inputSignalCounts.AddressCount > 0 &&
                !GersSql.HasPlaceAddressColumns(targetColumns))
            {
                throw new InvalidOperationException(
                    "The selected Overture Places files do not contain place address fields that GERSify can compare. " +
                    "Your input has address text but no names, so this run would only produce nearby_only matches. " +
                    "Close any map layers or tables using the old Places Parquet files, re-download Places with this add-in version, then choose that release folder again.");
            }

            if (options.TargetType == GersifyTargetType.Addresses && !GersSql.HasAddressDatasetColumns(targetColumns))
            {
                throw new InvalidOperationException(
                    "The selected Overture Addresses files do not contain the address number and street fields that GERSify needs for strict address validation. " +
                    "Load the Overture Addresses theme with this add-in version, then choose that release folder again.");
            }

            progress?.Report($"Matching {inputCount:N0} point feature(s) to {options.TargetLabel}...");
            string candidateSql = options.TargetType == GersifyTargetType.Addresses
                ? GersSql.BuildAddressesCandidateTablesCommand(targetGlob, options, targetColumns)
                : GersSql.BuildPlacesCandidateTablesCommand(targetGlob, options, targetColumns);
            await ExecuteNonQueryAsync(candidateSql, cancellationToken);

            progress?.Report("Writing GERSify outputs...");
            string datasetName = string.IsNullOrWhiteSpace(options.DatasetName) ? "user_data" : options.DatasetName;
            string overtureRelease = Path.GetFileName(
                options.OvertureReleaseFolder.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            await ExecuteNonQueryAsync(
                GersSql.BuildCopyGersifyOutputsCommand(
                    outputCsvPath,
                    candidatesCsvPath,
                    bridgeCsvPath,
                    datasetName,
                    options.TargetTheme,
                    options.TargetDatasetType,
                    overtureRelease,
                    options.InputLayerName),
                cancellationToken);

            int candidateCount = await ExecuteCountAsync(GersSql.CandidateTable, cancellationToken);
            var candidateSignalCounts = await ReadCandidateSignalCountsAsync(cancellationToken);
            int acceptedCount = await ExecuteCountAsync(GersSql.AcceptedTable, cancellationToken);
            var strategyCounts = await ReadAcceptedStrategyCountsAsync(cancellationToken);
            var acceptedMatches = await ReadAcceptedMatchesAsync(cancellationToken);

            return new GersifyResult
            {
                TargetLabel = options.TargetLabel,
                TargetTheme = options.TargetTheme,
                TargetDatasetType = options.TargetDatasetType,
                InputCount = inputCount,
                InputNameCount = inputSignalCounts.NameCount,
                InputAddressCount = inputSignalCounts.AddressCount,
                CandidateCount = candidateCount,
                CandidateOvertureAddressCount = candidateSignalCounts.OvertureAddressCount,
                CandidateAddressSimilarityCount = candidateSignalCounts.AddressSimilarityCount,
                AcceptedCount = acceptedCount,
                OutputCsvPath = outputCsvPath,
                CandidateCsvPath = candidatesCsvPath,
                BridgeCsvPath = bridgeCsvPath,
                AcceptedStrategyCounts = strategyCounts,
                AcceptedMatches = acceptedMatches
            };
        }

        private async Task<(int NameCount, int AddressCount)> ReadInputSignalCountsAsync(CancellationToken cancellationToken)
        {
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = $@"
                SELECT
                    sum(CASE WHEN trim(coalesce(name, '')) <> '' THEN 1 ELSE 0 END) AS name_count,
                    sum(CASE WHEN trim(coalesce(address, '')) <> '' THEN 1 ELSE 0 END) AS address_count
                FROM {GersSql.UserInputTable};";

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return (0, 0);

            return (
                ReadInt32(reader.GetValue(0)),
                ReadInt32(reader.GetValue(1)));
        }

        private async Task<(int OvertureAddressCount, int AddressSimilarityCount)> ReadCandidateSignalCountsAsync(CancellationToken cancellationToken)
        {
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = $@"
                SELECT
                    sum(CASE WHEN trim(coalesce(overture_address, '')) <> '' THEN 1 ELSE 0 END) AS overture_address_count,
                    sum(CASE WHEN address_similarity IS NOT NULL THEN 1 ELSE 0 END) AS address_similarity_count
                FROM {GersSql.CandidateTable};";

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return (0, 0);

            return (
                ReadInt32(reader.GetValue(0)),
                ReadInt32(reader.GetValue(1)));
        }

        private async Task<Dictionary<string, int>> ReadAcceptedStrategyCountsAsync(CancellationToken cancellationToken)
        {
            var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = $@"
                SELECT
                    coalesce(match_strategy, 'unknown') AS match_strategy,
                    count(*) AS match_count
                FROM {GersSql.AcceptedTable}
                GROUP BY match_strategy
                ORDER BY match_strategy;";

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                string strategy = ReadString(reader, 0);
                if (!string.IsNullOrWhiteSpace(strategy))
                {
                    counts[strategy] = ReadInt32(reader.GetValue(1));
                }
            }

            return counts;
        }

        private async Task<IReadOnlyCollection<string>> ReadParquetColumnsAsync(
            string parquetGlob,
            string readParquetOptions,
            CancellationToken cancellationToken)
        {
            string escapedPath = GeoParquetSql.EscapeSqlLiteral(parquetGlob.Replace('\\', '/'));
            await ExecuteNonQueryAsync($@"
                CREATE OR REPLACE TABLE gers_target_schema_probe AS
                SELECT * FROM read_parquet('{escapedPath}', {readParquetOptions}) LIMIT 0;", cancellationToken);

            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using var command = _duckDb.Connection.CreateCommand();
            command.CommandText = "DESCRIBE gers_target_schema_probe";
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
                    CandidateRank = reader.IsDBNull(12) ? 0 : ReadInt32(reader.GetValue(12)),
                    Accepted = !reader.IsDBNull(13) && Convert.ToBoolean(reader.GetValue(13), CultureInfo.InvariantCulture)
                });
            }

            return matches;
        }

        private static string GetReadParquetOptions(GersifyTargetType targetType) =>
            targetType == GersifyTargetType.Addresses
                ? GersSql.AddressesReadParquetOptions
                : GersSql.PlacesReadParquetOptions;

        private static string ResolveTargetParquetGlob(string releaseFolder, GersifyTargetType targetType) =>
            targetType == GersifyTargetType.Addresses
                ? ResolveAddressesParquetGlob(releaseFolder)
                : ResolvePlacesParquetGlob(releaseFolder);

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

        private static string ResolveAddressesParquetGlob(string releaseFolder)
        {
            if (string.IsNullOrWhiteSpace(releaseFolder) || !Directory.Exists(releaseFolder))
                return null;

            string[] candidateDirectories =
            [
                Path.Combine(releaseFolder, "address"),
                Path.Combine(releaseFolder, "addresses", "address"),
                Path.Combine(releaseFolder, "theme=addresses", "type=address")
            ];

            foreach (string directory in candidateDirectories)
            {
                if (Directory.Exists(directory) &&
                    Directory.EnumerateFiles(directory, "*.parquet", SearchOption.TopDirectoryOnly).Any())
                {
                    return Path.Combine(directory, "*.parquet").Replace('\\', '/');
                }
            }

            var nestedAddressDirectory = new DirectoryInfo(releaseFolder)
                .EnumerateDirectories("*", SearchOption.AllDirectories)
                .FirstOrDefault(directory =>
                    string.Equals(directory.Name, "address", StringComparison.OrdinalIgnoreCase) &&
                    directory.EnumerateFiles("*.parquet", SearchOption.TopDirectoryOnly).Any());

            return nestedAddressDirectory == null
                ? null
                : Path.Combine(nestedAddressDirectory.FullName, "*.parquet").Replace('\\', '/');
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
            return ReadInt32(value);
        }

        private static string ReadString(System.Data.Common.DbDataReader reader, int index) =>
            reader.IsDBNull(index) ? string.Empty : Convert.ToString(reader.GetValue(index), CultureInfo.InvariantCulture) ?? string.Empty;

        private static double ReadDouble(System.Data.Common.DbDataReader reader, int index) =>
            reader.IsDBNull(index) ? 0 : Convert.ToDouble(reader.GetValue(index), CultureInfo.InvariantCulture);

        private static int ReadInt32(object value)
        {
            if (value == null || value == DBNull.Value)
                return 0;

            return value switch
            {
                int intValue => intValue,
                long longValue => ClampToInt32(longValue),
                BigInteger bigIntegerValue => bigIntegerValue > int.MaxValue
                    ? int.MaxValue
                    : bigIntegerValue < int.MinValue ? int.MinValue : (int)bigIntegerValue,
                _ => Convert.ToInt32(value, CultureInfo.InvariantCulture)
            };
        }

        private static int ClampToInt32(long value) =>
            value > int.MaxValue ? int.MaxValue : value < int.MinValue ? int.MinValue : (int)value;

        public void Dispose()
        {
            if (_isDisposed) return;
            _duckDb.Dispose();
            _isDisposed = true;
        }
    }
}
