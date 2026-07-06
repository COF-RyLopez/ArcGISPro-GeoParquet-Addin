using DuckDBGeoparquet.Models;
using System;
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public sealed class BridgeFileService : IDisposable
    {
        private readonly DuckDBManager _duckDb = new();
        private bool _isDisposed;

        public async Task<TraceSourcesResult> TraceSourcesAsync(
            TraceSourcesOptions options,
            IProgress<string> progress = null,
            CancellationToken cancellationToken = default)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.InputCsvPath) || !File.Exists(options.InputCsvPath))
                throw new FileNotFoundException("Input GERS ID CSV was not found.", options.InputCsvPath);
            if (string.IsNullOrWhiteSpace(options.OutputFolder))
                throw new ArgumentException("Output folder is required.", nameof(options));

            Directory.CreateDirectory(options.OutputFolder);
            string suffix = GersifyOptions.BuildTimestampSuffix();
            string outputCsvPath = Path.Combine(options.OutputFolder, $"gers_source_lookup_{suffix}.csv");

            await _duckDb.InitializeAsync(cancellationToken);
            await ExecuteNonQueryAsync(GersSql.BuildUtilityFunctionsCommand(), cancellationToken);

            progress?.Report("Reading GERS IDs...");
            await ExecuteNonQueryAsync(GersSql.BuildCreateTraceInputCommand(options.InputCsvPath), cancellationToken);
            int inputCount = await ExecuteCountAsync(GersSql.TraceInputTable, cancellationToken);
            if (inputCount == 0)
            {
                throw new InvalidOperationException("No GERS IDs were found in the selected layer or table.");
            }

            progress?.Report("Querying Overture bridge files...");
            await ExecuteNonQueryAsync(GersSql.BuildTraceSourcesCommand(options, outputCsvPath), cancellationToken);
            int outputCount = await ExecuteCountAsync(GersSql.TraceOutputTable, cancellationToken);

            return new TraceSourcesResult
            {
                InputCount = inputCount,
                OutputCount = outputCount,
                OutputCsvPath = outputCsvPath
            };
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
