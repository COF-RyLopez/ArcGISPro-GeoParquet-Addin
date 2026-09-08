using System;
using System.Threading;
using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    /// <summary>Owns one connection's jobs. Cancellation finishes before another job can enter.</summary>
    public sealed class DuckDBJobQueue : IDisposable
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        private bool _disposed;

        public async Task<T> RunAsync<T>(Func<Task<T>> work, Action interrupt, CancellationToken token = default)
        {
            await _gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                return await Task.Run(async () =>
                {
                    token.ThrowIfCancellationRequested();
                    using var registration = token.Register(interrupt);
                    try
                    {
                        token.ThrowIfCancellationRequested();
                        var result = await work().ConfigureAwait(false);
                        token.ThrowIfCancellationRequested();
                        return result;
                    }
                    catch (Exception) when (token.IsCancellationRequested)
                    {
                        throw new OperationCanceledException(token);
                    }
                }, token).ConfigureAwait(false);
            }
            finally { _gate.Release(); }
        }

        public void Dispose() => DisposeAfterJobs(() => { });

        public void DisposeAfterJobs(Action releaseConnection)
        {
            _gate.Wait();
            try
            {
                if (_disposed) return;
                _disposed = true;
                releaseConnection();
            }
            finally { _gate.Release(); }
        }
    }
}
