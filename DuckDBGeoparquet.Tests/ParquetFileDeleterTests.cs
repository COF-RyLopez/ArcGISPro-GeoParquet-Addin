using DuckDBGeoparquet.Services;
using System;
using System.IO;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class ParquetFileDeleterTests
    {
        [Theory]
        [InlineData(32, true)]  // ERROR_SHARING_VIOLATION
        [InlineData(33, true)]  // ERROR_LOCK_VIOLATION
        [InlineData(5, true)]   // ERROR_ACCESS_DENIED
        [InlineData(2, false)]  // ERROR_FILE_NOT_FOUND
        [InlineData(0, false)]  // success / no error
        public void IsLockWin32Error_TreatsInUseCodesAsLocked(int code, bool expected)
        {
            Assert.Equal(expected, ParquetFileDeleter.IsLockWin32Error(code));
        }

        [Fact]
        public void TryDelete_MissingFile_ReportsDeleted()
        {
            // No P/Invoke: File.Exists is false so it returns before touching kernel32.
            string path = Path.Combine(Path.GetTempPath(), $"pfd_missing_{Guid.NewGuid():N}.parquet");
            Assert.False(File.Exists(path));
            Assert.Equal(FileDeleteResult.Deleted, ParquetFileDeleter.TryDelete(path));
        }

        [Fact]
        public void TryDelete_ExistingUnlockedFile_DeletesIt()
        {
            if (!OperatingSystem.IsWindows()) return; // kernel32 path; CI runs on Windows

            string path = Path.Combine(Path.GetTempPath(), $"pfd_del_{Guid.NewGuid():N}.parquet");
            File.WriteAllText(path, "x");
            try
            {
                Assert.Equal(FileDeleteResult.Deleted, ParquetFileDeleter.TryDelete(path));
                Assert.False(File.Exists(path));
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }

        [Fact]
        public void TryDelete_LockedFile_ReportsLocked_ThenDeletesOnceReleased()
        {
            if (!OperatingSystem.IsWindows()) return; // kernel32 path; CI runs on Windows

            string path = Path.Combine(Path.GetTempPath(), $"pfd_lock_{Guid.NewGuid():N}.parquet");
            File.WriteAllText(path, "x");
            try
            {
                using (new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                    // Held open with no sharing → the delete open fails with a
                    // sharing violation, reported as Locked rather than throwing.
                    Assert.Equal(FileDeleteResult.Locked, ParquetFileDeleter.TryDelete(path));
                }

                // Handle released → deletes cleanly.
                Assert.Equal(FileDeleteResult.Deleted, ParquetFileDeleter.TryDelete(path));
                Assert.False(File.Exists(path));
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }
    }
}
