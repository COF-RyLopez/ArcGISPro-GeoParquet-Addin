using System;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DuckDBGeoparquet.Services
{
    /// <summary>Outcome of a best-effort parquet file delete.</summary>
    public enum FileDeleteResult
    {
        Deleted,
        Locked,
        Failed
    }

    /// <summary>
    /// Best-effort deletion of on-disk parquet files, distinguishing a file
    /// that is locked (held open by ArcGIS Pro and will free up later) from
    /// one that genuinely failed to delete. Uses the Win32 API directly so a
    /// lock is reported as a clean status instead of surfacing as an
    /// exception. No ArcGIS dependencies, so it is unit-testable.
    /// Extracted from WizardDockpaneViewModel (Phase 2c stage 3).
    /// </summary>
    public static class ParquetFileDeleter
    {
        private const uint FILE_DELETE_ACCESS = 0x00010000;
        private const int ERROR_SHARING_VIOLATION = 32;
        private const int ERROR_LOCK_VIOLATION = 33;
        private const int ERROR_ACCESS_DENIED = 5;

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern SafeFileHandle CreateFileW(
            string lpFileName,
            uint dwDesiredAccess,
            FileShare dwShareMode,
            IntPtr lpSecurityAttributes,
            FileMode dwCreationDisposition,
            FileAttributes dwFlagsAndAttributes,
            IntPtr hTemplateFile);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool DeleteFileW(string lpFileName);

        /// <summary>
        /// Attempts to delete <paramref name="filePath"/> without throwing.
        /// A missing file counts as already <see cref="FileDeleteResult.Deleted"/>.
        /// A sharing/lock/access-denied error is reported as
        /// <see cref="FileDeleteResult.Locked"/>; any other failure as
        /// <see cref="FileDeleteResult.Failed"/>.
        /// </summary>
        public static FileDeleteResult TryDelete(string filePath)
        {
            if (!File.Exists(filePath))
            {
                return FileDeleteResult.Deleted;
            }

            using (var handle = CreateFileW(
                filePath,
                FILE_DELETE_ACCESS,
                FileShare.None,
                IntPtr.Zero,
                FileMode.Open,
                FileAttributes.Normal,
                IntPtr.Zero))
            {
                if (handle.IsInvalid)
                {
                    int openError = Marshal.GetLastWin32Error();
                    return IsLockWin32Error(openError) ? FileDeleteResult.Locked : FileDeleteResult.Failed;
                }
            }

            if (DeleteFileW(filePath))
            {
                return FileDeleteResult.Deleted;
            }

            int deleteError = Marshal.GetLastWin32Error();
            return IsLockWin32Error(deleteError) ? FileDeleteResult.Locked : FileDeleteResult.Failed;
        }

        /// <summary>True for Win32 error codes that mean "the file is in use", not a hard failure.</summary>
        internal static bool IsLockWin32Error(int errorCode)
        {
            return errorCode == ERROR_SHARING_VIOLATION
                   || errorCode == ERROR_LOCK_VIOLATION
                   || errorCode == ERROR_ACCESS_DENIED;
        }
    }
}
