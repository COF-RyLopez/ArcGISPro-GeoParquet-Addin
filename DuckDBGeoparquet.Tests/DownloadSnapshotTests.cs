using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.IO;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public sealed class DownloadSnapshotTests : IDisposable
    {
        private readonly string _root = Path.Combine(Path.GetTempPath(), "snapshots-" + Guid.NewGuid().ToString("N"));
        private DownloadSnapshot Create() => new(_root, "2026-08-19.0", new ExtentBounds(-120, 35, -119, 36), "ZSTD");
        private static void Complete(DownloadSnapshot snapshot)
        {
            Directory.CreateDirectory(Path.Combine(snapshot.StagingPath, "segment"));
            File.WriteAllText(Path.Combine(snapshot.StagingPath, "segment", "roads.parquet"), "display fixture");
            File.WriteAllText(Path.Combine(snapshot.StagingPath, "source.geoparquet"), "source fixture");
            snapshot.Manifest.Datasets.Add(new DatasetLoadResult
            {
                Type = "segment", Status = DatasetLoadStatus.Completed, RowCount = 1,
                SourceFile = "source.geoparquet", DisplayFiles = new() { "segment/roads.parquet" }
            });
        }
        [Fact]
        public void FailedNewRelease_DoesNotHidePublishedPreviousRelease()
        {
            using var first = new DownloadSnapshot(Path.Combine(_root, "2026-07-22.0"), "2026-07-22.0", new ExtentBounds(0, 0, 1, 1), "ZSTD");
            Complete(first); string published = first.Commit();
            using (var failed = new DownloadSnapshot(Path.Combine(_root, "2026-08-19.0"), "2026-08-19.0", new ExtentBounds(0, 0, 1, 1), "ZSTD"))
                Assert.Equal(published, OvertureDataPaths.ResolveNewestReleaseFolder(_root));
            Assert.Equal(published, OvertureDataPaths.ResolveNewestReleaseFolder(_root));
        }

        [Fact]
        public void FailedOrCancelledReplacement_PreservesPublishedData()
        {
            using var first = Create(); Complete(first); string original = first.Commit();
            using (var failed = Create())
            {
                failed.Manifest.Datasets.Add(new DatasetLoadResult { Type = "segment", Status = DatasetLoadStatus.Failed });
                Assert.Throws<InvalidOperationException>(() => failed.Commit());
                Assert.Equal(original, OvertureDataPaths.ResolveNewestReleaseFolder(_root));
            }
            using (var cancelled = Create()) { Complete(cancelled); }
            Assert.Equal(original, OvertureDataPaths.ResolveNewestReleaseFolder(_root));
            Assert.True(File.Exists(Path.Combine(original, "segment", "roads.parquet")));
        }
        [Fact]
        public void SuccessfulReplacement_PublishesCompleteSnapshotAndRetainsPrevious()
        {
            using var first = Create(); Complete(first); string original = first.Commit();
            using var second = Create(); Complete(second); string current = second.Commit();
            Assert.Equal(current, DownloadSnapshot.ResolveCurrent(_root));
            Assert.True(Directory.Exists(original));
            Assert.Equal("2026-08-19.0", DownloadSnapshot.ReadRelease(current));
        }
        [Fact]
        public void LayerRefreshPlan_ReplacesOnlyMatchingDatasetFilesFromOlderSnapshots()
        {
            using var first = Create(); Complete(first); string old = first.Commit();
            using var second = Create(); Complete(second); string current = second.Commit();
            var files = SnapshotLayerPlan.GetSupersededFiles(current);
            Assert.Equal(Path.Combine(old, "segment", "roads.parquet"), Assert.Single(files));
            Assert.DoesNotContain(Path.Combine(current, "segment", "roads.parquet"), files);
            using var unrelated = Create();
            unrelated.Manifest.Datasets.Add(new DatasetLoadResult { Type = "address", Status = DatasetLoadStatus.Empty });
            Assert.Empty(SnapshotLayerPlan.GetSupersededFiles(unrelated.Commit()));
        }

        [Fact]
        public void LayerRefreshPlan_FindsPreviousReleaseAndLegacyFiles()
        {
            using var first = new DownloadSnapshot(Path.Combine(_root, "2026-07-22.0"), "2026-07-22.0", new ExtentBounds(0, 0, 1, 1), "ZSTD");
            Complete(first); string old = first.Commit();
            using var second = new DownloadSnapshot(Path.Combine(_root, "2026-08-19.0"), "2026-08-19.0", new ExtentBounds(0, 0, 1, 1), "ZSTD");
            Complete(second); string current = second.Commit();
            string legacy = Path.Combine(_root, "2026-06-17.0", "segment", "legacy.parquet");
            Directory.CreateDirectory(Path.GetDirectoryName(legacy)); File.WriteAllText(legacy, "fixture");
            var files = SnapshotLayerPlan.GetSupersededFiles(current);
            Assert.Equal(2, files.Count);
            Assert.Contains(Path.Combine(old, "segment", "roads.parquet"), files);
            Assert.Contains(legacy, files);
            Assert.DoesNotContain(Path.Combine(current, "segment", "roads.parquet"), files);
        }

        [Fact]
        public void MissingOutputCannotBePublished()
        {
            using var snapshot = Create(); Complete(snapshot);
            File.Delete(Path.Combine(snapshot.StagingPath, "segment", "roads.parquet"));
            Assert.Throws<InvalidDataException>(() => snapshot.Commit());
            Assert.Null(DownloadSnapshot.ResolveCurrent(_root));
            Assert.Null(OvertureDataPaths.ResolveNewestReleaseFolder(_root));
        }
        [Theory]
        [InlineData(179.9999, 0)]
        [InlineData(-179.9999, 0)]
        [InlineData(0, 89.9999)]
        public void RadiusBounds_ConservativelyIncludeDatelineAndPoles(double x, double y)
        {
            var bounds = GeographicBounds.Expand(new ExtentBounds(x, y, x, y), 75);
            Assert.Equal(-180, bounds.XMin); Assert.Equal(180, bounds.XMax);
            GeographicBounds.Validate(bounds);
        }
        public void Dispose() { if (Directory.Exists(_root)) Directory.Delete(_root, true); }
    }
}
