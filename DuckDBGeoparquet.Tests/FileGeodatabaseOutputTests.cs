using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class FileGeodatabaseOutputSettingsTests
    {
        [Fact]
        public void Default_KeepsGeoParquetAsTheOutput()
        {
            var settings = new FileGeodatabaseOutputSettings();

            Assert.False(settings.Enabled);
            Assert.True(settings.OverwriteExisting);
            Assert.True(string.IsNullOrEmpty(settings.GeodatabasePath));
            Assert.False(FileGeodatabaseOutput.IsEnabled(settings));
            Assert.False(FileGeodatabaseOutput.IsEnabled(null));
        }

        [Fact]
        public void GeoParquetDefault_IsDisabled()
        {
            Assert.False(FileGeodatabaseOutputSettings.GeoParquetDefault.Enabled);
        }

        [Fact]
        public void Enabled_TurnsOnFileGeodatabaseExport()
        {
            var settings = new FileGeodatabaseOutputSettings { Enabled = true };

            Assert.True(FileGeodatabaseOutput.IsEnabled(settings));
        }
    }

    public class FileGeodatabaseOutputTests
    {
        [Theory]
        [InlineData(@"C:\data\Overture.gdb", true)]
        [InlineData(@"C:\data\Overture.GDB\", true)]
        [InlineData(@"C:\data\output", false)]
        [InlineData("", false)]
        [InlineData(null, false)]
        public void IsGeodatabasePath_DetectsGdbFolders(string path, bool expected)
        {
            Assert.Equal(expected, FileGeodatabaseOutput.IsGeodatabasePath(path));
        }

        [Fact]
        public void ResolveGeodatabasePath_CreatesDefaultUnderOutputFolderWhenUnset()
        {
            string output = Path.Combine(Path.GetTempPath(), "overture_out");
            string resolved = FileGeodatabaseOutput.ResolveGeodatabasePath(output, null);

            Assert.Equal(Path.GetFullPath(Path.Combine(output, FileGeodatabaseOutput.DefaultGeodatabaseName)), resolved);
        }

        [Fact]
        public void ResolveGeodatabasePath_UsesUserChosenGdb()
        {
            string chosen = Path.Combine(Path.GetTempPath(), "custom.gdb");
            string resolved = FileGeodatabaseOutput.ResolveGeodatabasePath(@"C:\unused", chosen);

            Assert.Equal(Path.GetFullPath(chosen), resolved);
        }

        [Fact]
        public void ResolveGeodatabasePath_PlacesDefaultGdbInUserFolder()
        {
            string folder = Path.Combine(Path.GetTempPath(), "gdb_parent");
            string resolved = FileGeodatabaseOutput.ResolveGeodatabasePath(@"C:\unused", folder);

            Assert.Equal(Path.GetFullPath(Path.Combine(folder, FileGeodatabaseOutput.DefaultGeodatabaseName)), resolved);
        }

        [Fact]
        public void ResolveGeodatabasePath_RequiresOutputFolderWhenPathMissing()
        {
            Assert.Throws<ArgumentException>(() => FileGeodatabaseOutput.ResolveGeodatabasePath(null, "  "));
        }

        [Fact]
        public void SplitGeodatabasePath_ReturnsParentAndName()
        {
            string gdb = Path.Combine(Path.GetTempPath(), "Overture.gdb");
            FileGeodatabaseOutput.SplitGeodatabasePath(gdb, out string parent, out string name);

            Assert.Equal(Path.GetFullPath(Path.GetTempPath()).TrimEnd(Path.DirectorySeparatorChar), parent.TrimEnd(Path.DirectorySeparatorChar));
            Assert.Equal("Overture.gdb", name);
        }

        [Fact]
        public void BuildFeatureClassName_UsesThemeTypeAndGeometry()
        {
            string name = FileGeodatabaseOutput.BuildFeatureClassName("buildings", "building", "POLYGON");

            Assert.Equal("buildings_building_polygons", name);
        }

        [Fact]
        public void BuildFeatureClassName_SanitizesInvalidCharacters()
        {
            string name = FileGeodatabaseOutput.BuildFeatureClassName("base", "land-use", "MULTIPOLYGON");

            Assert.Equal("base_land_use_multipolygons", name);
        }

        [Fact]
        public void AllocateFeatureClassName_ReusesStableNameThenSuffixes()
        {
            var reserved = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string first = FileGeodatabaseOutput.AllocateFeatureClassName("buildings_building_polygons", reserved);
            string second = FileGeodatabaseOutput.AllocateFeatureClassName(
                "buildings_building_polygons", reserved, forceSuffix: true, conflictSuffix: "20260101120000");

            Assert.Equal("buildings_building_polygons", first);
            Assert.Equal("buildings_building_polygons_20260101120000", second);
            Assert.Contains(first, reserved);
            Assert.Contains(second, reserved);
        }

        [Fact]
        public void AllocateFeatureClassName_NumericSuffixWhenTimestampCollides()
        {
            var reserved = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "buildings_building_polygons",
                "buildings_building_polygons_retry"
            };

            string name = FileGeodatabaseOutput.AllocateFeatureClassName(
                "buildings_building_polygons", reserved, forceSuffix: true, conflictSuffix: "retry");

            Assert.Equal("buildings_building_polygons_2", name);
        }

        [Fact]
        public void LayerSourceExists_AcceptsParquetFileAndGdbFeatureClass()
        {
            string parquet = Path.Combine(Path.GetTempPath(), $"fgdb_{Guid.NewGuid():N}.parquet");
            string gdb = Path.Combine(Path.GetTempPath(), $"fgdb_{Guid.NewGuid():N}.gdb");
            File.WriteAllText(parquet, "parquet");
            Directory.CreateDirectory(gdb);
            try
            {
                Assert.True(FileGeodatabaseOutput.LayerSourceExists(parquet));
                Assert.True(FileGeodatabaseOutput.LayerSourceExists(Path.Combine(gdb, "buildings_building_polygons")));
                Assert.False(FileGeodatabaseOutput.LayerSourceExists(Path.Combine(Path.GetTempPath(), "missing.parquet")));
            }
            finally
            {
                File.Delete(parquet);
                Directory.Delete(gdb, recursive: true);
            }
        }

        [Fact]
        public void ShouldSkipParquetDomainLimitation_OnlyForParquetNotFileGeodatabase()
        {
            Assert.True(FileGeodatabaseOutput.ShouldSkipParquetDomainLimitation(
                isFileGeodatabase: false,
                filePath: @"C:\data\building.parquet",
                exceptionMessage: "Coded value domain access is not supported"));

            Assert.False(FileGeodatabaseOutput.ShouldSkipParquetDomainLimitation(
                isFileGeodatabase: true,
                filePath: @"C:\data\Overture.gdb\buildings_building_polygons",
                exceptionMessage: "Coded value domain access is not supported"));

            Assert.False(FileGeodatabaseOutput.ShouldSkipParquetDomainLimitation(
                isFileGeodatabase: false,
                filePath: @"C:\data\Overture.gdb\buildings_building_polygons",
                exceptionMessage: "Coded value domain access is not supported"));
        }

        [Fact]
        public void FormatToolFailure_KeepsParquetAndIncludesGpMessages()
        {
            string text = FileGeodatabaseOutput.FormatToolFailure(
                FileGeodatabaseOutput.ExportFeaturesTool,
                "Export Features for buildings",
                GeoprocessingToolResult.Failure("ERROR 000732: Input Features: Dataset does not exist"));

            Assert.Contains("conversion.ExportFeatures", text);
            Assert.Contains("ERROR 000732", text);
            Assert.Contains("GeoParquet files were kept", text);
        }
    }
}
