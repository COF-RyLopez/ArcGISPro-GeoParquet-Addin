using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using DuckDBGeoparquet.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Windows.Media.Imaging;

namespace DuckDBGeoparquet.Views
{
    /// <summary>
    /// Inline ribbon galleries listing each provider's datasets, generated
    /// from PresetCatalog so the ribbon and the wizard tree share one list.
    /// Clicking an item opens the GeoParquet Data Loader with that dataset
    /// checked and its provider group expanded.
    /// </summary>
    internal abstract class ProviderDatasetGallery : Gallery
    {
        private readonly string _themeKey;

        protected ProviderDatasetGallery(string themeKey, IReadOnlyList<UserDataSource> presets, string iconFileName)
        {
            _themeKey = themeKey;
            var icon = LoadIcon(iconFileName);
            foreach (var preset in presets)
                Add(new GalleryItem(preset.Name, icon, preset.Url));
        }

        protected override void OnClick(GalleryItem item) =>
            WizardDockpaneViewModel.ShowFocusedDataset(_themeKey, item.Text);

        /// <summary>Theme-matching provider glyph from the add-in's install folder.</summary>
        private static BitmapImage LoadIcon(string fileName)
        {
            try
            {
                string assemblyDir = Path.GetDirectoryName(typeof(ProviderDatasetGallery).Assembly.Location) ?? "";
                string folder = FrameworkApplication.ApplicationTheme == ApplicationTheme.Dark ? "DarkImages" : "Images";
                string path = Path.Combine(assemblyDir, folder, fileName);
                if (!File.Exists(path)) path = Path.Combine(assemblyDir, "Images", fileName);
                return File.Exists(path) ? new BitmapImage(new Uri(path)) : null;
            }
            catch { return null; }
        }
    }

    internal class OsmDatasetGallery : ProviderDatasetGallery
    {
        public OsmDatasetGallery()
            : base(Services.PresetCatalog.OsmThemeKey, Services.PresetCatalog.OpenStreetMap, "OsmProvider32.png") { }
    }

    internal class SourceCoopDatasetGallery : ProviderDatasetGallery
    {
        public SourceCoopDatasetGallery()
            : base(Services.PresetCatalog.SourceCoopThemeKey, Services.PresetCatalog.SourceCooperative, "SourceCoopProvider32.png") { }
    }
}
