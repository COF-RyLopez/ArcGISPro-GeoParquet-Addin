using DuckDBGeoparquet.Models;
using System;
using System.IO;
using System.Text.Json;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Loads and saves <see cref="AddinSettings"/> from a machine-local JSON file.
    /// Proxy passwords are persisted for corporate authenticated proxies but are
    /// never written to logs.
    /// </summary>
    public static class AddinSettingsStore
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented = true,
            PropertyNameCaseInsensitive = true
        };

        public static string GetSettingsFilePath()
        {
            return Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "DuckDBGeoparquet",
                "settings.json");
        }

        public static AddinSettings Load()
        {
            try
            {
                string path = GetSettingsFilePath();
                if (!File.Exists(path))
                    return new AddinSettings();

                var loaded = JsonSerializer.Deserialize<AddinSettings>(File.ReadAllText(path), JsonOptions);
                return loaded ?? new AddinSettings();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to load add-in settings: {ex.Message}");
                return new AddinSettings();
            }
        }

        public static void Save(AddinSettings settings)
        {
            if (settings == null)
                settings = new AddinSettings();

            try
            {
                string path = GetSettingsFilePath();
                string directory = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(directory))
                    Directory.CreateDirectory(directory);

                File.WriteAllText(path, JsonSerializer.Serialize(settings, JsonOptions));
                System.Diagnostics.Debug.WriteLine(
                    $"Saved add-in settings (proxy={settings.HttpProxy}, memory={settings.EffectiveMemoryLimit}/{settings.EffectiveMaxMemory})");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to save add-in settings: {ex.Message}");
            }
        }
    }
}
