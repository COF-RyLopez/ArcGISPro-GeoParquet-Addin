using DuckDBGeoparquet.Models;
using System.Collections.Generic;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Built-in preset GeoParquet sources shown in the wizard's theme tree
    /// beside the Overture themes: curated public datasets that download
    /// through the same pipeline as user-defined custom sources. The catalog
    /// mirrors the QGIS GeoParquet Downloader's presets.json (OpenStreetMap
    /// LayerCake and Source Cooperative groups; Overture is already native).
    /// Geometry metadata is shipped here instead of probed at add time — every
    /// entry is standard GeoParquet whose geometry column reads as GEOMETRY
    /// when the spatial extension is loaded (verified against the live URLs
    /// with the real engine; see DuckDBGeoparquet.Tests/PresetCatalogTests).
    /// </summary>
    public static class PresetCatalog
    {
        /// <summary>
        /// Session secret for the Source Cooperative bucket: its dotted name
        /// (us-west-2.opendata.source.coop) breaks DuckDB's default virtual-host
        /// URL style against AWS's *.s3.amazonaws.com certificate, so reads of
        /// that bucket use path-style URLs on the regional endpoint. Scoped to
        /// the bucket, so Overture and other S3 reads are untouched; empty keys
        /// keep any user AWS credentials from being sent.
        /// </summary>
        public const string SourceCoopSecretSql =
            "CREATE OR REPLACE TEMPORARY SECRET sourcecoop_presets (" +
            " TYPE s3, PROVIDER config, KEY_ID '', SECRET '', REGION 'us-west-2'," +
            " ENDPOINT 's3.us-west-2.amazonaws.com', URL_STYLE 'path'," +
            " SCOPE 's3://us-west-2.opendata.source.coop/');";

        public const string OsmThemeKey = "osm";
        public const string SourceCoopThemeKey = "sourcecoop";
        public const string OsmDisplayName = "OpenStreetMap (LayerCake)";
        public const string SourceCoopDisplayName = "Source Cooperative";

        private static UserDataSource Preset(string name, string url) =>
            new() { Name = name, Url = url, GeometryColumn = "geometry", GeometryIsWkb = false, IsBuiltIn = true };

        /// <summary>Community OSM extracts from data.openstreetmap.us (LayerCake).</summary>
        public static IReadOnlyList<UserDataSource> OpenStreetMap { get; } = new[]
        {
            Preset("OSM Addresses", "https://data.openstreetmap.us/layercake/addresses.parquet"),
            Preset("OSM Boundaries", "https://data.openstreetmap.us/layercake/boundaries.parquet"),
            Preset("OSM Buildings", "https://data.openstreetmap.us/layercake/buildings.parquet"),
            Preset("OSM Highways", "https://data.openstreetmap.us/layercake/highways.parquet"),
            Preset("OSM Parks", "https://data.openstreetmap.us/layercake/parks.parquet"),
            Preset("OSM POIs", "https://data.openstreetmap.us/layercake/pois.parquet"),
            Preset("OSM Settlements", "https://data.openstreetmap.us/layercake/settlements.parquet"),
            Preset("OSM Waterways", "https://data.openstreetmap.us/layercake/waterways.parquet"),
            Preset("OSM Wetlands", "https://data.openstreetmap.us/layercake/wetlands.parquet"),
        };

        /// <summary>Curated datasets hosted on Source Cooperative.</summary>
        public static IReadOnlyList<UserDataSource> SourceCooperative { get; } = new[]
        {
            Preset("VIDA Google/Microsoft/OSM Buildings", "s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/*/*.parquet"),
            Preset("Microsoft ML Road Detections", "s3://us-west-2.opendata.source.coop/nlebovits/microsoft-ml-road-detections/road-detections/by_country/country=*/*.parquet"),
            Preset("GlobalBuildingAtlas", "s3://us-west-2.opendata.source.coop/tge-labs/globalbuildingatlas-lod1/*.parquet"),
            Preset("OpenBuildingMap", "s3://us-west-2.opendata.source.coop/tge-labs/openbuildingmap/*.parquet"),
            Preset("Foursquare Open Source Places", "s3://us-west-2.opendata.source.coop/fused/fsq-os-places/2025-02-06/places/*.parquet"),
            Preset("US Structures from ORNL", "s3://us-west-2.opendata.source.coop/wherobots/usa-structures/geoparquet/*.parquet"),
            Preset("Planet EU Field Boundaries (2022)", "https://data.source.coop/planet/eu-field-boundaries/field_boundaries.parquet"),
            Preset("USDA Crop Sequence Boundaries", "https://data.source.coop/fiboa/us-usda-cropland/us_usda_cropland.parquet"),
        };
    }
}
