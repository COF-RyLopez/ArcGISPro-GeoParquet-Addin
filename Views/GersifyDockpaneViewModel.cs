using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Core.Geoprocessing;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using DuckDBGeoparquet.Models;
using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows.Forms;

namespace DuckDBGeoparquet.Views
{
    internal class GersifyDockpaneViewModel : DockPane
    {
        private const string DockPaneId = "DuckDBGeoparquet_Views_GersifyDockpane_V2";
        private readonly StringBuilder _logBuilder = new();
        private bool _isBusy;
        private LayerSelectionItem _selectedInputLayer;
        private LayerSelectionItem _selectedTraceLayer;
        private GersifyTargetOption _selectedGersifyTarget;
        private string _selectedIdField;
        private string _selectedNameField;
        private string _selectedAddressField;
        private string _selectedStreetNumberField;
        private string _selectedStreetFractionField;
        private string _selectedStreetPrefixField;
        private string _selectedStreetNameField;
        private string _selectedStreetTypeField;
        private string _selectedStreetSuffixField;
        private string _selectedUnitField;
        private string _selectedCityField;
        private string _selectedStateField;
        private string _selectedPostcodeField;
        private string _selectedTraceGersIdField;
        private string _releaseFolderPath;
        private string _outputFolder;
        private string _datasetName = "user_data";
        private string _maxDistanceMeters = "75";
        private string _nameSimilarityThreshold = "0.86";
        private string _addressSimilarityThreshold = "0.72";
        private string _acceptScoreThreshold = "72";
        private bool _allowNearbyOnlyMatches;
        private bool _generateStableLinkIds;
        private bool _relateLinkageToSourceLayer = true;
        private bool _hasLastGersifyRun;
        private string _lastCandidateCsvPath;
        private string _lastBridgeCsvPath;
        private string _lastOutputFeatureClassPath;
        private string _lastRunSummaryText = string.Empty;
        private string _bridgeRoot = "https://overturemapswestus2.blob.core.windows.net/bridgefiles";
        private string _bridgeRelease = GersifyOptions.DefaultReleaseVersion;
        private string _bridgeTheme = "places";
        private string _bridgeType = "place";
        private string _statusText = "Link, don't replace: choose your authoritative layer, then map fields to add GERS IDs without changing source geometry.";
        private string _logText = string.Empty;
        private string _linkagePreviewText =
            "Output layer adds: record_id, source_record_key, gers_id, gers_match_score, gers_match_strategy, and match diagnostics." + Environment.NewLine +
            "Bridge CSV adds: record_id, source_record_key, linkage_policy, match_strategy, match_score, overture_release, source_layer, linkage_tool." + Environment.NewLine +
            "Use a business key (GlobalID, site_id) when possible. Enable Generate stable link IDs when only OBJECTID/FID exists.";
        private string _fieldMappingPreviewText = "Choose an input layer to preview the resolved field mapping.";
        private string _fieldMappingWarningText = string.Empty;

        protected GersifyDockpaneViewModel()
        {
            InputLayers = [];
            TraceLayers = [];
            GersifyTargets =
            [
                new GersifyTargetOption("Addresses", GersifyTargetType.Addresses, "Validate address points against Overture Addresses."),
                new GersifyTargetOption("Places", GersifyTargetType.Places, "Enrich local places or facilities with Overture Places GERS IDs.")
            ];
            InputFields = [];
            OptionalInputFields = [];
            TraceFields = [];
            SelectedGersifyTarget = GersifyTargets.FirstOrDefault();

            string addinDataBase = ProjectDataLocator.GetAddinDataBase();
            ReleaseFolderPath = ProjectDataLocator.GetNewestLoadedReleaseFolder();
            OutputFolder = Path.Combine(addinDataBase, "GERSify");

            RefreshLayersCommand = new RelayCommand(async () => await RefreshLayersAsync(), () => !IsRunning);
            BrowseReleaseFolderCommand = new RelayCommand(() => BrowseReleaseFolder(), () => !IsRunning);
            BrowseOutputFolderCommand = new RelayCommand(() => BrowseOutputFolder(), () => !IsRunning);
            RunGersifyCommand = new RelayCommand(async () => await RunGersifyAsync(), CanRunGersify);
            RunTraceSourcesCommand = new RelayCommand(async () => await RunTraceSourcesAsync(), CanRunTraceSources);
            OpenReviewFolderCommand = new RelayCommand(OpenReviewFolder, () => CanOpenLastRunArtifact(_lastOutputFeatureClassPath, _lastCandidateCsvPath, _lastBridgeCsvPath));
            OpenCandidatesCsvCommand = new RelayCommand(() => OpenFileInShell(_lastCandidateCsvPath), () => CanOpenLastRunFile(_lastCandidateCsvPath));
            OpenBridgeCsvCommand = new RelayCommand(() => OpenFileInShell(_lastBridgeCsvPath), () => CanOpenLastRunFile(_lastBridgeCsvPath));
        }

        protected override async Task InitializeAsync()
        {
            await RefreshLayersAsync();
        }

        public static void Show()
        {
            if (FrameworkApplication.DockPaneManager.Find(DockPaneId) is GersifyDockpaneViewModel pane)
            {
                pane.Activate();
            }
        }

        public ObservableCollection<LayerSelectionItem> InputLayers { get; }
        public ObservableCollection<LayerSelectionItem> TraceLayers { get; }
        public ObservableCollection<GersifyTargetOption> GersifyTargets { get; }
        public ObservableCollection<string> InputFields { get; }
        public ObservableCollection<string> OptionalInputFields { get; }
        public ObservableCollection<string> TraceFields { get; }

        public GersifyTargetOption SelectedGersifyTarget
        {
            get => _selectedGersifyTarget;
            set
            {
                if (SetProperty(ref _selectedGersifyTarget, value))
                {
                    if (value != null)
                    {
                        BridgeTheme = value.TargetTheme;
                        BridgeType = value.TargetDatasetType;
                    }

                    RaiseCommandStatesChanged();
                }
            }
        }

        public LayerSelectionItem SelectedInputLayer
        {
            get => _selectedInputLayer;
            set
            {
                if (SetProperty(ref _selectedInputLayer, value))
                {
                    _ = RefreshInputFieldsAsync(value);
                    RaiseCommandStatesChanged();
                }
            }
        }

        public LayerSelectionItem SelectedTraceLayer
        {
            get => _selectedTraceLayer;
            set
            {
                if (SetProperty(ref _selectedTraceLayer, value))
                {
                    _ = RefreshTraceFieldsAsync(value);
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedIdField
        {
            get => _selectedIdField;
            set
            {
                if (SetProperty(ref _selectedIdField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedNameField
        {
            get => _selectedNameField;
            set
            {
                if (SetProperty(ref _selectedNameField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedAddressField
        {
            get => _selectedAddressField;
            set
            {
                if (SetProperty(ref _selectedAddressField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedStreetNumberField
        {
            get => _selectedStreetNumberField;
            set
            {
                if (SetProperty(ref _selectedStreetNumberField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedStreetFractionField
        {
            get => _selectedStreetFractionField;
            set
            {
                if (SetProperty(ref _selectedStreetFractionField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedStreetPrefixField
        {
            get => _selectedStreetPrefixField;
            set
            {
                if (SetProperty(ref _selectedStreetPrefixField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedStreetNameField
        {
            get => _selectedStreetNameField;
            set
            {
                if (SetProperty(ref _selectedStreetNameField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedStreetTypeField
        {
            get => _selectedStreetTypeField;
            set
            {
                if (SetProperty(ref _selectedStreetTypeField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedStreetSuffixField
        {
            get => _selectedStreetSuffixField;
            set
            {
                if (SetProperty(ref _selectedStreetSuffixField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedUnitField
        {
            get => _selectedUnitField;
            set
            {
                if (SetProperty(ref _selectedUnitField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedCityField
        {
            get => _selectedCityField;
            set
            {
                if (SetProperty(ref _selectedCityField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedStateField
        {
            get => _selectedStateField;
            set
            {
                if (SetProperty(ref _selectedStateField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedPostcodeField
        {
            get => _selectedPostcodeField;
            set
            {
                if (SetProperty(ref _selectedPostcodeField, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public string SelectedTraceGersIdField
        {
            get => _selectedTraceGersIdField;
            set
            {
                if (SetProperty(ref _selectedTraceGersIdField, value))
                    RaiseCommandStatesChanged();
            }
        }

        public string ReleaseFolderPath
        {
            get => _releaseFolderPath;
            set
            {
                if (SetProperty(ref _releaseFolderPath, value))
                    RaiseCommandStatesChanged();
            }
        }

        public string OutputFolder
        {
            get => _outputFolder;
            set
            {
                if (SetProperty(ref _outputFolder, value))
                    RaiseCommandStatesChanged();
            }
        }

        public string DatasetName
        {
            get => _datasetName;
            set => SetProperty(ref _datasetName, value);
        }

        public string MaxDistanceMeters
        {
            get => _maxDistanceMeters;
            set => SetProperty(ref _maxDistanceMeters, value);
        }

        public string NameSimilarityThreshold
        {
            get => _nameSimilarityThreshold;
            set => SetProperty(ref _nameSimilarityThreshold, value);
        }

        public string AddressSimilarityThreshold
        {
            get => _addressSimilarityThreshold;
            set => SetProperty(ref _addressSimilarityThreshold, value);
        }

        public string AcceptScoreThreshold
        {
            get => _acceptScoreThreshold;
            set => SetProperty(ref _acceptScoreThreshold, value);
        }

        public bool AllowNearbyOnlyMatches
        {
            get => _allowNearbyOnlyMatches;
            set => SetProperty(ref _allowNearbyOnlyMatches, value);
        }

        public bool GenerateStableLinkIds
        {
            get => _generateStableLinkIds;
            set
            {
                if (SetProperty(ref _generateStableLinkIds, value))
                {
                    _ = RefreshFieldMappingPreviewAsync();
                    RaiseCommandStatesChanged();
                }
            }
        }

        public bool RelateLinkageToSourceLayer
        {
            get => _relateLinkageToSourceLayer;
            set => SetProperty(ref _relateLinkageToSourceLayer, value);
        }

        public bool HasLastGersifyRun
        {
            get => _hasLastGersifyRun;
            private set => SetProperty(ref _hasLastGersifyRun, value);
        }

        public string LastRunSummaryText
        {
            get => _lastRunSummaryText;
            private set => SetProperty(ref _lastRunSummaryText, value);
        }

        public string BridgeRoot
        {
            get => _bridgeRoot;
            set => SetProperty(ref _bridgeRoot, value);
        }

        public string BridgeRelease
        {
            get => _bridgeRelease;
            set => SetProperty(ref _bridgeRelease, value);
        }

        public string BridgeTheme
        {
            get => _bridgeTheme;
            set => SetProperty(ref _bridgeTheme, value);
        }

        public string BridgeType
        {
            get => _bridgeType;
            set => SetProperty(ref _bridgeType, value);
        }

        public bool IsRunning
        {
            get => _isBusy;
            set
            {
                if (SetProperty(ref _isBusy, value))
                    RaiseCommandStatesChanged();
            }
        }

        public string StatusText
        {
            get => _statusText;
            set => SetProperty(ref _statusText, value);
        }

        public string LogText
        {
            get => _logText;
            set => SetProperty(ref _logText, value);
        }

        public string FieldMappingPreviewText
        {
            get => _fieldMappingPreviewText;
            set => SetProperty(ref _fieldMappingPreviewText, value);
        }

        public string FieldMappingWarningText
        {
            get => _fieldMappingWarningText;
            set => SetProperty(ref _fieldMappingWarningText, value);
        }

        public string LinkagePreviewText
        {
            get => _linkagePreviewText;
            private set => SetProperty(ref _linkagePreviewText, value);
        }

        public ICommand RefreshLayersCommand { get; }
        public ICommand BrowseReleaseFolderCommand { get; }
        public ICommand BrowseOutputFolderCommand { get; }
        public ICommand RunGersifyCommand { get; }
        public ICommand RunTraceSourcesCommand { get; }
        public ICommand OpenReviewFolderCommand { get; }
        public ICommand OpenCandidatesCsvCommand { get; }
        public ICommand OpenBridgeCsvCommand { get; }

        private async Task RefreshLayersAsync()
        {
            try
            {
                var layers = await QueuedTask.Run(() =>
                {
                    var map = MapView.Active?.Map;
                    if (map == null)
                        return new List<LayerSelectionItem>();

                    return map.GetLayersAsFlattenedList()
                        .OfType<FeatureLayer>()
                        .Select(layer => new LayerSelectionItem(layer.Name, layer))
                        .OrderBy(layer => layer.Name, StringComparer.OrdinalIgnoreCase)
                        .ToList();
                });

                InputLayers.Clear();
                TraceLayers.Clear();
                foreach (var layer in layers)
                {
                    InputLayers.Add(layer);
                    TraceLayers.Add(layer);
                }

                SelectedInputLayer ??= InputLayers.FirstOrDefault();
                SelectedTraceLayer ??= TraceLayers.FirstOrDefault();
                StatusText = layers.Count == 0 ? "Open a map with feature layers to start." : $"Found {layers.Count} feature layer(s).";
            }
            catch (Exception ex)
            {
                StatusText = $"Could not refresh map layers: {ex.Message}";
                AddToLog(ex.ToString());
            }
        }

        private async Task RefreshInputFieldsAsync(LayerSelectionItem layerItem)
        {
            var fields = await ReadLayerFieldsAsync(layerItem);
            InputFields.Clear();
            OptionalInputFields.Clear();
            OptionalInputFields.Add(string.Empty);
            foreach (var field in fields)
            {
                InputFields.Add(field);
                OptionalInputFields.Add(field);
            }

            SelectedIdField = GersifyRecordIdPolicy.SelectPreferredIdField(fields);
            if (string.IsNullOrWhiteSpace(SelectedIdField))
            {
                string fallbackIdField = SelectField(fields, "objectid", "fid");
                if (!string.IsNullOrWhiteSpace(fallbackIdField))
                {
                    SelectedIdField = fallbackIdField;
                    GenerateStableLinkIds = true;
                }
                else
                {
                    SelectedIdField = fields.FirstOrDefault();
                }
            }
            SelectedNameField = SelectField(fields, "name", "business_name", "poi_name", "label", "title");
            SelectedAddressField = SelectField(fields, "address", "full_address", "street_address", "addr", "situs");
            SelectedStreetNumberField = SelectField(fields, "address_number", "address_num", "addressno", "house_number", "housenumber", "addrnum", "saddno", "situs_number", "number");
            SelectedStreetFractionField = SelectField(fields, "address_fraction", "fraction", "saddfrac");
            SelectedStreetPrefixField = SelectField(fields, "address_prefix", "address_street_prefix", "address_direction", "street_direction", "street_dir", "street_pre_direction", "street_predirection", "street_pre_dir", "street_prefix", "street_predir", "predir", "pre_direction", "pre_dir", "saddpref", "prefix");
            SelectedStreetNameField = SelectField(fields, "address_street_name", "address_street", "address_road", "street_name", "street", "road", "saddstr", "streetname");
            SelectedStreetTypeField = SelectField(fields, "address_street_type", "address_type", "street_type", "sttype", "saddsttyp", "street_suffix_type");
            SelectedStreetSuffixField = SelectField(fields, "address_suffix", "address_street_suffix", "address_post_direction", "street_suffix", "street_post_direction", "street_postdirection", "street_post_dir", "street_postdir", "postdir", "post_direction", "post_dir", "saddstsuf", "suffix");
            SelectedUnitField = SelectField(fields, "address_unit", "unit", "apt", "apartment", "suite", "sunit");
            SelectedCityField = SelectField(fields, "address_zipcity", "city", "locality", "town", "scity");
            SelectedStateField = SelectField(fields, "address_state", "state", "region", "province", "state2");
            SelectedPostcodeField = SelectField(fields, "address_zip5", "postcode", "postal_code", "zip", "zip5", "address_zip4");
            _ = RefreshFieldMappingPreviewAsync();
        }

        private async Task RefreshFieldMappingPreviewAsync()
        {
            var layerItem = SelectedInputLayer;
            if (layerItem?.Layer == null)
            {
                FieldMappingPreviewText = "Choose an input layer to preview the resolved field mapping.";
                FieldMappingWarningText = string.Empty;
                return;
            }

            string idField = SelectedIdField;
            string nameField = SelectedNameField;
            string addressField = SelectedAddressField;
            string streetNumberField = SelectedStreetNumberField;
            string streetFractionField = SelectedStreetFractionField;
            string streetPrefixField = SelectedStreetPrefixField;
            string streetNameField = SelectedStreetNameField;
            string streetTypeField = SelectedStreetTypeField;
            string streetSuffixField = SelectedStreetSuffixField;
            string unitField = SelectedUnitField;
            string cityField = SelectedCityField;
            string stateField = SelectedStateField;
            string postcodeField = SelectedPostcodeField;
            bool generateStableLinkIds = GenerateStableLinkIds;
            var inputFields = InputFields.ToList();

            try
            {
                var preview = await QueuedTask.Run(() => BuildFieldMappingPreview(
                    layerItem.Layer,
                    inputFields,
                    idField,
                    nameField,
                    addressField,
                    streetNumberField,
                    streetFractionField,
                    streetPrefixField,
                    streetNameField,
                    streetTypeField,
                    streetSuffixField,
                    unitField,
                    cityField,
                    stateField,
                    postcodeField,
                    generateStableLinkIds));

                if (ReferenceEquals(layerItem, SelectedInputLayer))
                {
                    FieldMappingPreviewText = preview.PreviewText;
                    FieldMappingWarningText = preview.WarningText;
                }
            }
            catch (Exception ex)
            {
                FieldMappingPreviewText = "Could not preview field mapping.";
                FieldMappingWarningText = ex.Message;
            }
        }

        private async Task RefreshTraceFieldsAsync(LayerSelectionItem layerItem)
        {
            var fields = await ReadLayerFieldsAsync(layerItem);
            TraceFields.Clear();
            foreach (var field in fields)
            {
                TraceFields.Add(field);
            }

            SelectedTraceGersIdField = SelectField(fields, "gers_id", "id", "overture_id") ?? fields.FirstOrDefault();
        }

        private static async Task<List<string>> ReadLayerFieldsAsync(LayerSelectionItem layerItem)
        {
            if (layerItem?.Layer == null)
                return [];

            return await QueuedTask.Run(() =>
            {
                using var featureClass = layerItem.Layer.GetFeatureClass();
                var definition = featureClass.GetDefinition();
                return definition.GetFields()
                    .Select(field => field.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            });
        }

        private static FieldMappingPreview BuildFieldMappingPreview(
            FeatureLayer layer,
            IReadOnlyCollection<string> inputFields,
            string idField,
            string nameField,
            string addressField,
            string streetNumberField,
            string streetFractionField,
            string streetPrefixField,
            string streetNameField,
            string streetTypeField,
            string streetSuffixField,
            string unitField,
            string cityField,
            string stateField,
            string postcodeField,
            bool generateStableLinkIds)
        {
            var warnings = new List<string>();
            warnings.AddRange(GersifyRecordIdPolicy.BuildIdFieldWarnings(idField, generateStableLinkIds));
            var addressSamples = new List<string>();
            var postcodeSamples = new List<string>();
            bool sampledSelectedPostcodeWithDigits = false;
            bool sampledSelectedPostcodeWithoutDigits = false;
            bool sampledUnitValues = false;

            using var featureClass = layer.GetFeatureClass();
            int previewRowCount = 0;
            using var cursor = featureClass.Search(new QueryFilter(), false);
            while (cursor.MoveNext() && previewRowCount < 25)
            {
                previewRowCount++;
                using var row = cursor.Current;
                string address = BuildAddressValue(row, addressField, streetNumberField, streetFractionField, streetPrefixField, streetNameField, streetTypeField, streetSuffixField, unitField);
                if (!string.IsNullOrWhiteSpace(address) && addressSamples.Count < 5)
                    addressSamples.Add(address);

                string selectedPostcode = CleanPart(GetRowValue(row, postcodeField));
                if (!string.IsNullOrWhiteSpace(selectedPostcode))
                {
                    if (selectedPostcode.Any(char.IsDigit))
                        sampledSelectedPostcodeWithDigits = true;
                    else
                        sampledSelectedPostcodeWithoutDigits = true;
                }

                string postcode = BuildPostcodeValue(row, postcodeField, inputFields);
                if (!string.IsNullOrWhiteSpace(postcode))
                    postcodeSamples.Add(postcode);

                if (!sampledUnitValues && !string.IsNullOrWhiteSpace(GetRowValue(row, unitField)))
                    sampledUnitValues = true;
            }

            if (string.IsNullOrWhiteSpace(addressField) && string.IsNullOrWhiteSpace(streetNameField))
                warnings.Add("No full-address or street-name field is selected.");
            if (sampledSelectedPostcodeWithoutDigits && !sampledSelectedPostcodeWithDigits)
                warnings.Add($"Selected postcode field '{postcodeField}' has no digits in the preview sample; export will fall back to known ZIP/postcode aliases when possible.");
            if (HasLikelyDirectionField(inputFields) && string.IsNullOrWhiteSpace(streetPrefixField) && string.IsNullOrWhiteSpace(streetSuffixField))
                warnings.Add("A likely street direction field exists, but Prefix and Suffix are blank.");
            if (sampledUnitValues)
                warnings.Add("Unit/subaddress values are present. Overture Addresses may only contain base address candidates for some records.");

            string mappingText = string.Join(Environment.NewLine, new[]
            {
                $"Unique ID field: {FormatField(idField)}",
                $"Stable link IDs: {(generateStableLinkIds ? "generated UUIDs in record_id" : "use selected field value in record_id")}",
                $"Audit key column: {GersifyRecordIdPolicy.SourceRecordKeyFieldName}",
                $"Name: {FormatField(nameField)}",
                $"Full Address: {FormatField(addressField)}",
                $"Number: {FormatField(streetNumberField)}",
                $"Fraction: {FormatField(streetFractionField)}",
                $"Prefix: {FormatField(streetPrefixField)}",
                $"Street: {FormatField(streetNameField)}",
                $"Type: {FormatField(streetTypeField)}",
                $"Suffix: {FormatField(streetSuffixField)}",
                $"Unit: {FormatField(unitField)}",
                $"City: {FormatField(cityField)}",
                $"State: {FormatField(stateField)}",
                $"Postcode: {FormatField(postcodeField)}"
            });

            string sampleText = string.Join(Environment.NewLine,
                addressSamples
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Take(3)
                    .Select((value, index) => $"Address sample {index + 1}: {value}")
                    .Concat(postcodeSamples
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .Take(3)
                        .Select((value, index) => $"Postcode sample {index + 1}: {value}")));

            if (string.IsNullOrWhiteSpace(sampleText))
                sampleText = "No non-empty address or postcode samples found in the first preview rows.";

            return new FieldMappingPreview(
                $"{mappingText}{Environment.NewLine}{Environment.NewLine}{sampleText}",
                warnings.Count == 0 ? "Mapping preview found no obvious issues in the sample." : string.Join(Environment.NewLine, warnings.Select(warning => $"- {warning}")));
        }

        private static bool HasLikelyDirectionField(IEnumerable<string> fields)
        {
            return fields.Any(field =>
            {
                string normalized = NormalizeFieldName(field);
                return normalized.Contains("direction", StringComparison.OrdinalIgnoreCase) ||
                       normalized.EndsWith("dir", StringComparison.OrdinalIgnoreCase) ||
                       normalized.Contains("predir", StringComparison.OrdinalIgnoreCase) ||
                       normalized.Contains("postdir", StringComparison.OrdinalIgnoreCase);
            });
        }

        private static string FormatField(string fieldName) =>
            string.IsNullOrWhiteSpace(fieldName) ? "(blank)" : fieldName;

        private async Task RunGersifyAsync()
        {
            IsRunning = true;
            ClearLog();
            string inputCsvPath = null;
            try
            {
                string releaseFolder = ResolveReleaseFolder();
                if (string.IsNullOrWhiteSpace(releaseFolder) || !Directory.Exists(releaseFolder))
                {
                    StatusText = "Choose a loaded Overture release folder.";
                    return;
                }

                Directory.CreateDirectory(OutputFolder);
                StatusText = "Exporting input points...";
                AddToLog($"Input layer: {SelectedInputLayer.Name}");
                var export = await ExportPointLayerAsync(
                    SelectedInputLayer.Layer,
                    SelectedIdField,
                    GenerateStableLinkIds,
                    SelectedNameField,
                    SelectedAddressField,
                    SelectedStreetNumberField,
                    SelectedStreetFractionField,
                    SelectedStreetPrefixField,
                    SelectedStreetNameField,
                    SelectedStreetTypeField,
                    SelectedStreetSuffixField,
                    SelectedUnitField,
                    SelectedCityField,
                    SelectedStateField,
                    SelectedPostcodeField);
                inputCsvPath = export.CsvPath;
                AddToLog($"Exported {export.FeatureCount:N0} point feature(s).");
                if (GersifyRecordIdPolicy.ShouldGenerateStableLinkId(SelectedIdField, GenerateStableLinkIds))
                {
                    AddToLog("Stable link ID mode: record_id uses durable UUIDs; source_record_key keeps the selected field value for audit and map relates.");
                }

                var progress = new Progress<string>(message =>
                {
                    StatusText = message;
                    AddToLog(message);
                });

                using var service = new GersifyService();
                var targetType = SelectedGersifyTarget?.TargetType ?? GersifyTargetType.Addresses;
                var result = await service.GersifyAsync(new GersifyOptions
                {
                    TargetType = targetType,
                    InputCsvPath = export.CsvPath,
                    InputLayerName = SelectedInputLayer.Name,
                    UniqueIdField = SelectedIdField,
                    NameField = SelectedNameField,
                    AddressField = SelectedAddressField,
                    StreetNumberField = SelectedStreetNumberField,
                    StreetFractionField = SelectedStreetFractionField,
                    StreetPrefixField = SelectedStreetPrefixField,
                    StreetNameField = SelectedStreetNameField,
                    StreetTypeField = SelectedStreetTypeField,
                    StreetSuffixField = SelectedStreetSuffixField,
                    UnitField = SelectedUnitField,
                    CityField = SelectedCityField,
                    StateField = SelectedStateField,
                    PostcodeField = SelectedPostcodeField,
                    OvertureReleaseFolder = releaseFolder,
                    OutputFolder = OutputFolder,
                    DatasetName = string.IsNullOrWhiteSpace(DatasetName) ? "user_data" : DatasetName.Trim(),
                    InputExtent = export.Extent,
                    MaxDistanceMeters = ParseDouble(MaxDistanceMeters, 75),
                    NameSimilarityThreshold = ParseDouble(NameSimilarityThreshold, 0.86),
                    AddressSimilarityThreshold = ParseDouble(AddressSimilarityThreshold, 0.72),
                    AcceptScoreThreshold = ParseDouble(AcceptScoreThreshold, 72),
                    AllowNearbyOnlyMatches = AllowNearbyOnlyMatches,
                    GenerateStableLinkIds = GenerateStableLinkIds
                }, progress);

                StatusText = "Adding GERSify outputs to the map...";
                string targetSuffix = string.IsNullOrWhiteSpace(result.TargetDatasetType) ? "data" : result.TargetDatasetType;
                string layerName = $"GERSified_{SelectedInputLayer.Name}_{targetSuffix}";
                result.OutputFeatureClassPath = await WriteGersifiedFeatureClassAsync(result.OutputCsvPath, OutputFolder, layerName);
                await TryAddStandaloneTableAsync(result.CandidateCsvPath);
                await TryAddStandaloneTableAsync(result.BridgeCsvPath);

                string relateSummary = "Map relate skipped.";
                if (RelateLinkageToSourceLayer && result.AcceptedCount > 0)
                {
                    string outputRelateField = GersifyRecordIdPolicy.ResolveOutputRelateField(
                        SelectedIdField,
                        GenerateStableLinkIds);
                    bool relateCreated = await TryRelateLinkageToSourceLayerAsync(
                        SelectedInputLayer.Layer,
                        SelectedIdField,
                        result.OutputFeatureClassPath,
                        outputRelateField);
                    relateSummary = relateCreated
                        ? $"Map relate created on '{SelectedInputLayer.Name}' — use the Attributes pane related records to inspect linkage fields."
                        : "Map relate could not be created automatically. Use the GERSified output layer or bridge CSV for review.";
                    AddToLog(relateSummary);
                }

                RememberLastGersifyRun(result, relateSummary);

                StatusText = $"Matched {result.AcceptedCount:N0} of {result.InputCount:N0} feature(s); reviewed {result.CandidateCount:N0} candidate(s). Source layer unchanged.";
                AddToLog("Linkage policy: link_dont_replace — authoritative local geometry was preserved; results are in a new output layer.");
                AddToLog($"Target: {result.TargetLabel} ({result.TargetTheme}/{result.TargetDatasetType}).");
                AddToLog($"Input text coverage: {result.InputNameCount:N0} with names; {result.InputAddressCount:N0} with addresses.");
                AddToLog($"Overture candidate address coverage: {result.CandidateOvertureAddressCount:N0} with address text; {result.CandidateAddressSimilarityCount:N0} with address similarity scores.");
                if (string.Equals(result.TargetDatasetType, "place", StringComparison.OrdinalIgnoreCase) &&
                    result.CandidateCount > 0 &&
                    result.CandidateAddressSimilarityCount == 0)
                {
                    AddToLog("No address similarity scores were produced. The selected Overture Places files likely do not include address_freeform/address_* fields or nested addresses; close map layers using old Places files and re-download Places with this add-in version to enable address-based GERSify scoring.");
                }
                AddToLog($"Accepted by strategy: {FormatStrategyCounts(result.AcceptedStrategyCounts)}.");
                AddToLog($"Candidates reviewed: {result.CandidateCount:N0}; accepted matches: {result.AcceptedCount:N0}.");
                AddToLog($"Output feature class: {result.OutputFeatureClassPath}");
                AddToLog($"Candidate review CSV: {result.CandidateCsvPath}");
                AddToLog($"Bridge CSV: {result.BridgeCsvPath}");
                AddToLog("Bridge CSV includes linkage_policy, source_geometry_policy, match_strategy, match_score, overture_release, source_layer, and linkage_tool for provenance review.");
            }
            catch (Exception ex)
            {
                StatusText = $"GERSify failed: {ex.Message}";
                AddToLog(ex.ToString());
            }
            finally
            {
                TryDelete(inputCsvPath);
                IsRunning = false;
            }
        }

        private async Task RunTraceSourcesAsync()
        {
            IsRunning = true;
            ClearLog();
            string inputCsvPath = null;
            try
            {
                Directory.CreateDirectory(OutputFolder);
                StatusText = "Exporting GERS IDs...";
                inputCsvPath = await ExportGersIdsAsync(SelectedTraceLayer.Layer, SelectedTraceGersIdField);
                AddToLog($"Trace layer: {SelectedTraceLayer.Name}");

                var progress = new Progress<string>(message =>
                {
                    StatusText = message;
                    AddToLog(message);
                });

                using var service = new BridgeFileService();
                var result = await service.TraceSourcesAsync(new TraceSourcesOptions
                {
                    InputCsvPath = inputCsvPath,
                    OutputFolder = OutputFolder,
                    BridgeRoot = BridgeRoot,
                    Release = BridgeRelease,
                    Theme = BridgeTheme,
                    Type = BridgeType
                }, progress);

                await TryAddStandaloneTableAsync(result.OutputCsvPath);
                StatusText = $"Found {result.OutputCount:N0} source record(s) for {result.InputCount:N0} GERS ID(s).";
                AddToLog($"Source lookup CSV: {result.OutputCsvPath}");
            }
            catch (Exception ex)
            {
                StatusText = $"Trace Sources failed: {ex.Message}";
                AddToLog(ex.ToString());
            }
            finally
            {
                TryDelete(inputCsvPath);
                IsRunning = false;
            }
        }

        private static async Task<GersifyInputExport> ExportPointLayerAsync(
            FeatureLayer layer,
            string idField,
            bool generateStableLinkIds,
            string nameField,
            string addressField,
            string streetNumberField,
            string streetFractionField,
            string streetPrefixField,
            string streetNameField,
            string streetTypeField,
            string streetSuffixField,
            string unitField,
            string cityField,
            string stateField,
            string postcodeField)
        {
            string csvPath = Path.Combine(Path.GetTempPath(), $"gersify_input_{Guid.NewGuid():N}.csv");
            return await QueuedTask.Run(() =>
            {
                int count = 0;
                double xMin = double.MaxValue;
                double yMin = double.MaxValue;
                double xMax = double.MinValue;
                double yMax = double.MinValue;
                var sb = new StringBuilder();
                sb.AppendLine("record_id,source_record_key,name,address,city,state,postcode,longitude,latitude");

                using var featureClass = layer.GetFeatureClass();
                var availableFields = featureClass.GetDefinition()
                    .GetFields()
                    .Select(field => field.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                using var cursor = featureClass.Search(new QueryFilter(), false);
                while (cursor.MoveNext())
                {
                    using var row = cursor.Current;
                    if (row is not Feature feature)
                        continue;

                    Geometry geometry = feature.GetShape();
                    if (geometry == null)
                        continue;

                    if (geometry.SpatialReference != null && geometry.SpatialReference.Wkid != 4326)
                    {
                        geometry = GeometryEngine.Instance.Project(geometry, SpatialReferences.WGS84);
                    }

                    MapPoint point = geometry as MapPoint;
                    if (point == null && geometry.Extent != null)
                    {
                        point = geometry.Extent.Center;
                    }

                    if (point == null || double.IsNaN(point.X) || double.IsNaN(point.Y))
                        continue;

                    string sourceRecordKey = GetRowValue(row, idField);
                    if (string.IsNullOrWhiteSpace(sourceRecordKey))
                    {
                        sourceRecordKey = (count + 1).ToString(CultureInfo.InvariantCulture);
                    }

                    string recordId = GersifyRecordIdPolicy.ShouldGenerateStableLinkId(idField, generateStableLinkIds)
                        ? Guid.NewGuid().ToString("D")
                        : sourceRecordKey;

                    sb.AppendLine(string.Join(",",
                        Csv(recordId),
                        Csv(sourceRecordKey),
                        Csv(GetRowValue(row, nameField)),
                        Csv(BuildAddressValue(row, addressField, streetNumberField, streetFractionField, streetPrefixField, streetNameField, streetTypeField, streetSuffixField, unitField)),
                        Csv(GetRowValue(row, cityField)),
                        Csv(GetRowValue(row, stateField)),
                        Csv(BuildPostcodeValue(row, postcodeField, availableFields)),
                        point.X.ToString("G", CultureInfo.InvariantCulture),
                        point.Y.ToString("G", CultureInfo.InvariantCulture)));

                    xMin = Math.Min(xMin, point.X);
                    yMin = Math.Min(yMin, point.Y);
                    xMax = Math.Max(xMax, point.X);
                    yMax = Math.Max(yMax, point.Y);
                    count++;
                }

                File.WriteAllText(csvPath, sb.ToString(), Encoding.UTF8);
                ExtentBounds extent = count == 0 ? null : new ExtentBounds(xMin, yMin, xMax, yMax);
                return new GersifyInputExport(csvPath, count, extent);
            });
        }

        private static async Task<string> ExportGersIdsAsync(FeatureLayer layer, string gersIdField)
        {
            string csvPath = Path.Combine(Path.GetTempPath(), $"gers_trace_ids_{Guid.NewGuid():N}.csv");
            return await QueuedTask.Run(() =>
            {
                var sb = new StringBuilder();
                sb.AppendLine("gers_id");
                using var featureClass = layer.GetFeatureClass();
                using var cursor = featureClass.Search(new QueryFilter(), false);
                while (cursor.MoveNext())
                {
                    using var row = cursor.Current;
                    string gersId = GetRowValue(row, gersIdField);
                    if (!string.IsNullOrWhiteSpace(gersId))
                    {
                        sb.AppendLine(Csv(gersId));
                    }
                }

                File.WriteAllText(csvPath, sb.ToString(), Encoding.UTF8);
                return csvPath;
            });
        }

        private static async Task<string> WriteGersifiedFeatureClassAsync(string outputCsvPath, string outputFolder, string layerName)
        {
            string suffix = GersifyOptions.BuildTimestampSuffix();
            string gdbPath;
            if (string.Equals(Path.GetExtension(outputFolder), ".gdb", StringComparison.OrdinalIgnoreCase))
            {
                gdbPath = outputFolder;
                if (!Directory.Exists(gdbPath))
                {
                    string parentFolder = Path.GetDirectoryName(gdbPath);
                    string gdbName = Path.GetFileName(gdbPath);
                    if (string.IsNullOrWhiteSpace(parentFolder) || string.IsNullOrWhiteSpace(gdbName))
                    {
                        throw new DirectoryNotFoundException($"Could not resolve file geodatabase path: {gdbPath}");
                    }

                    Directory.CreateDirectory(parentFolder);
                    var createResult = await Geoprocessing.ExecuteToolAsync(
                        "management.CreateFileGDB",
                        Geoprocessing.MakeValueArray(parentFolder, gdbName),
                        null,
                        flags: GPExecuteToolFlags.None);
                    if (createResult.IsFailed)
                    {
                        throw new InvalidOperationException("Create File Geodatabase failed: " +
                            string.Join(" | ", createResult.Messages.Select(message => message.Text)));
                    }
                }
            }
            else
            {
                Directory.CreateDirectory(outputFolder);
                string gdbName = $"GERSify_{suffix}.gdb";
                gdbPath = Path.Combine(outputFolder, gdbName);
                if (!Directory.Exists(gdbPath))
                {
                    var createResult = await Geoprocessing.ExecuteToolAsync(
                        "management.CreateFileGDB",
                        Geoprocessing.MakeValueArray(outputFolder, gdbName),
                        null,
                        flags: GPExecuteToolFlags.None);
                    if (createResult.IsFailed)
                    {
                        throw new InvalidOperationException("Create File Geodatabase failed: " +
                            string.Join(" | ", createResult.Messages.Select(message => message.Text)));
                    }
                }
            }

            string featureClassName = ArcGisNameSanitizer.ToFileGeodatabaseFeatureClassName(layerName);

            string outputFeatureClassPath = Path.Combine(gdbPath, featureClassName);
            var result = await Geoprocessing.ExecuteToolAsync(
                "management.XYTableToPoint",
                Geoprocessing.MakeValueArray(outputCsvPath, outputFeatureClassPath, "longitude", "latitude", "", SpatialReferences.WGS84),
                null,
                flags: GPExecuteToolFlags.None);

            if (result.IsFailed)
            {
                throw new InvalidOperationException("XY Table To Point failed: " +
                    string.Join(" | ", result.Messages.Select(message => message.Text)));
            }

            await QueuedTask.Run(() =>
            {
                var map = MapView.Active?.Map;
                if (map != null)
                {
                    LayerFactory.Instance.CreateLayer(ToFileUri(outputFeatureClassPath), map, layerName: featureClassName);
                }
            });

            return outputFeatureClassPath;
        }

        private void RememberLastGersifyRun(GersifyResult result, string relateSummary)
        {
            _lastCandidateCsvPath = result.CandidateCsvPath;
            _lastBridgeCsvPath = result.BridgeCsvPath;
            _lastOutputFeatureClassPath = result.OutputFeatureClassPath;
            HasLastGersifyRun = true;
            LastRunSummaryText =
                $"Accepted {result.AcceptedCount:N0} of {result.InputCount:N0} features against {result.TargetLabel}. {relateSummary}";
            RaiseCommandStatesChanged();
        }

        private bool CanOpenLastRunFile(string path) =>
            HasLastGersifyRun && !IsRunning && !string.IsNullOrWhiteSpace(path) && File.Exists(path);

        private bool CanOpenLastRunArtifact(params string[] paths) =>
            HasLastGersifyRun && !IsRunning && paths.Any(path => !string.IsNullOrWhiteSpace(path) && (File.Exists(path) || Directory.Exists(path)));

        private void OpenReviewFolder()
        {
            string folderPath = ResolveReviewFolderPath();
            if (string.IsNullOrWhiteSpace(folderPath) || !Directory.Exists(folderPath))
            {
                StatusText = "GERSify output folder was not found.";
                return;
            }

            OpenPathInShell(folderPath);
            StatusText = "Opened GERSify output folder.";
        }

        private string ResolveReviewFolderPath()
        {
            if (!string.IsNullOrWhiteSpace(_lastOutputFeatureClassPath))
            {
                string gdbPath = Path.GetDirectoryName(_lastOutputFeatureClassPath);
                if (!string.IsNullOrWhiteSpace(gdbPath) && Directory.Exists(gdbPath))
                    return gdbPath;
            }

            if (!string.IsNullOrWhiteSpace(_lastCandidateCsvPath))
            {
                string folder = Path.GetDirectoryName(_lastCandidateCsvPath);
                if (!string.IsNullOrWhiteSpace(folder) && Directory.Exists(folder))
                    return folder;
            }

            return string.IsNullOrWhiteSpace(OutputFolder) ? null : OutputFolder;
        }

        private void OpenFileInShell(string filePath)
        {
            if (!CanOpenLastRunFile(filePath))
            {
                StatusText = "GERSify output file was not found.";
                return;
            }

            OpenPathInShell(filePath);
            StatusText = $"Opened {Path.GetFileName(filePath)}.";
        }

        private static void OpenPathInShell(string path)
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = path,
                UseShellExecute = true
            });
        }

        private static async Task<bool> TryRelateLinkageToSourceLayerAsync(
            FeatureLayer sourceLayer,
            string idField,
            string outputFeatureClassPath,
            string outputRelateField)
        {
            if (sourceLayer == null ||
                string.IsNullOrWhiteSpace(idField) ||
                string.IsNullOrWhiteSpace(outputFeatureClassPath) ||
                string.IsNullOrWhiteSpace(outputRelateField))
            {
                return false;
            }

            string relateName = $"GERSify_{SanitizeRelateName(sourceLayer.Name)}";
            var result = await Geoprocessing.ExecuteToolAsync(
                "management.AddRelate",
                Geoprocessing.MakeValueArray(
                    sourceLayer,
                    idField,
                    outputFeatureClassPath,
                    outputRelateField,
                    relateName,
                    "SIMPLE"),
                null,
                flags: GPExecuteToolFlags.Default);

            if (result.IsFailed)
            {
                System.Diagnostics.Debug.WriteLine(
                    "AddRelate failed: " +
                    string.Join(" | ", result.Messages.Select(message => message.Text)));
            }

            return !result.IsFailed;
        }

        private static string SanitizeRelateName(string layerName)
        {
            if (string.IsNullOrWhiteSpace(layerName))
                return "Link";

            var sb = new StringBuilder(layerName.Length);
            foreach (char ch in layerName)
            {
                if (char.IsLetterOrDigit(ch) || ch == '_')
                    sb.Append(ch);
            }

            if (sb.Length == 0)
                return "Link";

            return sb.Length <= 40 ? sb.ToString() : sb.ToString(0, 40);
        }

        private static async Task TryAddStandaloneTableAsync(string tablePath)
        {
            if (string.IsNullOrWhiteSpace(tablePath) || !File.Exists(tablePath))
                return;

            await QueuedTask.Run(() =>
            {
                try
                {
                    var map = MapView.Active?.Map;
                    if (map != null)
                    {
                        StandaloneTableFactory.Instance.CreateStandaloneTable(ToFileUri(tablePath), map);
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Could not add standalone table '{tablePath}': {ex.Message}");
                }
            });
        }

        private static Uri ToFileUri(string path) =>
            new(Path.GetFullPath(path), UriKind.Absolute);

        private bool CanRunGersify()
        {
            return !IsRunning &&
                   SelectedInputLayer?.Layer != null &&
                   SelectedGersifyTarget != null &&
                   !string.IsNullOrWhiteSpace(SelectedIdField) &&
                   HasAnyGersifyMatchField() &&
                   !string.IsNullOrWhiteSpace(OutputFolder);
        }

        private bool HasAnyGersifyMatchField()
        {
            return !string.IsNullOrWhiteSpace(SelectedNameField) ||
                   !string.IsNullOrWhiteSpace(SelectedAddressField) ||
                   !string.IsNullOrWhiteSpace(SelectedStreetNameField);
        }

        private bool CanRunTraceSources()
        {
            return !IsRunning &&
                   SelectedTraceLayer?.Layer != null &&
                   !string.IsNullOrWhiteSpace(SelectedTraceGersIdField) &&
                   !string.IsNullOrWhiteSpace(OutputFolder) &&
                   !string.IsNullOrWhiteSpace(BridgeRoot) &&
                   !string.IsNullOrWhiteSpace(BridgeRelease);
        }

        private void BrowseReleaseFolder()
        {
            using var dialog = new FolderBrowserDialog
            {
                Description = "Select Overture release folder",
                UseDescriptionForTitle = true
            };
            if (!string.IsNullOrWhiteSpace(ReleaseFolderPath) && Directory.Exists(ReleaseFolderPath))
                dialog.SelectedPath = ReleaseFolderPath;

            if (dialog.ShowDialog() == DialogResult.OK)
            {
                ReleaseFolderPath = dialog.SelectedPath;
            }
        }

        private void BrowseOutputFolder()
        {
            using var dialog = new FolderBrowserDialog
            {
                Description = "Select GERSify output folder",
                UseDescriptionForTitle = true
            };
            if (!string.IsNullOrWhiteSpace(OutputFolder) && Directory.Exists(OutputFolder))
                dialog.SelectedPath = OutputFolder;

            if (dialog.ShowDialog() == DialogResult.OK)
            {
                OutputFolder = dialog.SelectedPath;
            }
        }

        private string ResolveReleaseFolder()
        {
            if (!string.IsNullOrWhiteSpace(ReleaseFolderPath))
                return ReleaseFolderPath;

            ReleaseFolderPath = ProjectDataLocator.GetNewestLoadedReleaseFolder();
            return ReleaseFolderPath;
        }

        private void RaiseCommandStatesChanged()
        {
            (RefreshLayersCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (BrowseReleaseFolderCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (BrowseOutputFolderCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (RunGersifyCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (RunTraceSourcesCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (OpenReviewFolderCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (OpenCandidatesCsvCommand as RelayCommand)?.RaiseCanExecuteChanged();
            (OpenBridgeCsvCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        private void AddToLog(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return;

            if (_logBuilder.Length > 0)
                _logBuilder.AppendLine();
            _logBuilder.Append(message);
            LogText = _logBuilder.ToString();
            System.Diagnostics.Debug.WriteLine(message);
        }

        private void ClearLog()
        {
            _logBuilder.Clear();
            LogText = string.Empty;
        }

        private static string FormatStrategyCounts(IReadOnlyDictionary<string, int> counts)
        {
            if (counts == null || counts.Count == 0)
                return "none";

            string[] preferredOrder = ["exact_address", "address", "name_address", "name", "nearby_only"];
            var ordered = preferredOrder
                .Where(counts.ContainsKey)
                .Select(strategy => new KeyValuePair<string, int>(strategy, counts[strategy]))
                .Concat(counts
                    .Where(item => !preferredOrder.Contains(item.Key, StringComparer.OrdinalIgnoreCase))
                    .OrderBy(item => item.Key, StringComparer.OrdinalIgnoreCase));

            return string.Join("; ", ordered.Select(item => $"{item.Key}: {item.Value:N0}"));
        }

        private static string SelectField(IEnumerable<string> fields, params string[] candidates)
        {
            var fieldList = fields?.ToList() ?? [];
            foreach (string candidate in candidates)
            {
                string match = fieldList.FirstOrDefault(field =>
                    string.Equals(NormalizeFieldName(field), NormalizeFieldName(candidate), StringComparison.OrdinalIgnoreCase));
                if (!string.IsNullOrWhiteSpace(match))
                    return match;
            }

            return null;
        }

        private static string NormalizeFieldName(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            var sb = new StringBuilder(value.Length);
            foreach (char ch in value)
            {
                if (char.IsLetterOrDigit(ch))
                    sb.Append(char.ToLowerInvariant(ch));
            }
            return sb.ToString();
        }

        private static string GetRowValue(Row row, string fieldName)
        {
            if (row == null || string.IsNullOrWhiteSpace(fieldName))
                return string.Empty;

            try
            {
                object value = row[fieldName];
                return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        private static string BuildAddressValue(
            Row row,
            string addressField,
            string streetNumberField,
            string streetFractionField,
            string streetPrefixField,
            string streetNameField,
            string streetTypeField,
            string streetSuffixField,
            string unitField)
        {
            string fullAddress = CleanPart(GetRowValue(row, addressField));
            if (!string.IsNullOrWhiteSpace(fullAddress))
                return fullAddress;

            return string.Join(" ", new[]
                {
                    CleanPart(GetRowValue(row, streetNumberField)),
                    CleanPart(GetRowValue(row, streetFractionField)),
                    CleanPart(GetRowValue(row, streetPrefixField)),
                    CleanPart(GetRowValue(row, streetNameField)),
                    CleanPart(GetRowValue(row, streetTypeField)),
                    CleanPart(GetRowValue(row, streetSuffixField)),
                    CleanPart(GetRowValue(row, unitField))
                }
                .Where(part => !string.IsNullOrWhiteSpace(part)));
        }

        private static string BuildPostcodeValue(Row row, string postcodeField, IReadOnlyCollection<string> availableFields = null)
        {
            string postcode = CleanPart(GetRowValue(row, postcodeField));
            if (postcode.Any(char.IsDigit))
                return postcode;

            string fallbackPostcode = SelectFirstNumericRowValue(row, availableFields,
                "ADDRESS_ZIP5",
                "address_zip5",
                "ZIP5",
                "zip5",
                "POSTCODE",
                "postcode",
                "POSTAL_CODE",
                "postal_code",
                "ZIP",
                "zip",
                "ADDRESS_ZIP4");
            return fallbackPostcode;
        }

        private static string SelectFirstNumericRowValue(Row row, IReadOnlyCollection<string> availableFields, params string[] fieldNames)
        {
            foreach (string fieldName in fieldNames)
            {
                if (availableFields != null && !availableFields.Contains(fieldName, StringComparer.OrdinalIgnoreCase))
                    continue;

                string value = CleanPart(GetRowValue(row, fieldName));
                if (value.Any(char.IsDigit))
                    return value;
            }

            return string.Empty;
        }

        private static string CleanPart(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            return string.Join(" ",
                value.Trim().Trim('"')
                    .Split((char[])null, StringSplitOptions.RemoveEmptyEntries));
        }

        private static string Csv(string value)
        {
            value ??= string.Empty;
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }

        private static double ParseDouble(string value, double fallback)
        {
            return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed)
                ? parsed
                : fallback;
        }

        private static void TryDelete(string path)
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
                    File.Delete(path);
            }
            catch
            {
                // Temp file cleanup is best effort.
            }
        }

        private sealed record GersifyInputExport(string CsvPath, int FeatureCount, ExtentBounds Extent);
        private sealed record FieldMappingPreview(string PreviewText, string WarningText);
    }

    public sealed class LayerSelectionItem
    {
        public LayerSelectionItem(string name, FeatureLayer layer)
        {
            Name = name;
            Layer = layer;
        }

        public string Name { get; }
        public FeatureLayer Layer { get; }
    }

    public sealed class GersifyTargetOption
    {
        public GersifyTargetOption(string name, GersifyTargetType targetType, string description)
        {
            Name = name;
            TargetType = targetType;
            Description = description;
        }

        public string Name { get; }
        public GersifyTargetType TargetType { get; }
        public string Description { get; }
        public string TargetTheme => TargetType == GersifyTargetType.Addresses ? "addresses" : "places";
        public string TargetDatasetType => TargetType == GersifyTargetType.Addresses ? "address" : "place";
    }
}
