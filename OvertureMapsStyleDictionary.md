# Overture Maps Custom Style Dictionary Implementation Plan

## Overview
This document outlines the implementation of a custom Esri dictionary renderer for Overture Maps data, designed to automatically style the six major themes with appropriate, meaningful symbology.

## Dictionary Structure

Based on the [Esri Dictionary Renderer Toolkit](https://github.com/Esri/dictionary-renderer-toolkit/blob/master/docs/tips-for-creating-custom-dictionaries.md), we'll create a comprehensive style dictionary that covers all Overture Maps themes.

### 1. Transportation Theme
**Primary Attributes**: `subtype`, `class`, `subclass`

#### Roads (`subtype = "road"`)
- **Motorway**: High-contrast, thick lines (highway symbols)
- **Primary/Secondary**: Medium-weight lines, color-coded by importance
- **Residential**: Standard street symbology
- **Service**: Thinner lines, muted colors
- **Footway/Path**: Dashed or dotted lines
- **Cycleway**: Bike-specific styling

#### Rail (`subtype = "rail"`)
- Railroad track symbology with appropriate gauge

#### Water Transportation (`subtype = "water"`)
- Ferry route styling

### 2. Buildings Theme
**Primary Attributes**: `subtype`, `class`, `facade_material`, `roof_material`

#### By Building Type
- **Residential**: House/apartment symbols, varying by class
- **Commercial**: Commercial building symbols
- **Industrial**: Factory/warehouse symbols
- **Religious**: Church/temple symbols
- **Educational**: School symbols
- **Medical**: Hospital/clinic symbols
- **Civic**: Government building symbols

#### By Material (fill patterns)
- **Brick**: Brick pattern fill
- **Concrete**: Solid gray
- **Wood**: Wood pattern
- **Metal**: Metallic appearance

### 3. Places Theme
**Primary Attributes**: `categories.primary`, `confidence`

#### By Category
- **Retail**: Shopping symbols
- **Food & Drink**: Restaurant symbols
- **Accommodation**: Hotel symbols
- **Recreation**: Park/entertainment symbols
- **Services**: Service-specific symbols
- **Healthcare**: Medical symbols

#### By Confidence Level
- High confidence: Full opacity, larger symbols
- Medium confidence: Reduced opacity
- Low confidence: Small, muted symbols

### 4. Addresses Theme
**Primary Attributes**: `country`, `region`

- **Address Points**: Standardized address symbols
- **Size by administrative level**: Country-specific styling if needed

### 5. Divisions Theme
**Primary Attributes**: `subtype`, `class`

#### Administrative Levels
- **Country**: Thick boundary lines, distinctive color
- **Region/State**: Medium boundary lines
- **County**: Thinner lines
- **Locality/City**: Urban area styling
- **Neighborhood**: Light boundaries or fill areas

#### Maritime vs Land
- **Land boundaries**: Solid lines
- **Maritime boundaries**: Dashed lines

### 6. Base Theme
**Primary Attributes**: `type`, `subtype`, `class`

#### Land Features
- **Natural areas**: Green tones, nature patterns
- **Urban areas**: Gray/developed patterns
- **Agricultural**: Agricultural patterns

#### Water Features
- **Ocean**: Deep blue
- **Rivers/Streams**: Blue lines, varying width
- **Lakes**: Blue polygons

#### Infrastructure
- **Bridges**: Bridge symbols
- **Towers**: Tower symbols
- **Utilities**: Infrastructure-specific symbols

## Technical Implementation

### 1. Dictionary File Structure
```
OvertureMaps/
├── overture_maps.stylx           # Main style file
├── transportation.lyrx          # Transportation layer template
├── buildings.lyrx              # Buildings layer template
├── places.lyrx                 # Places layer template
├── addresses.lyrx              # Addresses layer template
├── divisions.lyrx              # Divisions layer template
├── base.lyrx                   # Base layer template
└── symbols/                    # Custom symbol library
    ├── transportation/
    ├── buildings/
    ├── places/
    └── infrastructure/
```

### 2. Integration Points in Add-in

#### A. Modify DataProcessor.cs
Add style application after layer creation:

```csharp
private async Task ApplyOvertureMapsStyling(FeatureLayer layer, string theme, string type)
{
    await QueuedTask.Run(() =>
    {
        var styleProjectItem = Project.Current.GetItems<StyleProjectItem>()
            .FirstOrDefault(item => item.Name.Contains("OvertureStyle"));
        
        if (styleProjectItem != null)
        {
            // Apply dictionary renderer based on theme and type
            ApplyDictionaryRenderer(layer, theme, type, styleProjectItem);
        }
    });
}
```

#### B. Update Layer Creation Logic
In the `AddAllLayersToMapAsync` method, add style application:

```csharp
// After layer creation
for (int i = 0; i < layers.Count && i < layerNames.Count; i++)
{
    if (layers[i] is FeatureLayer featureLayer)
    {
        ApplyLayerSettings(featureLayer);
        
        // Apply Overture Maps styling
        await ApplyOvertureMapsStyling(featureLayer, 
            layerInfo.ParentTheme, 
            layerInfo.ActualType);
    }
}
```

### 3. Configuration Integration

#### Add Style Options to UI
In `WizardDockpane.xaml`, add styling options:

```xml
<CheckBox Content="Apply Overture Maps Styling" 
          IsChecked="{Binding ApplyOvertureStyle}" 
          Margin="0,4,0,0"
          ToolTipService.ToolTip="Automatically apply professional cartographic styling to loaded data"/>
```

#### Style Management
- **Default**: Always apply Overture styling
- **User Choice**: Allow users to opt-out
- **Custom Styles**: Option to override with user's own styling

### 4. Dictionary Renderer Configuration

#### Rule Structure Example (Transportation)
```json
{
  "type": "CIMDictionaryRenderer",
  "dictionaryName": "overture_transportation",
  "fieldMap": [
    {"fieldName": "subtype", "dictionaryFieldName": "transport_type"},
    {"fieldName": "class", "dictionaryFieldName": "road_class"},
    {"fieldName": "subclass", "dictionaryFieldName": "road_subclass"}
  ],
  "symbolLayers": [
    {
      "rule": "transport_type == 'road' AND road_class == 'motorway'",
      "symbol": "highway_symbol",
      "scaleDependency": "true"
    }
  ]
}
```

## Development Phases

### Phase 1: Foundation
1. Create basic dictionary structure
2. Implement core transportation styling (most visible impact)
3. Integrate with existing layer creation logic

### Phase 2: Comprehensive Coverage
1. Complete all six themes
2. Add confidence-based and material-based styling
3. Scale-dependent rendering

### Phase 3: Advanced Features
1. Custom symbol library
2. User customization options
3. Export capabilities for style packages

### Phase 4: Distribution
1. Bundle styles with add-in installation
2. Auto-update mechanism for style improvements
3. Community feedback integration

## Benefits

### For Users
- **Immediate Visual Context**: Data is meaningful from the moment it loads
- **Professional Cartography**: Publication-ready maps without manual styling
- **Consistent Visualization**: Standardized appearance across different projects

### For the Add-in
- **Competitive Advantage**: No other Overture Maps tool offers this level of visualization
- **User Retention**: Better user experience leads to continued usage
- **Professional Credibility**: Demonstrates cartographic expertise

## Resource Requirements

### Development
- **Time**: 2-3 weeks for initial implementation
- **Skills**: ArcGIS Pro SDK, CIM knowledge, cartographic design
- **Testing**: Multiple themes, various geographic areas

### Maintenance
- **Updates**: Sync with Overture Maps schema changes
- **User Feedback**: Continuous improvement based on usage
- **Performance**: Monitor rendering performance with large datasets

## Success Metrics

1. **User Adoption**: Percentage of users who keep styling enabled
2. **Performance**: Layer rendering times with/without styling
3. **Feedback**: User satisfaction with default visualization
4. **Usage**: Frequency of manual style override

## Next Steps

1. **Research Phase**: Study existing Esri dictionary implementations
2. **Design Phase**: Create comprehensive symbol specifications
3. **Development Phase**: Implement core functionality
4. **Testing Phase**: Validate across different Overture Maps themes
5. **Documentation Phase**: Create user guides and technical documentation 