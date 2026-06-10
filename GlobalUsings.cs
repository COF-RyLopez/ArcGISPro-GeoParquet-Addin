// Global Usings to resolve type ambiguities in ArcGIS Pro SDK 3.x
// These aliases resolve name conflicts between standard .NET namespaces and ArcGIS Core namespaces.
// Aliases take precedence over namespace imports, resolving conflicts without modifying source files.

global using Path = System.IO.Path;
global using Envelope = ArcGIS.Core.Geometry.Envelope;
global using Geometry = ArcGIS.Core.Geometry.Geometry;
global using Field = ArcGIS.Core.Data.Field;
global using QueryFilter = ArcGIS.Core.Data.QueryFilter;
global using EditOperation = ArcGIS.Desktop.Editing.EditOperation;
