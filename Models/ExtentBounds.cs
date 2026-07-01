namespace DuckDBGeoparquet.Models
{
    /// <summary>
    /// Typed spatial extent replacing <c>dynamic</c> extent parameters
    /// throughout DataProcessor to restore compile-time safety.
    /// </summary>
    public record ExtentBounds(double XMin, double YMin, double XMax, double YMax);
}
