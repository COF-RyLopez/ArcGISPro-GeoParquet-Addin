using System.Threading.Tasks;

namespace DuckDBGeoparquet.Services
{
    public interface IFileHandler
    {
        Task DeleteFileAsync(string path);
    }
}
