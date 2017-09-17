using Halforbit.DataStores.FileStores.Model;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Interface
{
    public interface IFileStore
    {
        Task<bool> Delete(string path);

        Task<bool> Exists(string path);

        Task<IEnumerable<string>> GetFiles(
            string pathPrefix, 
            string extension);

        Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path,
            bool getETag = false);

        Task<bool> WriteAllBytes(
            string path, 
            byte[] contents,
            string eTag = null);
    }
}
