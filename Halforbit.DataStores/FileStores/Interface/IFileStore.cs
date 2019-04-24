using Halforbit.DataStores.FileStores.Model;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Interface
{
    public interface IFileStore
    {
        IFileStoreContext FileStoreContext { get; }

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

        Task<Stream> ReadStream(string path, bool getETag = false);

        Task<bool> WriteStream(
            string path,
            Stream contents,
            string eTag = null);
    }
}
