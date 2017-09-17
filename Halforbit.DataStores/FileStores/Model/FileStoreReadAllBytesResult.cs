
namespace Halforbit.DataStores.FileStores.Model
{
    public class FileStoreReadAllBytesResult
    {
        public FileStoreReadAllBytesResult(
            byte[] bytes,
            string eTag = null)
        {
            Bytes = bytes;

            ETag = eTag;
        }

        public byte[] Bytes { get; }

        public string ETag { get; }
    }
}
