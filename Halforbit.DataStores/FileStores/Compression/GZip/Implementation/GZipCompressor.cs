using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Compression.GZip.Implementation
{
    public class GZipCompressor : ICompressor
    {
        public async Task<byte[]> Compress(byte[] value)
        {
            using (var sourceStream = new MemoryStream(value))
            using (var destStream = new MemoryStream())
            {
                using (var gZipStream = new GZipStream(
                    destStream,
                    CompressionMode.Compress))
                {
                    await sourceStream.CopyToAsync(gZipStream).ConfigureAwait(false);
                }

                return destStream.ToArray();
            }
        }

        public async Task<byte[]> Decompress(byte[] data)
        {
            using (var sourceStream = new MemoryStream(data))
            using (var gZipStream = new GZipStream(sourceStream, CompressionMode.Decompress))
            using (var destStream = new MemoryStream())
            {
                await gZipStream.CopyToAsync(destStream).ConfigureAwait(false);

                return destStream.ToArray();
            }
        }
    }
}
