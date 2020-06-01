using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface ICompressor
    {
        Task<byte[]> Compress(byte[] value);

        Task<byte[]> Decompress(byte[] data);
    }
}
