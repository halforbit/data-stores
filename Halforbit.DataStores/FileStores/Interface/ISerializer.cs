using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Interface
{
    public interface ISerializer
    {
        Task<byte[]> Serialize<TValue>(TValue value);

        Task<TValue> Deserialize<TValue>(byte[] data);
    }
}
