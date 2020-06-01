using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization
{
    public abstract class DataSerializer<TDataValue> : ISerializer
    {
        public Task<TValue> Deserialize<TValue>(byte[] data) => Task.FromResult((TValue)(object)Deserialize(data));

        public Task<byte[]> Serialize<TValue>(TValue value) => Task.FromResult(Serialize((TDataValue)(object)value));

        protected abstract TDataValue Deserialize(byte[] data);

        protected abstract byte[] Serialize(TDataValue value);
    }
}
