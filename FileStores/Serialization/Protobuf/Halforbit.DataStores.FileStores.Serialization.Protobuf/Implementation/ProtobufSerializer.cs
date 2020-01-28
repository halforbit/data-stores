using Halforbit.DataStores.FileStores.Interface;
using ProtoBuf;
using System.IO;
using System.Threading.Tasks;

namespace Halforbit.DataStores.Serialization.Protobuf.Implementation
{
    public class ProtobufSerializer : ISerializer
    {
        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            var sourceStream = new MemoryStream(data);

            return Task.FromResult(Serializer.Deserialize<TValue>(sourceStream));
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            var destStream = new MemoryStream();

            Serializer.Serialize(destStream, value);

            return Task.FromResult(destStream.ToArray());
        }
    }
}
