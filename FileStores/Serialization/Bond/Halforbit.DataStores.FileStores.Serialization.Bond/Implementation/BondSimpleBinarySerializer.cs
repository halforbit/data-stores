using Bond.IO.Safe;
using Bond.Protocols;
using Halforbit.DataStores.FileStores.Interface;
using System.Linq;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization.Bond.Implementation
{
    public class BondSimpleBinarySerializer : ISerializer
    {
        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            var input = new InputBuffer(data);

            var reader = new SimpleBinaryReader<InputBuffer>(input);

            return Task.FromResult(global::Bond.Deserialize<TValue>.From(reader));
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            var output = new OutputBuffer();

            var writer = new SimpleBinaryWriter<OutputBuffer>(output);

            global::Bond.Serialize.To(writer, value);

            var data = output.Data;

            return Task.FromResult(data.Array
                .Skip(data.Offset)
                .Take(data.Count)
                .ToArray());
        }
    }
}
