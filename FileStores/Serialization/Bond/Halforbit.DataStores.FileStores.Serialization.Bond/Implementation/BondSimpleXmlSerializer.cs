using Bond.Protocols;
using System.IO;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization.Bond.Implementation
{
    public class BondSimpleXmlSerializer : ISerializer
    {
        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            var sourceStream = new MemoryStream(data);

            var reader = new SimpleXmlReader(sourceStream);

            return Task.FromResult(global::Bond.Deserialize<TValue>.From(reader));
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            var destStream = new MemoryStream();

            var writer = new SimpleXmlWriter(destStream);

            global::Bond.Serialize.To(writer, value);

            writer.Flush();

            return Task.FromResult(destStream.ToArray());
        }
    }
}
