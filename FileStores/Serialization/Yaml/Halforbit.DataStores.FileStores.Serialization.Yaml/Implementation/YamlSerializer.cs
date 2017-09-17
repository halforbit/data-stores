using Halforbit.DataStores.FileStores.Interface;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;

namespace Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation
{
    public class YamlSerializer : ISerializer
    {
        readonly Serializer _serializer = new Serializer();

        readonly Deserializer _deserializer = new Deserializer();

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            return Task.FromResult(_deserializer.Deserialize<TValue>(
                Encoding.UTF8.GetString(data, 0, data.Length)));
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            return Task.FromResult(Encoding.UTF8.GetBytes(_serializer.Serialize(value)));
        }
    }
}
