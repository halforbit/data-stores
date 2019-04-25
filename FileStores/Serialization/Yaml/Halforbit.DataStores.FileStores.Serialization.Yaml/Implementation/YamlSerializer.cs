using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Newtonsoft.Json.Linq;
using System.Dynamic;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using ISerializer = Halforbit.DataStores.FileStores.Interface.ISerializer;

namespace Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation
{
    public class YamlSerializer : ISerializer
    {
        readonly JsonSerializer _jsonSerializer = new JsonSerializer();

        readonly Serializer _serializer = new Serializer();

        readonly Deserializer _deserializer = new Deserializer();

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            return Task.FromResult(JToken.FromObject(_deserializer.Deserialize<ExpandoObject>(
                Encoding.UTF8.GetString(data, 0, data.Length))).ToObject<TValue>());
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            return Task.FromResult(Encoding.UTF8.GetBytes(_serializer.Serialize(
                JToken.FromObject(value).ToObject<ExpandoObject>())));
        }
    }
}
