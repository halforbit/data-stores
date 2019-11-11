using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using System.Dynamic;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using ISerializer = Halforbit.DataStores.FileStores.Interface.ISerializer;

namespace Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation
{
    public class YamlSerializer : ISerializer
    {
        readonly JsonSerializer _jsonSerializer;

        readonly Serializer _serializer = new Serializer();

        readonly Deserializer _deserializer = new Deserializer();

        public YamlSerializer()
        {
             _jsonSerializer = new JsonSerializer
             {
                 ContractResolver = new CamelCasePropertyNamesContractResolver(), 
                 
                 DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate
             };

            _jsonSerializer.Converters.Add(new StringEnumConverter { CamelCaseText = true });
        }

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            return Task.FromResult(JToken
                .FromObject(
                    _deserializer.Deserialize<ExpandoObject>(
                        Encoding.UTF8.GetString(data, 0, data.Length)),
                    _jsonSerializer)
                .ToObject<TValue>());
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            return Task.FromResult(Encoding.UTF8.GetBytes(_serializer.Serialize(
                JToken.FromObject(value, _jsonSerializer).ToObject<ExpandoObject>())));
        }
    }
}
