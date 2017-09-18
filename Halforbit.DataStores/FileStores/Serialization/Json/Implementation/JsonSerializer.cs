using Halforbit.DataStores.FileStores.Interface;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Implementation
{
    public class JsonSerializer : ISerializer
    {
        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            string text = Encoding.UTF8.GetString(data, 0, data.Length);

            try
            {
                return Task.FromResult(JsonConvert.DeserializeObject<TValue>(text));
            }
            catch //(Exception ex)
            {
                return Task.FromResult(default(TValue));
            }            
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            return Task.FromResult(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value)));
        }
    }
}
