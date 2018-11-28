using Halforbit.DataStores.FileStores.Interface;
using Newtonsoft.Json;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Implementation
{
    public class JsonSerializer : ISerializer
    {
        static byte[] _byteOrderMark = new byte[] { 0xEF, 0xBB, 0xBF };
        const int _bomLength = 3;

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            var hasBom = HasByteOrderMark(data);

            var text = Encoding.UTF8.GetString(
                data, 
                hasBom ? _bomLength : 0, 
                hasBom ? data.Length - _bomLength : data.Length);

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

        bool HasByteOrderMark(byte[] data)
        {
            if (data?.Length < _bomLength)
            {
                return false;
            }

            for (int i = 0; i < _bomLength; i++)
            {
                if (data[i] != _byteOrderMark[i])
                {
                    return false;
                }
            }

            return true;
        }
    }
}
