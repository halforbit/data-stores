using System;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization.ByteSerialization.Implementation
{
    public class ByteSerializer : ISerializer
    {
        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            if(value is byte[])
            {
                return Task.FromResult(value as byte[]);
            }
            else if(value is string)
            {
                return Task.FromResult(Encoding.UTF8.GetBytes(value as string));
            }
            else
            {
                throw new ArgumentException($"TValue is unhandled type {typeof(TValue).Name}");
            }
        }

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            if (typeof(TValue) == typeof(byte[]))
            {
                return Task.FromResult((TValue)(object)data);
            }
            else if (typeof(TValue) == typeof(string))
            {
                return Task.FromResult((TValue)(object)Encoding.UTF8.GetString(data));
            }
            else
            {
                throw new ArgumentException($"TValue is unhandled type {typeof(TValue).Name}");
            }
        }
    }
}
