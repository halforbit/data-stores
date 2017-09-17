using Halforbit.DataStores.FileStores.Interface;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization.ByteSerialization.Implementation
{
    class ByteSerializer : ISerializer
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
            if (default(TValue) is byte[])
            {
                return Task.FromResult((TValue)(object)data);
            }
            else if (default(TValue) is string)
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
