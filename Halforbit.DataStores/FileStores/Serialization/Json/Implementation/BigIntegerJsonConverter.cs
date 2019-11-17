using Newtonsoft.Json;
using System;
using System.Numerics;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Implementation
{
    public class BigIntegerJsonConverter : JsonConverter
    {
        public override bool CanRead => true;

        public override bool CanConvert(Type objectType)
        {
            return objectType.IsAssignableFrom(typeof(BigInteger));
        }

        public override void WriteJson(
            JsonWriter writer,
            object value,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            writer.WriteValue(((BigInteger)value).ToString());
        }

        public override object ReadJson(
            JsonReader reader,
            Type objectType,
            object existingValue,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            return BigInteger.Parse((string)reader.Value);
        }
    }
}
