using Newtonsoft.Json;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Implementation
{
    public class GuidJsonConverter : JsonConverter
    {
        public override bool CanRead => false;

        public override bool CanConvert(Type objectType)
        {
            return objectType.IsAssignableFrom(typeof(Guid));
        }

        public override void WriteJson(
            JsonWriter writer,
            object value,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            writer.WriteValue(((Guid)value).ToString("N"));
        }

        public override object ReadJson(
            JsonReader reader,
            Type objectType,
            object existingValue,
            Newtonsoft.Json.JsonSerializer serializer)
        {
            // We won't hit this because CanRead is false.

            throw new NotImplementedException();
        }
    }
}
