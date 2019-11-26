using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Serialization.Json.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Implementation
{
    public class JsonSerializer : ISerializer
    {
        const int _byteOrderMarkLength = 3;

        static byte[] _byteOrderMark = new byte[] { 0xEF, 0xBB, 0xBF };

        readonly Newtonsoft.Json.JsonSerializer _jsonSerializer;

        readonly Encoding _encoding;

        public JsonSerializer(
            string options)
        {
            if (!Enum.TryParse<JsonOptions>(options, out var o))
            {
                throw new ArgumentException($"Could not parse JSON options '{options}'.");
            }

            _jsonSerializer = new Newtonsoft.Json.JsonSerializer();

            _jsonSerializer.Converters.Add(new BigIntegerJsonConverter());

            if (o.HasFlag(JsonOptions.CamelCaseEnumValues))
            {
                _jsonSerializer.Converters.Add(new StringEnumConverter { CamelCaseText = true });
            }

            if (o.HasFlag(JsonOptions.CamelCasePropertyNames))
            {
                _jsonSerializer.ContractResolver =
                    new DefaultContractResolver
                    {
                        NamingStrategy = new CamelCaseNamingStrategy
                        {
                            ProcessDictionaryKeys = false
                        }
                    };
            }

            if (o.HasFlag(JsonOptions.RemoveDefaultValues))
            {
                _jsonSerializer.DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate;
            }

            if (o.HasFlag(JsonOptions.OmitGuidDashes))
            {
                _jsonSerializer.Converters.Add(new GuidJsonConverter());
            }

            if (o.HasFlag(JsonOptions.Indented))
            {
                _jsonSerializer.Formatting = Formatting.Indented;
            }

            _encoding = new UTF8Encoding(false);
        }

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            var hasBom = HasByteOrderMark(data);

            var text = _encoding.GetString(
                data, 
                hasBom ? _byteOrderMarkLength : 0, 
                hasBom ? data.Length - _byteOrderMarkLength : data.Length);

            try
            {
                using (var reader = new JsonTextReader(new StringReader(text)))
                {
                    return Task.FromResult((TValue)_jsonSerializer.Deserialize(reader, typeof(TValue)));
                }
            }
            catch
            {
                return Task.FromResult(default(TValue));
            }
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            var sb = new StringBuilder(256);

            var sw = new StringWriter(sb, CultureInfo.InvariantCulture);

            using (var jsonWriter = new JsonTextWriter(sw))
            {
                jsonWriter.Formatting = _jsonSerializer.Formatting;

                _jsonSerializer.Serialize(jsonWriter, value, typeof(TValue));
            }

            return Task.FromResult(_encoding.GetBytes(sw.ToString()));
        }

        bool HasByteOrderMark(byte[] data)
        {
            if (data?.Length < _byteOrderMarkLength)
            {
                return false;
            }
            
            for (int i = 0; i < _byteOrderMarkLength; i++)
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
