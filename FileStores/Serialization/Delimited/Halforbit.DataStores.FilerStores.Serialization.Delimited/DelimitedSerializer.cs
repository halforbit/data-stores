using CsvHelper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public class DelimitedSerializer : ISerializer
    {
        readonly string _delimiter;
        readonly bool _hasHeader;

        public DelimitedSerializer(
            string delimiter = Delimiter.Tab,
            bool hasHeader = true)
        {
            _delimiter = delimiter;
            _hasHeader = hasHeader;
        }

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            var valueType = typeof(TValue);

            if (valueType.IsGenericType && valueType.GenericTypeArguments.Count() == 1)
            {
                var recordType = valueType.GenericTypeArguments.Single();

                var genericType = valueType.GetGenericTypeDefinition();

                using (var memoryStream = new MemoryStream(data))
                using (var streamReader = new StreamReader(memoryStream))
                using (var reader = new CsvReader(streamReader, CultureInfo.InvariantCulture))
                {
                    reader.Configuration.HasHeaderRecord = _hasHeader;

                    reader.Configuration.Delimiter = _delimiter;

                    reader.Configuration.PrepareHeaderForMatch = (s, i) => s.ToLower();

                    var castMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast)).MakeGenericMethod(recordType);

                    var castResult = castMethod.Invoke(
                        null, 
                        new[] 
                        { 
                            reader.GetRecords(recordType).ToList() 
                        });

                    if (genericType.Equals(typeof(IEnumerable<>)))
                    {
                        return Task.FromResult((TValue)castResult);
                    }
                    else if (genericType.Equals(typeof(IReadOnlyList<>)))
                    {
                        var toListMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.ToList)).MakeGenericMethod(recordType);

                        var toListResult = toListMethod.Invoke(null, new[] { castResult });

                        return Task.FromResult((TValue)toListResult);
                    }
                }
            }

            throw new ArgumentException($"Delimited `TValue` type is `{valueType.Name}`, but must be `IReadOnlyList<>` or `IEnumerable<>`");
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            var valueType = typeof(TValue);

            if (typeof(IEnumerable).IsAssignableFrom(valueType))
            {
                using (var memoryStream = new MemoryStream())
                using (var streamWriter = new StreamWriter(memoryStream))
                using (var writer = new CsvWriter(streamWriter, CultureInfo.InvariantCulture))
                {
                    writer.Configuration.HasHeaderRecord = _hasHeader;

                    writer.Configuration.Delimiter = _delimiter;

                    writer.WriteRecords((IEnumerable)value);

                    writer.Flush();

                    memoryStream.Flush();

                    return Task.FromResult(memoryStream.ToArray());
                }
            }

            throw new ArgumentException($"Delimited `TValue` type is `{valueType.Name}`, but must be `IEnumerable`");
        }
    }
}