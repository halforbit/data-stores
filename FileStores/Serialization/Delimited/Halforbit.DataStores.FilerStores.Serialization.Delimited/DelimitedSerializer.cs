using CsvHelper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Linq;
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

            var enumerableInterface = valueType.Name == "IEnumerable`1" ? 
                valueType : 
                valueType.GetInterface("IEnumerable`1");

            if (enumerableInterface != null)
            {
                var recordType =
                    enumerableInterface.GenericTypeArguments.Single();

                using (var memoryStream = new MemoryStream(data))
                using (var streamReader = new StreamReader(memoryStream))
                using (var reader = new CsvReader(streamReader, CultureInfo.InvariantCulture))
                {
                    reader.Configuration.HasHeaderRecord = _hasHeader;

                    reader.Configuration.Delimiter = _delimiter;
                    
                    var castMethod = typeof(Enumerable)
                        .GetMethod(nameof(Enumerable.Cast))
                        .MakeGenericMethod(recordType);

                    var castResult = castMethod.Invoke(
                        null,
                        new[]
                        {
                            reader
                                .GetRecords(typeof(object))
                                .Cast<ExpandoObject>()
                                .Select(e => JObject.FromObject(e).ToObject(recordType))
                        });

                    var readOnlyListInterface = valueType.GetInterface("IReadOnlyList`1");

                    var valueConstructor = valueType.GetConstructor(new[] 
                    { 
                        typeof(IEnumerable<>).MakeGenericType(recordType) 
                    });

                    if (valueConstructor != null)
                    {
                        return Task.FromResult((TValue)valueConstructor.Invoke(new[] { castResult }));
                    }
                    else if (valueType.IsGenericType)
                    {
                        var valueGenericTypeDefinition = valueType.GetGenericTypeDefinition();

                        if (valueGenericTypeDefinition.Equals(typeof(IReadOnlyList<>)) ||
                            valueGenericTypeDefinition.Equals(typeof(IEnumerable<>)))
                        {
                            var toListMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.ToList)).MakeGenericMethod(recordType);

                            var toListResult = toListMethod.Invoke(null, new[] { castResult });

                            return Task.FromResult((TValue)toListResult);
                        }
                        //else if (valueType.GetGenericTypeDefinition().Equals(typeof(IEnumerable<>)))
                        //{
                        //    return Task.FromResult((TValue)castResult);
                        //}
                    }
                }
            }

            throw new ArgumentException($"Delimited `TValue` type is `{valueType.Name}`, but must be `IReadOnlyList<>`");
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            var valueType = typeof(TValue);

            if (typeof(IEnumerable).IsAssignableFrom(valueType))
            {
                var castMethod = typeof(Enumerable)
                    .GetMethod(nameof(Enumerable.Cast))
                    .MakeGenericMethod(typeof(object));

                var castResult = castMethod.Invoke(null, new object[] { value }) as IEnumerable<object>; 

                using (var memoryStream = new MemoryStream())
                using (var streamWriter = new StreamWriter(memoryStream))
                using (var writer = new CsvWriter(streamWriter, CultureInfo.InvariantCulture))
                {
                    writer.Configuration.HasHeaderRecord = _hasHeader;

                    writer.Configuration.Delimiter = _delimiter;

                    writer.WriteRecords(castResult.Select(r => JObject.FromObject(r).ToObject<ExpandoObject>()).Cast<object>());

                    writer.Flush();

                    streamWriter.Flush();

                    memoryStream.Flush();

                    return Task.FromResult(memoryStream.ToArray());
                }
            }

            throw new ArgumentException($"Delimited `TValue` type is `{valueType.Name}`, but must be `IReadOnlyList<>`");
        }
    }
}