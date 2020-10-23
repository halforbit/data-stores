using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Halforbit.DataStores.FileStores.Serialization.Delimited.Tests
{
    [Trait("Type", "Unit"), Trait("Type", "RunOnBuild")]
    public class DelimitedSerializerTests
    {
        [Fact]
        public async Task ListOfDictionary_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<IReadOnlyDictionary<string, string>>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task EnumerableOfDictionary_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IEnumerable<IReadOnlyDictionary<string, string>>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task ListOfDataClass_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<TestDataClass>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task EnumerableOfDataClass_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IEnumerable<TestDataClass>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task ListOfJObject_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<JObject>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task EnumerableOfJObject_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IEnumerable<JObject>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task RecordSet_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<RecordSet>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task ListOfRecord_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<Record>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact]
        public async Task EnumerableOfRecord_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IEnumerable<Record>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        static bool ByteArraysEqual(byte[] a, byte[] b)
        {
            if (a.Length != b.Length) return false;

            for (var i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i]) return false;
            }

            return true;
        }

        class TestDataClass
        {
            public TestDataClass(
                string alfa,
                double bravo,
                bool charlie)
            {
                Alfa = alfa;
                Bravo = bravo;
                Charlie = charlie;
            }

            public string Alfa { get; }
            public double Bravo { get; }
            public bool Charlie { get; }
        }

        public class Record : IReadOnlyDictionary<string, string>
        {
            readonly IReadOnlyList<KeyValuePair<string, string>> _keyValues;

            [JsonConstructor]
            public Record(IEnumerable<KeyValuePair<string, string>> keyValues)
            {
                _keyValues = keyValues.ToList();
            }

            public Record(params (string Key, string Value)[] data)
            {
                _keyValues = data
                    .Select(d => new KeyValuePair<string, string>(d.Key, d.Value))
                    .ToList();
            }

            public string this[string key] => _keyValues.Single(kv => kv.Key == key).Value;

            public IEnumerable<string> Keys => _keyValues.Select(kv => kv.Key);

            public IEnumerable<string> Values => _keyValues.Select(kv => kv.Value);

            public int Count => _keyValues.Count;

            public bool ContainsKey(string key) => _keyValues.Any(kv => kv.Key == key);

            public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _keyValues.GetEnumerator();

            public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
            {
                var keyValue = _keyValues.SingleOrDefault(kv => kv.Key == key);

                if (!string.IsNullOrWhiteSpace(keyValue.Key))
                {
                    value = keyValue.Value;

                    return true;
                }
                else
                {
                    value = default;

                    return false;
                }
            }

            IEnumerator IEnumerable.GetEnumerator() => _keyValues.GetEnumerator();
        }

        public class RecordSet : IReadOnlyList<Record>
        {
            readonly IReadOnlyList<Record> _records;

            public RecordSet(IEnumerable<Record> records)
            {
                _records = records.ToList();
            }

            public Record this[int index] => _records[index];

            public int Count => _records.Count;

            public IEnumerator<Record> GetEnumerator() => _records.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => _records.GetEnumerator();

            public static implicit operator RecordSet(Record[] records) => new RecordSet(records);

            public static implicit operator RecordSet(List<Record> records) => new RecordSet(records);

            public static RecordSet Empty { get; } = new RecordSet(new Record[0]);
        }

        readonly byte[] _tsvWithHeader = new UTF8Encoding(false).GetBytes(
            "Alfa\tBravo\tCharlie\r\n" +
            "Delta\t1.23\tTrue\r\n" +
            "Golf\t2.34\tFalse\r\n");
    }
}
