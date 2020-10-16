using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Halforbit.DataStores.FileStores.Serialization.Delimited.Tests
{
    public class DelimitedSerializerTests
    {
        [Fact, Trait("Type", "Unit")]
        public async Task ListOfDictionary_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<IReadOnlyDictionary<string, string>>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact, Trait("Type", "Unit")]
        public async Task ListOfDataClass_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<TestRecord>>(_tsvWithHeader);

            var reserialized = await delimitedSerializer.Serialize(records);

            Assert.True(ByteArraysEqual(_tsvWithHeader, reserialized));
        }

        [Fact, Trait("Type", "Unit")]
        public async Task ListOfJObject_RoundTrip_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<JObject>>(_tsvWithHeader);

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

        class TestRecord
        {
            public TestRecord(
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

        readonly byte[] _tsvWithHeader = new UTF8Encoding(false).GetBytes(
            "Alfa\tBravo\tCharlie\r\n" +
            "Delta\t1.23\tTrue\r\n" +
            "Golf\t2.34\tFalse\r\n");
    }
}
