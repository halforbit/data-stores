using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using static Halforbit.DataStores.FileStores.Serialization.Json.Model.JsonOptions;
using HalforbitJsonSerializer = Halforbit.DataStores.FileStores.Serialization.Json.Implementation.JsonSerializer;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Tests
{
    public class HalforbitJsonSerializerTests
    {
        readonly Encoding _encoding = new UTF8Encoding(false);

        // SERIALIZE - OPTIONS ////////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = _encoding.GetBytes(@"
{
  ""itemId"": ""7e900426dbc246bcbe100d503644b830"",
  ""itemName"": ""alfa"",
  ""subItems"": [
    ""bravo"",
    ""charlie""
  ],
  ""options"": ""all"",
  ""createTime"": ""2019-01-02T03:04:05.0060007Z""
}
".Trim());

            var actual = await jsonSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Serialize(null as Item);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIntegerEnumValues_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{CamelCasePropertyNames | RemoveDefaultValues | OmitGuidDashes | Indented}");

            var expected = _encoding.GetBytes(@"
{
  ""itemId"": ""7e900426dbc246bcbe100d503644b830"",
  ""itemName"": ""alfa"",
  ""subItems"": [
    ""bravo"",
    ""charlie""
  ],
  ""options"": 3,
  ""createTime"": ""2019-01-02T03:04:05.0060007Z""
}
".Trim());

            var actual = await jsonSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializePascalCasePropertyNames_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{CamelCaseEnumValues | RemoveDefaultValues | OmitGuidDashes | Indented }");

            var expected = _encoding.GetBytes(@"
{
  ""ItemId"": ""7e900426dbc246bcbe100d503644b830"",
  ""ItemName"": ""alfa"",
  ""SubItems"": [
    ""bravo"",
    ""charlie""
  ],
  ""Options"": ""all"",
  ""CreateTime"": ""2019-01-02T03:04:05.0060007Z""
}
".Trim());

            var actual = await jsonSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIncludeDefaultValues_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{CamelCaseEnumValues | CamelCasePropertyNames | OmitGuidDashes | Indented}");

            var expected = _encoding.GetBytes(@"
{
  ""itemId"": ""7e900426dbc246bcbe100d503644b830"",
  ""itemName"": ""alfa"",
  ""defaultValue"": null,
  ""subItems"": [
    ""bravo"",
    ""charlie""
  ],
  ""options"": ""all"",
  ""createTime"": ""2019-01-02T03:04:05.0060007Z""
}".Trim());

            var actual = await jsonSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeWithGuidDashes_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{CamelCaseEnumValues | CamelCasePropertyNames | RemoveDefaultValues | Indented}");

            var expected = _encoding.GetBytes(@"
{
  ""itemId"": ""7e900426-dbc2-46bc-be10-0d503644b830"",
  ""itemName"": ""alfa"",
  ""subItems"": [
    ""bravo"",
    ""charlie""
  ],
  ""options"": ""all"",
  ""createTime"": ""2019-01-02T03:04:05.0060007Z""
}
".Trim());

            var actual = await jsonSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeClassNonIndented_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{CamelCaseEnumValues | CamelCasePropertyNames | RemoveDefaultValues | OmitGuidDashes}");

            var expected = _encoding.GetBytes(@"{""itemId"":""7e900426dbc246bcbe100d503644b830"",""itemName"":""alfa"",""subItems"":[""bravo"",""charlie""],""options"":""all"",""createTime"":""2019-01-02T03:04:05.0060007Z""}");

            var actual = await jsonSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        // SERIALIZE - SIMPLE TYPES /////////////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeStringDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = _encoding.GetBytes("\"hello, world\"");

            var actual = await jsonSerializer.Serialize("hello, world");

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeDateTimeDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var dateTime = new DateTime(2019, 01, 02, 03, 04, 05, 006, DateTimeKind.Utc).AddTicks(0007);

            var expected = _encoding.GetBytes("\"2019-01-02T03:04:05.0060007Z\"");

            var actual = await jsonSerializer.Serialize(dateTime);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullDateTimeDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Serialize(null as DateTime?);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullableGuidDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var guid = new Guid("7e900426dbc246bcbe100d503644b830") as Guid?;

            var expected = _encoding.GetBytes("\"7e900426dbc246bcbe100d503644b830\"");

            var actual = await jsonSerializer.Serialize(guid);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullGuidDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Serialize(null as Guid?);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeGuidWithGuidDashes_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{CamelCaseEnumValues | CamelCasePropertyNames | RemoveDefaultValues | Indented}");

            var guid = new Guid("7e900426dbc246bcbe100d503644b830");

            var expected = _encoding.GetBytes("\"7e900426-dbc2-46bc-be10-0d503644b830\"");

            var actual = await jsonSerializer.Serialize(guid);

            Assert.Equal(expected, actual);
        }

        // SERIALIZE - ARRAY TYPES ////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIReadOnlyListOfClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = _encoding.GetBytes(@"
[
  {
    ""itemId"": ""7e900426dbc246bcbe100d503644b830"",
    ""itemName"": ""alfa"",
    ""subItems"": [
      ""bravo"",
      ""charlie""
    ],
    ""options"": ""all"",
    ""createTime"": ""2019-01-02T03:04:05.0060007Z""
  },
  {
    ""itemId"": ""7e900426dbc246bcbe100d503644b830"",
    ""itemName"": ""alfa"",
    ""subItems"": [
      ""bravo"",
      ""charlie""
    ],
    ""options"": ""all"",
    ""createTime"": ""2019-01-02T03:04:05.0060007Z""
  }
]
".Trim());

            var actual = await jsonSerializer.Serialize(new List<Item> { TestItem, TestItem } as IReadOnlyList<Item>);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIReadOnlyListOfStringDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = _encoding.GetBytes(@"
[
  ""alfa"",
  ""bravo""
]
".Trim());

            var actual = await jsonSerializer.Serialize(new List<string> { "alfa", "bravo" } as IReadOnlyList<string>);

            Assert.Equal(expected, actual);
        }

        // SERIALIZE - JTOKEN TYPES ///////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeJObjectDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var jObject = JObject.FromObject(new
            {
                Alfa = "Bravo",

                Charlie = 123
            });

            var expected = _encoding.GetBytes(@"
{
  ""Alfa"": ""Bravo"",
  ""Charlie"": 123
}
".Trim());

            var actual = await jsonSerializer.Serialize(jObject);

            Assert.Equal(expected, actual);
        }

        // DESERIALIZE ////////////////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = await jsonSerializer.Serialize(TestItem);

            var actual = await jsonSerializer.Deserialize<Item>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(TestItem),
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Deserialize<Item>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeClassWithByteOrderMark_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = new byte[] { 0xEF, 0xBB, 0xBF }
                .Concat(await jsonSerializer.Serialize(TestItem))
                .ToArray();

            var actual = await jsonSerializer.Deserialize<Item>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(TestItem),
                JsonConvert.SerializeObject(actual));
        }

        // DESERIALIZE - SIMPLE TYPES /////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeStringDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = "hello, world";

            var serialized = _encoding.GetBytes("\"hello, world\"");

            var actual = await jsonSerializer.Deserialize<string>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeDateTimeDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = new DateTime(2019, 01, 02, 03, 04, 05, 006, DateTimeKind.Utc).AddTicks(0007);

            var serialized = await jsonSerializer.Serialize(expected);

            var actual = await jsonSerializer.Deserialize<DateTime>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeGuidDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = new Guid("7e900426dbc246bcbe100d503644b830");

            var serialized = await jsonSerializer.Serialize(expected);

            var actual = await jsonSerializer.Deserialize<Guid>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullStringDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Deserialize<string>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullDateTimeDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Deserialize<DateTime?>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullGuidDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = await jsonSerializer.Serialize(null as Guid?);

            var actual = await jsonSerializer.Deserialize<Guid?>(serialized);

            Assert.Null(actual);
        }

        // DESERIALIZE - ARRAY TYPES //////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeIReadOnlyListOfClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = new List<Item> { TestItem, TestItem } as IReadOnlyList<Item>;

            var serialized = await jsonSerializer.Serialize(expected);

            var actual = await jsonSerializer.Deserialize<IReadOnlyList<Item>>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(expected),
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeArrayOfClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = new[] { TestItem, TestItem };

            var serialized = await jsonSerializer.Serialize(expected);

            var actual = await jsonSerializer.Deserialize<Item[]>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(expected),
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeIReadOnlyListOfStringDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = new List<string> { "alfa", "bravo" } as IReadOnlyList<string>;

            var serialized = await jsonSerializer.Serialize(expected);

            var actual = await jsonSerializer.Deserialize<IReadOnlyList<string>>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullIReadOnlyListOfClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Deserialize<IReadOnlyList<Item>>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullArrayOfClassDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Deserialize<Item[]>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullIReadOnlyListOfStringDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Deserialize<IReadOnlyList<string>>(serialized);

            Assert.Null(actual);
        }

        // DESERIALIZE - JTOKEN TYPES /////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeJObjectDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var expected = JObject.FromObject(new
            {
                alfa = "Bravo",

                charlie = "Delta"
            });

            var serialized = await jsonSerializer.Serialize(expected);

            var actual = await jsonSerializer.Deserialize<JObject>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(expected),
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullJObjectDefaultOptions_Success()
        {
            var jsonSerializer = new HalforbitJsonSerializer($"{Default}");

            var serialized = _encoding.GetBytes("null");

            var actual = await jsonSerializer.Deserialize<JObject>(serialized);

            Assert.Null(actual);
        }

        // TEST HELPERS ///////////////////////////////////////////////////////

        Item TestItem => new Item(
            itemId: new Guid("7e900426dbc246bcbe100d503644b830"),
            itemName: "alfa",
            defaultValue: default,
            subItems: new[]
            {
                "bravo",
                "charlie"
            },
            options: Options.All,
            createTime: new DateTime(2019, 01, 02, 03, 04, 05, 006, DateTimeKind.Utc).AddTicks(0007));

        [Flags]
        enum Options
        {
            None = 0,

            Apples = 1,

            Oranges = 2,

            All = Apples | Oranges
        }

        class Item
        {
            public Item(
                Guid itemId,
                string itemName,
                string defaultValue,
                IReadOnlyList<string> subItems,
                Options options,
                DateTime createTime)
            {
                ItemId = itemId;

                ItemName = itemName;

                DefaultValue = defaultValue;

                SubItems = subItems;

                Options = options;

                CreateTime = createTime;
            }

            public Guid ItemId { get; }

            public string ItemName { get; }

            public string DefaultValue { get; }

            public IReadOnlyList<string> SubItems { get; }

            public Options Options { get; }

            public DateTime CreateTime { get; }
        }
    }
}
