using Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using static Halforbit.DataStores.FileStores.Serialization.Yaml.Model.YamlOptions;

namespace Halforbit.DataStores.FileStores.Serialization.Yaml.Tests
{
    public class YamlSerializerTests
    {
        readonly Encoding _encoding = new UTF8Encoding(false);

        // SERIALIZE - OPTIONS ////////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = _encoding.GetBytes(@"
itemId: 7e900426dbc246bcbe100d503644b830
itemName: alfa
subItems:
- bravo
- charlie
options: all
createTime: 2019-01-02T03:04:05.0060007Z
".TrimStart());

            var actual = await yamlSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Serialize(null as Item);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIntegerEnumValues_Success()
        {
            var yamlSerializer = new YamlSerializer($"{CamelCasePropertyNames | RemoveDefaultValues | OmitGuidDashes}");

            var expected = _encoding.GetBytes(@"
itemId: 7e900426dbc246bcbe100d503644b830
itemName: alfa
subItems:
- bravo
- charlie
options: 3
createTime: 2019-01-02T03:04:05.0060007Z
".TrimStart());

            var actual = await yamlSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializePascalCasePropertyNames_Success()
        {
            var yamlSerializer = new YamlSerializer($"{CamelCaseEnumValues | RemoveDefaultValues | OmitGuidDashes }");

            var expected = _encoding.GetBytes(@"
ItemId: 7e900426dbc246bcbe100d503644b830
ItemName: alfa
SubItems:
- bravo
- charlie
Options: all
CreateTime: 2019-01-02T03:04:05.0060007Z
".TrimStart());

            var actual = await yamlSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIncludeDefaultValues_Success()
        {
            var yamlSerializer = new YamlSerializer($"{CamelCaseEnumValues | CamelCasePropertyNames | OmitGuidDashes}");

            var expected = _encoding.GetBytes(@"
itemId: 7e900426dbc246bcbe100d503644b830
itemName: alfa
defaultValue: 
subItems:
- bravo
- charlie
options: all
createTime: 2019-01-02T03:04:05.0060007Z
".TrimStart());

            var actual = await yamlSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeWithGuidDashes_Success()
        {
            var yamlSerializer = new YamlSerializer($"{CamelCaseEnumValues | CamelCasePropertyNames | RemoveDefaultValues}");

            var expected = _encoding.GetBytes(@"
itemId: 7e900426-dbc2-46bc-be10-0d503644b830
itemName: alfa
subItems:
- bravo
- charlie
options: all
createTime: 2019-01-02T03:04:05.0060007Z
".TrimStart());

            var actual = await yamlSerializer.Serialize(TestItem);

            Assert.Equal(expected, actual);
        }

        // SERIALIZE - SIMPLE TYPES /////////////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeStringDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = _encoding.GetBytes("hello, world\r\n");

            var actual = await yamlSerializer.Serialize("hello, world");

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeDateTimeDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var dateTime = new DateTime(2019, 01, 02, 03, 04, 05, 006, DateTimeKind.Utc).AddTicks(0007);

            var expected = _encoding.GetBytes("2019-01-02T03:04:05.0060007Z\r\n");

            var actual = await yamlSerializer.Serialize(dateTime);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullDateTimeDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Serialize(null as DateTime?);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullableGuidDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var guid = new Guid("7e900426dbc246bcbe100d503644b830") as Guid?;

            var expected = _encoding.GetBytes("7e900426dbc246bcbe100d503644b830\r\n");

            var actual = await yamlSerializer.Serialize(guid);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeNullGuidDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Serialize(null as Guid?);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeGuidWithGuidDashes_Success()
        {
            var yamlSerializer = new YamlSerializer($"{CamelCaseEnumValues | CamelCasePropertyNames | RemoveDefaultValues}");

            var guid = new Guid("7e900426dbc246bcbe100d503644b830");

            var expected = _encoding.GetBytes("7e900426-dbc2-46bc-be10-0d503644b830\r\n");

            var actual = await yamlSerializer.Serialize(guid);

            Assert.Equal(expected, actual);
        }

        // SERIALIZE - ARRAY TYPES ////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIReadOnlyListOfClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = _encoding.GetBytes(@"
- itemId: 7e900426dbc246bcbe100d503644b830
  itemName: alfa
  subItems:
  - bravo
  - charlie
  options: all
  createTime: 2019-01-02T03:04:05.0060007Z
- itemId: 7e900426dbc246bcbe100d503644b830
  itemName: alfa
  subItems:
  - bravo
  - charlie
  options: all
  createTime: 2019-01-02T03:04:05.0060007Z
".TrimStart());

            var actual = await yamlSerializer.Serialize(new List<Item> { TestItem, TestItem } as IReadOnlyList<Item>);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeIReadOnlyListOfStringDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = _encoding.GetBytes(@"
- alfa
- bravo
".TrimStart());

            var actual = await yamlSerializer.Serialize(new List<string> { "alfa", "bravo" } as IReadOnlyList<string>);

            Assert.Equal(expected, actual);
        }

        // SERIALIZE - JTOKEN TYPES ///////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task SerializeJObjectDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var jObject = JObject.FromObject(new
            {
                Alfa = "Bravo",

                Charlie = 123
            });

            var expected = _encoding.GetBytes(@"
Alfa: Bravo
Charlie: 123
".TrimStart());

            var actual = await yamlSerializer.Serialize(jObject);

            Assert.Equal(expected, actual);
        }

        // DESERIALIZE ////////////////////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = await yamlSerializer.Serialize(TestItem);

            var actual = await yamlSerializer.Deserialize<Item>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(TestItem), 
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Deserialize<Item>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeClassWithByteOrderMark_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = new byte[] { 0xEF, 0xBB, 0xBF }
                .Concat(await yamlSerializer.Serialize(TestItem))
                .ToArray();

            var actual = await yamlSerializer.Deserialize<Item>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(TestItem),
                JsonConvert.SerializeObject(actual));
        }

        // DESERIALIZE - SIMPLE TYPES /////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeStringDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = "hello, world";

            var serialized = _encoding.GetBytes("hello, world\r\n");

            var actual = await yamlSerializer.Deserialize<string>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeDateTimeDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = new DateTime(2019, 01, 02, 03, 04, 05, 006, DateTimeKind.Utc).AddTicks(0007);

            var serialized = await yamlSerializer.Serialize(expected);

            var actual = await yamlSerializer.Deserialize<DateTime>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeGuidDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = new Guid("7e900426dbc246bcbe100d503644b830");
            
            var serialized = await yamlSerializer.Serialize(expected);

            var actual = await yamlSerializer.Deserialize<Guid>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullStringDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Deserialize<string>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullDateTimeDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Deserialize<DateTime?>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullGuidDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = await yamlSerializer.Serialize(null as Guid?);

            var actual = await yamlSerializer.Deserialize<Guid?>(serialized);

            Assert.Null(actual);
        }

        // DESERIALIZE - ARRAY TYPES //////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeIReadOnlyListOfClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = new List<Item> { TestItem, TestItem } as IReadOnlyList<Item>;

            var serialized = await yamlSerializer.Serialize(expected);

            var actual = await yamlSerializer.Deserialize<IReadOnlyList<Item>>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(expected), 
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeArrayOfClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = new[] { TestItem, TestItem };

            var serialized = await yamlSerializer.Serialize(expected);

            var actual = await yamlSerializer.Deserialize<Item[]>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(expected), 
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeIReadOnlyListOfStringDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = new List<string> { "alfa", "bravo" } as IReadOnlyList<string>;

            var serialized = await yamlSerializer.Serialize(expected);

            var actual = await yamlSerializer.Deserialize<IReadOnlyList<string>>(serialized);

            Assert.Equal(expected, actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullIReadOnlyListOfClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Deserialize<IReadOnlyList<Item>>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullArrayOfClassDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Deserialize<Item[]>(serialized);

            Assert.Null(actual);
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullIReadOnlyListOfStringDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Deserialize<IReadOnlyList<string>>(serialized);

            Assert.Null(actual);
        }

        // DESERIALIZE - JTOKEN TYPES /////////////////////////////////////////

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeJObjectDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var expected = JObject.FromObject(new
            {
                alfa = "Bravo",

                charlie = "Delta"
            });

            var serialized = await yamlSerializer.Serialize(expected);

            var actual = await yamlSerializer.Deserialize<JObject>(serialized);

            Assert.Equal(
                JsonConvert.SerializeObject(expected),
                JsonConvert.SerializeObject(actual));
        }

        [Fact, Trait("Type", "RunOnBuild")]
        public async Task DeserializeNullJObjectDefaultOptions_Success()
        {
            var yamlSerializer = new YamlSerializer($"{Default}");

            var serialized = _encoding.GetBytes("--- \r\n");

            var actual = await yamlSerializer.Deserialize<JObject>(serialized);

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
