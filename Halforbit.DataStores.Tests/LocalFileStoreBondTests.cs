using Bond;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Bond.Implementation;
using Halforbit.DataStores.Tests;
using System;
using System.IO;
using Xunit;

namespace Halforbit.DataStores.FileStores.LocalStorage.Tests
{
    public class LocalFileStoreBondTests : UniversalIntegrationTest
    {
        [Fact, Trait("Type", "Integration")]
        public void TestLocalFileStore_BondSimpleBinary()
        {
            InitializeLocalStorage();

            var rootPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "test-data-store");

            //if (Directory.Exists(rootPath)) Directory.Delete(rootPath, true);

            var testKey = new BondTestValueKey(accountId: Guid.NewGuid());

            var testValueA = new BondTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Hello, world!"
            };

            var testValueB = new BondTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Kthx, world!"
            };

            var dataStore = new FileStoreDataStore<BondTestValueKey, BondTestValue>(
                fileStore: new LocalFileStore(rootPath: rootPath),
                serializer: new BondSimpleBinarySerializer(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".bond.data");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        [Fact, Trait("Type", "Integration")]
        public void TestLocalFileStore_BondSimpleXml()
        {
            InitializeLocalStorage();

            var rootPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "test-data-store");

            //if (Directory.Exists(rootPath)) Directory.Delete(rootPath, true);

            var testKey = new BondTestValueKey(accountId: Guid.NewGuid());

            var testValueA = new BondTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Hello, world!"
            };

            var testValueB = new BondTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Kthx, world!"
            };

            var dataStore = new FileStoreDataStore<BondTestValueKey, BondTestValue>(
                fileStore: new LocalFileStore(rootPath: rootPath),
                serializer: new BondSimpleXmlSerializer(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".bond.xml");

            UniversalIntegrationTest.TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        static void InitializeLocalStorage()
        {
            var path = Path.Combine(
                Path.GetTempPath(),
                Guid.NewGuid().ToString("N"));

            Directory.CreateDirectory(path);

            Directory.SetCurrentDirectory(path);
        }
    }

    [Schema]
    public class BondTestValue
    {
        [Id(0), Type(typeof(Bond.Tag.wstring))]
        public Guid AccountId { get; set; }

        [Id(1)]
        public string Message { get; set; }
    }

    public class BondTestValueKey : UniversalIntegrationTest.ITestKey
    {
        public BondTestValueKey(Guid? accountId)
        {
            AccountId = accountId;
        }

        public Guid? AccountId { get; }
    }

    public static class BondTypeAliasConverter
    {
        #region Conversion routines for the guid_str alias

        public static Guid Convert(string value, Guid unused)
        {
            return new Guid(value);
        }

        public static string Convert(Guid value, string unused)
        {
            return value.ToString();
        }

        #endregion

        #region Conversion routines for the guid_bin alias

        public static Guid Convert(ArraySegment<byte> value, Guid unused)
        {
            if (value.Count != 16)
            {
                throw new InvalidDataException("value must be of length 16");
            }

            byte[] array = value.Array;
            int offset = value.Offset;

            int a =
                  ((int)array[offset + 3] << 24)
                | ((int)array[offset + 2] << 16)
                | ((int)array[offset + 1] << 8)
                | array[offset];
            short b = (short)(((int)array[offset + 5] << 8) | array[offset + 4]);
            short c = (short)(((int)array[offset + 7] << 8) | array[offset + 6]);

            return new Guid(a, b, c,
                array[offset + 8],
                array[offset + 9],
                array[offset + 10],
                array[offset + 11],
                array[offset + 12],
                array[offset + 13],
                array[offset + 14],
                array[offset + 15]);
        }

        public static ArraySegment<byte> Convert(Guid value, ArraySegment<byte> unused)
        {
            return new ArraySegment<byte>(value.ToByteArray());
        }

        #endregion
    }
}
