using Halforbit.DataStores.FileStores.Compression.GZip.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Model;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.IO;
using Xunit;

namespace Halforbit.DataStores.FileStores.LocalStorage.Tests
{
    public class LocalFileStoreJsonTests : UniversalIntegrationTest
    {
        public void TestDataActionModifier()
        {

        }

        [Fact, Trait("Type", "Integration"), Trait("Type", "RunOnBuild")]
        public void TestLocalFileStore_Json()
        {
            InitializeLocalStorage();

            var rootPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "test-data-store");

            //if (Directory.Exists(rootPath)) Directory.Delete(rootPath, true);

            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new LocalFileStore(rootPath: rootPath),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        [Fact, Trait("Type", "Integration"), Trait("Type", "RunOnBuild")]
        public void TestLocalFileStore_GZip_Json()
        {
            var rootPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "test-data-store");

            //if (Directory.Exists(rootPath)) Directory.Delete(rootPath, true);

            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new LocalFileStore(rootPath: rootPath),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                compressor: new GZipCompressor(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json.gzip");

            UniversalIntegrationTest.TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        public class TestValue
        {
            public TestValue(
                Guid accountId = default(Guid),
                string message = default(string))
            {
                AccountId = accountId.OrNewGuidIfDefault();

                Message = message;
            }

            public Guid AccountId { get; }

            public string Message { get; }

            public class Key : UniversalIntegrationTest.ITestKey
            {
                public Key(Guid? accountId)
                {
                    AccountId = accountId;
                }

                public Guid? AccountId { get; }
            }
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
}
