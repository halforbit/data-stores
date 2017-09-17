using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation;
using Halforbit.DataStores.Tests;
using System;
using System.IO;
using Xunit;

namespace Halforbit.DataStores.FileStores.LocalStorage.Tests
{
    public class LocalFileStoreYamlTests : UniversalIntegrationTest
    {
        [Fact, Trait("Type", "Integration")]
        public void TestLocalFileStore_Yaml()
        {
            InitializeLocalStorage();

            var rootPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "test-file-store");

            //if (Directory.Exists(rootPath)) Directory.Delete(rootPath, true);

            var testKey = new YamlTestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new YamlTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Hello, world!"
            };

            var testValueB = new YamlTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Kthx, world!"
            };

            var dataStore = new FileStoreDataStore<YamlTestValue.Key, YamlTestValue>(
                fileStore: new LocalFileStore(rootPath: rootPath),
                serializer: new YamlSerializer(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".yaml");

            TestDataStore(
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

        public class YamlTestValue
        {
            public Guid AccountId { get; set; }

            public string Message { get; set; }

            public class Key : UniversalIntegrationTest.ITestKey
            {
                public Key(Guid? accountId)
                {
                    AccountId = accountId;
                }

                public Guid? AccountId { get; }
            }
        }
    }
}
