using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation;
using Halforbit.DataStores.Tests;
using System;
using System.IO;
using Xunit;

namespace Halforbit.DataStores.FileStores.LocalStorage.Tests
{
    public class LocalFileStoreByteSerializationTests : UniversalIntegrationTest
    {
        [Fact, Trait("Type", "Integration"), Trait("Type", "RunOnBuild")]
        public void TestLocalFileStore_Yaml()
        {
            InitializeLocalStorage();

            var rootPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "test-file-store");

            //if (Directory.Exists(rootPath)) Directory.Delete(rootPath, true);

            var testKey = new TestKey(accountId: Guid.NewGuid());

            var testValueA = "Hello, world!";

            var testValueB = "Kthx, world!";

            //var dataStore = new FileStoreDataStore<TestKey, string>(
            //    fileStore: new LocalFileStore(rootPath: rootPath),
            //    serializer: new ByteSerializer(),
            //    keyMap: "test-values/{AccountId}",
            //    fileExtension: ".txt");

            //TestDataStore(
            //    dataStore,
            //    testKey,
            //    testValueA,
            //    testValueB);
        }

        static void InitializeLocalStorage()
        {
            var path = Path.Combine(
                Path.GetTempPath(),
                Guid.NewGuid().ToString("N"));

            Directory.CreateDirectory(path);

            Directory.SetCurrentDirectory(path);
        }

        public class TestKey : ITestKey
        {
            public TestKey(Guid? accountId)
            {
                AccountId = accountId;
            }

            public Guid? AccountId { get; }
        }
    }
}
