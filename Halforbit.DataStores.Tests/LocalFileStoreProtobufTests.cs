using Cloud.Data.Serialization.Protobuf.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.DataStores.Tests;
using ProtoBuf;
using System;
using System.IO;
using Xunit;

namespace Halforbit.DataStores.FileStores.LocalStorage.Tests
{
    public class LocalFileStoreProtobufTests : UniversalIntegrationTest
    {
        [Fact, Trait("Type", "Integration"), Trait("Type", "RunOnBuild")]
        public void TestLocalFileStore_Protobuf()
        {
            InitializeLocalStorage();

            var rootPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "test-file-store");

            //if (Directory.Exists(rootPath)) Directory.Delete(rootPath, true);

            var testKey = new ProtobufTestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new ProtobufTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Hello, world!"
            };

            var testValueB = new ProtobufTestValue
            {
                AccountId = testKey.AccountId.Value,

                Message = "Kthx, world!"
            };

            var dataStore = new FileStoreDataStore<ProtobufTestValue.Key, ProtobufTestValue>(
                fileStore: new LocalFileStore(rootPath: rootPath),
                serializer: new ProtobufSerializer(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".protobuf.data");

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

        [ProtoContract]
        public class ProtobufTestValue
        {
            [ProtoMember(1)]
            public Guid AccountId { get; set; }

            [ProtoMember(2)]
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
