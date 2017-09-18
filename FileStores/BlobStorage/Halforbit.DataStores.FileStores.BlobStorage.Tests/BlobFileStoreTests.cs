using Halforbit.DataStores.FileStores.BlobStorage.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using Xunit;

namespace Halforbit.DataStores.FileStores.BlobStorage.Tests
{
    public class BlobFileStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.FileStores.BlobStorage.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestBlobFileStore()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new BlobFileStore(
                    GetConfig("ConnectionString"),
                    "test-kvs",
                    "application/json"),
                serializer: new JsonSerializer(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        [Fact, Trait("Type", "Integration")]
        public void TestBlobFileStore_OptimisticConcurrency()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new BlobFileStore(
                    GetConfig("ConnectionString"),
                    "test-kvs",
                    "application/json"),
                serializer: new JsonSerializer(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            var upsertResult1 = dataStore
                .Upsert(testKey, o =>
                {
                    Assert.Null(o);

                    return testValueB;
                })
                .Result;

            var upsertResult2 = dataStore
                .Upsert(testKey, o =>
                {
                    Assert.Equal(testValueB.AccountId, o.AccountId);

                    Assert.Equal(testValueB.Message, o.Message);

                    return testValueA;
                })
                .Result;
        }
    }

    public class TestValue
    {
        public TestValue(
            Guid accountId = default(Guid),
            string message = default(string))
        {
            AccountId = accountId.OrNewIfEmpty();

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
}
