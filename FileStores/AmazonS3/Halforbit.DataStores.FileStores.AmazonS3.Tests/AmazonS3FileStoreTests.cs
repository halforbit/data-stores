using Halforbit.DataStores.FileStores.AmazonS3.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Halforbit.DataStores.FileStores.AmazonS3.Tests
{
    public class AmazonS3FileStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.FileStores.AmazonS3.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestAmazonS3FileStore()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new AmazonS3FileStore(
                    accessKeyId: GetConfig("AccessKeyId"),
                    secretAccessKey: GetConfig("SecretAccessKey"),
                    bucketName: GetConfig("BucketName")),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }
        
        [Fact, Trait("Type", "Integration")]
        public async Task RunBulkApiTests()
        {
            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new AmazonS3FileStore(
                    accessKeyId: GetConfig("AccessKeyId"),
                    secretAccessKey: GetConfig("SecretAccessKey"),
                    bucketName: GetConfig("BucketName")),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            ClearDataStore(dataStore);
     
            await TestBulkApi(dataStore,
                (keyGen, dataGen) =>
                {
                    var accountId = keyGen.ToGuid();

                    return new KeyValuePair<TestValue.Key, TestValue>(new TestValue.Key(accountId),
                        new TestValue(accountId, $"Test: {dataGen}"));
                });
        }
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
}
