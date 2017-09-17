using Halforbit.DataStores.FileStores.AmazonS3.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using Microsoft.Extensions.Configuration;
using System;
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
                serializer: new JsonSerializer(),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
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
