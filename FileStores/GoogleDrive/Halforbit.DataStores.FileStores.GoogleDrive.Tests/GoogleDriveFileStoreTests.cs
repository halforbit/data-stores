using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Model;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using Xunit;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Tests
{
    public class GoogleFileStoreDriveTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.FileStores.GoogleDrive.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestGoogleDrive()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new GoogleDriveFileStore(
                    applicationName: GetConfig("ApplicationName"),
                    serviceAccountEmail: GetConfig("ServiceAccountEmail"),
                    serviceAccountKey: GetConfig("ServiceAccountKey")),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            TestDataStore(
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
    }
}
