using Halforbit.DataStores.DocumentStores.DocumentDb.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using Microsoft.Extensions.Configuration;
using System;
using Xunit;

namespace Halforbit.DataStores.DocumentStores.DocumentDb.Tests
{
    public class DocumentDbDataStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.DocumentStores.DocumentDb.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestDocumentDb()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            try
            {
                var dataStore = new DocumentDbDataStore<TestValue.Key, TestValue>(
                    endpoint: GetConfig("Endpoint"),
                    authKey: GetConfig("AuthKey"),
                    database: GetConfig("Database"),
                    collection: GetConfig("Collection"),
                    keyMap: "test-values/{AccountId}");

                TestDataStore(
                    dataStore,
                    testKey,
                    testValueA,
                    testValueB);
            }
            catch (Exception ex)
            {
                if (ex.ToString().Contains("Unable to connect"))
                {
                    //Assert.Inconclusive("Cannot connect to the DocumentDB emulator");
                }

                throw;
            }
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
