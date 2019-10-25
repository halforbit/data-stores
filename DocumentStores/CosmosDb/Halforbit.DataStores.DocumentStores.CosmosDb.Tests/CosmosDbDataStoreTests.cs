using Halforbit.DataStores.DocumentStores.CosmosDb.Implementation;
using Halforbit.DataStores.DocumentStores.Model;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Linq;
using Xunit;

namespace Halforbit.DataStores.DocumentStores.CosmosDb.Tests
{
    public class CosmosDbDataStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.DocumentStores.CosmosDb.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestCosmosDb()
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

                ClearDataStore(dataStore);
                
                TestDataStore(
                    dataStore,
                    testKey,
                    testValueA,
                    testValueB);

                TestQuery(dataStore);
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

        static void TestQuery(IDataStore<TestValue.Key, TestValue> dataStore)
        {
            var values = new[]
            {
                    new TestValue(Guid.NewGuid(), "abc"),

                    new TestValue(Guid.NewGuid(), "bcd"),

                    new TestValue(Guid.NewGuid(), "cde")
                };

            foreach (var value in values)
            {
                dataStore.Create(new TestValue.Key(value.AccountId), value).Wait();
            }

            using (var session = dataStore.StartQuery())
            {
                var result = session.Query()
                    .Where(v => v.Message.Contains("b"))
                    .ToList();

                Assert.Equal(2, result.Count);
            }

            ClearDataStore(dataStore);
        }
    }

    public class TestValue : Document
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
