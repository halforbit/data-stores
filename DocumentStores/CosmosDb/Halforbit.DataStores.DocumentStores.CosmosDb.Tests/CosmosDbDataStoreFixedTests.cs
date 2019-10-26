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
    public class CosmosDbDataStoreFixedTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.DocumentStores.CosmosDb.Tests.Fixed.";

        [Fact, Trait("Type", "Integration")]
        public void TestCosmosDbFixed()
        {
            var testKey = new FixedTestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new FixedTestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new FixedTestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new CosmosDbDataStore<FixedTestValue.Key, FixedTestValue>(
                endpoint: GetConfig("Endpoint"),
                authKey: GetConfig("AuthKey"),
                databaseId: GetConfig("DatabaseId"),
                containerId: GetConfig("CollectionId"),
                keyMap: "test-values/{AccountId}");

            ClearDataStore(dataStore);
                
            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);

            TestQuery(dataStore);
        }

        static void TestQuery(IDataStore<FixedTestValue.Key, FixedTestValue> dataStore)
        {
            var values = new[]
            {
                    new FixedTestValue(Guid.NewGuid(), "abc"),

                    new FixedTestValue(Guid.NewGuid(), "bcd"),

                    new FixedTestValue(Guid.NewGuid(), "cde")
                };

            foreach (var value in values)
            {
                dataStore.Create(new FixedTestValue.Key(value.AccountId), value).Wait();
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

    public class FixedTestValue : Document
    {
        public FixedTestValue(
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
