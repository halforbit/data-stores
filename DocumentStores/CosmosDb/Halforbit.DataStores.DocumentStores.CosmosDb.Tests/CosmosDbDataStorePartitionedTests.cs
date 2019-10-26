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
    public class CosmosDbDataStorePartitionedTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.DocumentStores.CosmosDb.Tests.Partitioned.";

        [Fact, Trait("Type", "Integration")]
        public void TestCosmosDbPartitioned()
        {
            var testKey = new PartitionedTestValue.Key(
                partitionId: Guid.NewGuid(),
                accountId: Guid.NewGuid());

            var testValueA = new PartitionedTestValue(
                partitionId: testKey.PartitionId.Value,
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new PartitionedTestValue(
                partitionId: testKey.PartitionId.Value,
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new CosmosDbDataStore<PartitionedTestValue.Key, PartitionedTestValue>(
                endpoint: GetConfig("Endpoint"),
                authKey: GetConfig("AuthKey"),
                databaseId: GetConfig("DatabaseId"),
                containerId: GetConfig("CollectionId"),
                keyMap: "{PartitionId:D}|test-values/{AccountId}");

            ClearDataStore(dataStore);
                
            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);

            TestQuery(dataStore);
        }

        static void TestQuery(IDataStore<PartitionedTestValue.Key, PartitionedTestValue> dataStore)
        {
            var values = new[]
            {
                new PartitionedTestValue(Guid.NewGuid(), Guid.NewGuid(), "abc"),

                new PartitionedTestValue(Guid.NewGuid(), Guid.NewGuid(), "bcd"),

                new PartitionedTestValue(Guid.NewGuid(), Guid.NewGuid(), "cde")
            };

            foreach (var value in values)
            {
                dataStore.Create(new PartitionedTestValue.Key(value.PartitionId, value.AccountId), value).Wait();
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

    public class PartitionedTestValue : Document
    {
        public PartitionedTestValue(
            Guid partitionId = default,
            Guid accountId = default,
            string message = default)
        {
            AccountId = accountId.OrNewGuidIfDefault();
            
            PartitionId = partitionId;
            
            Message = message;
        }

        public Guid PartitionId { get; }

        public Guid AccountId { get; }
        
        public string Message { get; }

        public class Key : UniversalIntegrationTest.ITestKey
        {
            public Key(
                Guid? partitionId, 
                Guid? accountId)
            {
                PartitionId = partitionId;
                
                AccountId = accountId;
            }

            public Guid? PartitionId { get; }
            
            public Guid? AccountId { get; }
        }
    }
}
