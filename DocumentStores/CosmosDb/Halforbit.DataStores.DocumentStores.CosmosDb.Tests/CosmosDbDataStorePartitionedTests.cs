using Halforbit.DataStores.DocumentStores.CosmosDb.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Halforbit.DataStores.DocumentStores.CosmosDb.Tests
{
    public class CosmosDbDataStorePartitionedTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.DocumentStores.CosmosDb.Tests.Partitioned.";

        [Fact, Trait("Type", "Integration")]
        public async Task TestCosmosDbPartitioned()
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
                connectionString: GetConfig("ConnectionString"),
                databaseId: GetConfig("DatabaseId"),
                containerId: GetConfig("CollectionId"),
                keyMap: "{PartitionId}|test-values/{AccountId}");

            ClearDataStore(dataStore);
                
            //TestDataStore(
            //    dataStore,
            //    testKey,
            //    testValueA,
            //    testValueB);

            //TestQuery(dataStore);

            //ClearDataStore(dataStore);

            await TestBatch(dataStore);
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

                var result2 = session
                    .Query(k => k.PartitionId == values[0].PartitionId)
                    .ToList();

                Assert.Single(result2);
            }

            ClearDataStore(dataStore);
        }

        static async Task TestBatch(
            IDataStore<PartitionedTestValue.Key, PartitionedTestValue> dataStore)
        {
            var allIds = Enumerable
                .Range(0, 1000)
                .Select(i => Guid.NewGuid())
                .ToList();

            var results = (await dataStore
                .BatchQuery(allIds, (b, q) => q.Where(r => b.Contains(r.AccountId))))
                .ToList();
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
