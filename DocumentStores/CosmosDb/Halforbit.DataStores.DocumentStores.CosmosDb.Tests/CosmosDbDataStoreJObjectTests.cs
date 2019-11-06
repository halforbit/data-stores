using Halforbit.DataStores.DocumentStores.CosmosDb.Implementation;
using Halforbit.DataStores.Tests;
using Newtonsoft.Json.Linq;
using System;
using Xunit;

namespace Halforbit.DataStores.DocumentStores.CosmosDb.Tests
{
    public class CosmosDbDataStoreJObjectTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.DocumentStores.CosmosDb.Tests.Partitioned.";

        [Fact, Trait("Type", "Integration")]
        public void TestCosmosDbJObject()
        {
            var testKey = new JObjectTestKey(
                partitionId: Guid.NewGuid(),
                accountId: Guid.NewGuid());

            var testValueA = JObject.FromObject(new PartitionedTestValue(
                partitionId: testKey.PartitionId.Value,
                accountId: testKey.AccountId.Value,
                message: "Hello, world!"));

            var testValueB = JObject.FromObject(new PartitionedTestValue(
                partitionId: testKey.PartitionId.Value,
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!"));

            var dataStore = new CosmosDbDataStore<JObjectTestKey, JObject>(
                endpoint: GetConfig("Endpoint"),
                authKey: GetConfig("AuthKey"),
                databaseId: GetConfig("DatabaseId"),
                containerId: GetConfig("CollectionId"),
                keyMap: "{PartitionId}|test-jobject-values/{AccountId}");

            ClearDataStore(dataStore);
                
            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);

            ClearDataStore(dataStore);
        }
    }

    public class JObjectTestKey : UniversalIntegrationTest.ITestKey
    {
        public JObjectTestKey(
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
