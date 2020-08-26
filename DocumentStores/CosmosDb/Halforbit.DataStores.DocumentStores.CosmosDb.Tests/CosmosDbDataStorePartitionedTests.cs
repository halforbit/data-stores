using Halforbit.DataStores.DocumentStores.CosmosDb.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Collections.Generic;
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

        [Fact, Trait("Type", "Integration")]
        public async Task RunBulkApiTests()
        {
            var ds = new CosmosDbDataStore<PartitionedTestValue.Key, PartitionedTestValue>(
                connectionString: GetConfig("ConnectionString"),
                databaseId: GetConfig("DatabaseId"),
                containerId: GetConfig("CollectionId"),
                keyMap: "{PartitionId}|test-values/{AccountId}");

            ClearDataStore(ds);
            
            await TestBulkApi(ds,
                (keyGen, dataGen) =>
                {
                    var partitionId = (keyGen / 100).ToGuid();
                    var accountId = keyGen.ToGuid();

                    return new KeyValuePair<PartitionedTestValue.Key, PartitionedTestValue>(new PartitionedTestValue.Key(partitionId, accountId),
                        new PartitionedTestValue(partitionId, accountId, $"Test: {dataGen}"));
                });
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

    public class PartitionedTestValue : Document, IEquatable<PartitionedTestValue>
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
        
        public bool Equals(
            PartitionedTestValue other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return PartitionId.Equals(other.PartitionId) && AccountId.Equals(other.AccountId) && Message == other.Message;
        }

        public override bool Equals(
            object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return Equals((PartitionedTestValue) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = PartitionId.GetHashCode();
                hashCode = (hashCode * 397) ^ AccountId.GetHashCode();
                hashCode = (hashCode * 397) ^ (Message != null ? Message.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(
            PartitionedTestValue left,
            PartitionedTestValue right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(
            PartitionedTestValue left,
            PartitionedTestValue right)
        {
            return !Equals(left, right);
        }
        
        public class Key : UniversalIntegrationTest.ITestKey, IEquatable<Key>
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

            public bool Equals(
                Key other)
            {
                if (ReferenceEquals(null, other))
                {
                    return false;
                }

                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                return Nullable.Equals(PartitionId, other.PartitionId) && Nullable.Equals(AccountId, other.AccountId);
            }

            public override bool Equals(
                object obj)
            {
                if (ReferenceEquals(null, obj))
                {
                    return false;
                }

                if (ReferenceEquals(this, obj))
                {
                    return true;
                }

                if (obj.GetType() != this.GetType())
                {
                    return false;
                }

                return Equals((Key) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (PartitionId.GetHashCode() * 397) ^ AccountId.GetHashCode();
                }
            }

            public static bool operator ==(
                Key left,
                Key right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(
                Key left,
                Key right)
            {
                return !Equals(left, right);
            }
        }
    }
}
