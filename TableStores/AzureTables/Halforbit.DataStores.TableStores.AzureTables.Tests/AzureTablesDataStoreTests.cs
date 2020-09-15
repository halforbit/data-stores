using Halforbit.DataStores.TableStores.AzureTables.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Halforbit.DataStores.TableStores.AzureTables.Tests
{
    public class AzureTablesDataStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.TableStores.AzureTables.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestAzureTables()
        {
            var testKey = new AzureTablesTestValue.Key(
                accountId: Guid.NewGuid(),
                secondaryId: Guid.NewGuid());

            var testValueA = new AzureTablesTestValue(
                accountId: testKey.AccountId.Value,
                secondaryId: testKey.SecondaryId.Value,
                message: "Hello, world!");

            var testValueB = new AzureTablesTestValue(
                accountId: testKey.AccountId.Value,
                secondaryId: testKey.SecondaryId.Value,
                message: "Kthx, world!");

            var dataStore = new AzureTableStore<AzureTablesTestValue.Key, AzureTablesTestValue>(
                connectionString: GetConfig("ConnectionString"),
                tableName: GetConfig("TableName"),
                keyMap: "test-values_{AccountId}|{SecondaryId}");

            ClearDataStore(dataStore);

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }
        
        [Fact, Trait("Type", "Integration")]
        public async Task RunBulkApiTests()
        {
            var dataStore = new AzureTableStore<AzureTablesTestValue.Key, AzureTablesTestValue>(
                connectionString: GetConfig("ConnectionString"),
                tableName: GetConfig("TableName"),
                keyMap: "test-values_{AccountId}|{SecondaryId}");

            ClearDataStore(dataStore);
     
            await TestBulkApi(dataStore,
                (keyGen, dataGen) =>
                {
                    var partitionId = (keyGen / 100).ToGuid();
                    var accountId = keyGen.ToGuid();

                    return new KeyValuePair<AzureTablesTestValue.Key, AzureTablesTestValue>(new AzureTablesTestValue.Key(partitionId, accountId),
                        new AzureTablesTestValue(partitionId, accountId, $"Test: {dataGen}"));
                });
        }

        [Fact, Trait("Type", "Integration")]
        public void TestPartialPartitionKey() 
        {
            var testKeyA = new AzureTablesTestValue.Key(
                accountId: Guid.NewGuid(),
                secondaryId: Guid.NewGuid());

            var testKeyB = new AzureTablesTestValue.Key(
                accountId: Guid.NewGuid(),
                secondaryId: Guid.NewGuid());

            var testKeyC = new AzureTablesTestValue.Key(
                accountId: testKeyB.AccountId,
                secondaryId: Guid.NewGuid());

            var testValueA = new AzureTablesTestValue(
                accountId: testKeyA.AccountId.Value,
                secondaryId: testKeyA.SecondaryId.Value,
                message: "Hello, world!");

            var testValueB = new AzureTablesTestValue(
                accountId: testKeyB.AccountId.Value,
                secondaryId: testKeyB.SecondaryId.Value,
                message: "Kthx, world!");

            var testValueC = new AzureTablesTestValue(
                accountId: testKeyC.AccountId.Value,
                secondaryId: testKeyC.SecondaryId.Value,
                message: "¡Hola, mundo!");

            var dataStore = new AzureTableStore<AzureTablesTestValue.Key, AzureTablesTestValue>(
                connectionString: GetConfig("ConnectionString"),
                tableName: GetConfig("TableName"),
                keyMap: "test-values_{AccountId}_key-map-test|{SecondaryId}");

            ClearDataStore(dataStore);

            var preResult = dataStore.ListValues().Result;

            var createAResult = dataStore.Create(testKeyA, testValueA).Result;

            var createBResult = dataStore.Create(testKeyB, testValueB).Result;

            var createCResult = dataStore.Create(testKeyC, testValueC).Result;

            var listAllResult = dataStore.ListValues().Result;

            Assert.True(listAllResult.Count() == 3);

            var listAResult = dataStore.ListValues(k => k.AccountId == testKeyA.AccountId).Result;

            Assert.True(listAResult.SingleOrDefault()?.AccountId == testKeyA.AccountId);

            var listBCResult = dataStore.ListValues(k => k.AccountId == testKeyB.AccountId).Result.ToList();

            Assert.True(
                listBCResult.Count == 2 
                && listBCResult.Select(r => r.AccountId).Distinct().SingleOrDefault() == testKeyB.AccountId);

            var listBSpecificResult = dataStore
                .ListValues(k => k.AccountId == testKeyB.AccountId && k.SecondaryId == testKeyB.SecondaryId)
                .Result;
        
            Assert.True(listBSpecificResult.SingleOrDefault()?.SecondaryId == testKeyB.SecondaryId);

            var deleteAResult = dataStore.Delete(testKeyA).Result;

            var deleteBResult = dataStore.Delete(testKeyB).Result;

            var deleteCResult = dataStore.Delete(testKeyC).Result;

            var postListResult = dataStore.ListKeyValues().Result;

            Assert.True(postListResult.Count() == 0);
        }
    }

    public class AzureTablesTestValue : IEquatable<AzureTablesTestValue>
    {
        public AzureTablesTestValue() {

        }

        public AzureTablesTestValue(
            Guid accountId = default(Guid),
            Guid secondaryId = default(Guid),
            string message = default(string))
        {
            AccountId = accountId.OrNewGuidIfDefault();

            SecondaryId = secondaryId.OrNewGuidIfDefault();

            Message = message;
        }

        public Guid AccountId { get; private set; }

        public Guid SecondaryId { get; private set; }

        public string Message { get; private set; }
        
        public bool Equals(
            AzureTablesTestValue other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return AccountId.Equals(other.AccountId) && SecondaryId.Equals(other.SecondaryId) && Message == other.Message;
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

            return Equals((AzureTablesTestValue) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = AccountId.GetHashCode();
                hashCode = (hashCode * 397) ^ SecondaryId.GetHashCode();
                hashCode = (hashCode * 397) ^ (Message != null ? Message.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(
            AzureTablesTestValue left,
            AzureTablesTestValue right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(
            AzureTablesTestValue left,
            AzureTablesTestValue right)
        {
            return !Equals(left, right);
        }
        
        public class Key : UniversalIntegrationTest.ITestKey, IEquatable<Key>
        {
            public Key(
                Guid? accountId,
                Guid? secondaryId)
            {
                AccountId = accountId;

                SecondaryId = secondaryId;
            }

            public Guid? AccountId { get; }

            public Guid? SecondaryId { get; }

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

                return Nullable.Equals(AccountId, other.AccountId) && Nullable.Equals(SecondaryId, other.SecondaryId);
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
                    return (AccountId.GetHashCode() * 397) ^ SecondaryId.GetHashCode();
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
