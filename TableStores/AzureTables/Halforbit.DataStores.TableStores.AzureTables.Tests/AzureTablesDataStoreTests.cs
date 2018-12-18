using Halforbit.DataStores.Interface;
using Halforbit.DataStores.TableStores.AzureTables.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Linq;
using Xunit;

namespace Halforbit.DataStores.TableStores.AzureTables.Tests
{
    public class AzureTablesDataStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.TableStores.AzureTables.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestAzureTables()
        {
            var testKey = new LocalTestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new LocalTestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new LocalTestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new AzureTableStore<LocalTestValue.Key, LocalTestValue>(
                connectionString: GetConfig("ConnectionString"),
                tableName: GetConfig("TableName"),
                keyMap: "test-values_{AccountId}");

            ClearDataStore(dataStore);

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }
    }

    public class LocalTestValue
    {
        public LocalTestValue() {

        }
        public LocalTestValue(
            Guid accountId = default(Guid),
            string message = default(string))
        {
            AccountId = accountId.OrNewGuidIfDefault();

            Message = message;
        }

        public Guid AccountId { get; private set; }

        public string Message { get; private set; }

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
