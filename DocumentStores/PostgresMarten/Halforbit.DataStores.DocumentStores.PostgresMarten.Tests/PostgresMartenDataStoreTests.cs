using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Linq;
using Xunit;

namespace Halforbit.DataStores.DocumentStores.PostgresMarten.Tests
{
    public class PostgresMartenDataStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.DocumentStores.PostgresMarten.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestPostgresMarten()
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
                var dataStore = new PostgresMartenDataStore<TestValue.Key, TestValue>(
                    connectionString: "User ID=postgres;Password=postgres;Host=localhost;Port=5432;Database=postgres",
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
