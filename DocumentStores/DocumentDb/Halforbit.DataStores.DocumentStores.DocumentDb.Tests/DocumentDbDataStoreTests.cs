using Halforbit.DataStores.DocumentStores.DocumentDb.Implementation;
using Halforbit.DataStores.DocumentStores.Model;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
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

                var values = new[]
                {
                    new TestValue(Guid.NewGuid(), "abc"),

                    new TestValue(Guid.NewGuid(), "bcd"),

                    new TestValue(Guid.NewGuid(), "cde")
                };

                foreach(var value in values)
                {
                    dataStore.Create(new TestValue.Key(value.AccountId), value).Wait();
                }

                //var result = dataStore
                //    .Query();

                //var result2 = result
                //    .Where(v => v.Message.Contains("b"))
                //    .ToList();

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

    public class TestValue : Document
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
