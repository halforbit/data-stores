using Halforbit.DataStores.DocumentStores.Model;
using Halforbit.DataStores.Interface;
using Halforbit.ObjectTools.Extensions;
using KellermanSoftware.CompareNetObjects;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Halforbit.DataStores.Tests
{
    public abstract class UniversalIntegrationTest
    {
        protected virtual string ConfigPrefix => string.Empty;

        IConfigurationRoot Configuration { get; set; }

        public UniversalIntegrationTest()
        {
            Configuration = new ConfigurationBuilder()
                .AddUserSecrets<UniversalIntegrationTest>()
                .Build();
        }

        protected string GetConfig(string key) => Configuration[ConfigPrefix + key];

        protected static void TestDataStore<TKey, TValue>(
            IDataStore<TKey, TValue> dataStore,
            TKey testKey,
            TValue testValueA,
            TValue testValueB)
            where TKey : ITestKey
        {
            var compareConfig = new ComparisonConfig
            {
                MembersToIgnore = new List<string> { "Id" }
            };

            Action<object, object> assertAreEqual = (a, b) =>
            {
                if (a.GetType() == typeof(string))
                {
                    Assert.Equal(a, b);
                }
                else
                {
                    Assert.Equal(
                      StripUnderscoreProperties(JObject.FromObject(a)).ToString(),
                      StripUnderscoreProperties(JObject.FromObject(b)).ToString());
                }
            };

            var preExistsResult = dataStore.Exists(testKey).Result;

            Assert.False(preExistsResult);

            var preGetResult = dataStore.Get(testKey).Result;

            Assert.Null(preGetResult);

            var preListResult = dataStore
                .ListValues(k => k.AccountId == testKey.AccountId).Result
                .ToList();

            Assert.False(preListResult.Any());

            var preUpdateResult = dataStore.Update(testKey, testValueA).Result;

            Assert.False(preUpdateResult);

            var createResult = dataStore.Create(testKey, testValueA).Result;

            Assert.True(createResult);

            var firstExistsResult = dataStore.Exists(testKey).Result;

            Assert.True(firstExistsResult);

            var firstGetResult = dataStore.Get(testKey).Result;

            assertAreEqual(testValueA, firstGetResult);

            var firstListResult = dataStore
                .ListValues(k => k.AccountId == testKey.AccountId).Result
                .ToList();

            assertAreEqual(testValueA, firstListResult.Single());

            var postCreateResult = dataStore.Create(testKey, testValueA).Result;

            Assert.False(postCreateResult);

            var updateResult = dataStore.Update(testKey, testValueB).Result;

            Assert.True(updateResult);

            var secondExistsResult = dataStore.Exists(testKey).Result;

            Assert.True(secondExistsResult);

            var secondGetResult = dataStore.Get(testKey).Result;

            assertAreEqual(testValueB, secondGetResult);

            dataStore.Upsert(testKey, testValueA).Wait();

            var thirdGetResult = dataStore.Get(testKey).Result;

            assertAreEqual(testValueA, thirdGetResult);

            var firstDeleteResult = dataStore.Delete(testKey).Result;

            Assert.True(firstDeleteResult);

            var postExistsResult = dataStore.Exists(testKey).Result;

            Assert.False(postExistsResult);

            var postGetResult = dataStore.Get(testKey).Result;

            Assert.Null(postGetResult);

            var postListResult = dataStore
                .ListValues(k => k.AccountId == testKey.AccountId).Result
                .ToList();

            Assert.False(postListResult.Any());

            var secondDeleteResult = dataStore.Delete(testKey).Result;

            Assert.False(secondDeleteResult);
        }

        protected static void ClearDataStore<TKey, TValue>(IDataStore<TKey, TValue> dataStore)
        {
            foreach (var k in dataStore.ListKeys().Result)
            {
                var deleted = dataStore.Delete(k).Result;

                if (!deleted)
                {
                    throw new Exception("Failed to clear data store");
                }
            }
        }

        static JObject StripUnderscoreProperties(JObject jObject)
        {
            var copy = jObject.DeepClone() as JObject;

            foreach (var property in jObject)
            {
                if (property.Key.StartsWith("_"))
                {
                    copy.Remove(property.Key);
                }
            }

            return copy;
        }

        public interface ITestKey
        {
            Guid? AccountId { get; }
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
}
