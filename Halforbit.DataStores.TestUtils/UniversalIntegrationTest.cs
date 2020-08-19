using Halforbit.ObjectTools.Extensions;
using KellermanSoftware.CompareNetObjects;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

        protected static async Task TestBulkApi<TKey, TValue>(
            IDataStore<TKey, TValue> dataStore,
            Func<int, int, KeyValuePair<TKey, TValue>> dataGenerator)
            where TKey : IEquatable<TKey>
            where TValue : IEquatable<TValue>
        {
            IReadOnlyList<KeyValuePair<TKey, TValue>> GenerateDataWithMod(
                int count,
                int mod)
            {
                var d =  new List<KeyValuePair<TKey, TValue>>();
                for (var i = 0; i < count; i++)
                {
                    d.Add(dataGenerator(i, i * mod));
                }

                return d;
            }

            var bulkData = GenerateDataWithMod(150, 1);
            
            void AssertSequenceEqual<T>(
                IEnumerable<T> expected,
                IEnumerable<T> actual)
            {
                //Sort the collections by hash code before checking for sequence equality.
                Assert.Equal(expected.OrderBy(x => x.GetHashCode()),
                    actual.OrderBy(x => x.GetHashCode()));
            }
            
            var createResult = await dataStore.Create(bulkData);
            Assert.True(createResult.All(kvp => kvp.Value));

            var listKeysResult = await dataStore.ListKeys();
            AssertSequenceEqual(bulkData.Select(x => x.Key), listKeysResult);

            var listKeyValuesResult = await dataStore.ListKeyValues();
            AssertSequenceEqual(bulkData, listKeyValuesResult);

            var updatedData = GenerateDataWithMod(150, 5);
            
            var updateResult = await dataStore.Update(updatedData);
            Assert.True(updateResult.All(kvp => kvp.Value));

            var updatedValuesResult = await dataStore.ListKeyValues();
            AssertSequenceEqual(updatedData, updatedValuesResult);

            var upsertData = GenerateDataWithMod(200, 10);
            await dataStore.Upsert(upsertData);

            var upsertValuesResult = await dataStore.ListKeyValues();
            AssertSequenceEqual(upsertData, upsertValuesResult);

            var deleteResult = await dataStore.Delete(upsertData.Select(k => k.Key));
            Assert.True(deleteResult.All(r => r.Value));

            Assert.Empty(await dataStore.ListKeys());
        }

        protected static void ClearDataStore<TKey, TValue>(IDataStore<TKey, TValue> dataStore)
        {
            var keys = dataStore.ListKeys().Result;

            var deleted = dataStore.Delete(keys).Result;
            if (deleted.Any(kvp => !kvp.Value))
            {
                throw new Exception("Failed to clear data store");
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

        public class TestValue : Document, IEquatable<TestValue>
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

            public bool Equals(
                TestValue other)
            {
                if (ReferenceEquals(null, other))
                {
                    return false;
                }

                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                return AccountId.Equals(other.AccountId) && Message == other.Message;
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

                return Equals((TestValue) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (AccountId.GetHashCode() * 397) ^ (Message != null ? Message.GetHashCode() : 0);
                }
            }

            public static bool operator ==(
                TestValue left,
                TestValue right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(
                TestValue left,
                TestValue right)
            {
                return !Equals(left, right);
            }
            
            public class Key : UniversalIntegrationTest.ITestKey, IEquatable<Key>
            {
                public Key(Guid? accountId)
                {
                    AccountId = accountId;
                }

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

                    return Nullable.Equals(AccountId, other.AccountId);
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
                    return AccountId.GetHashCode();
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
}
