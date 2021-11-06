using Halforbit.DataStores.FileStores.BlobStorage.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Halforbit.DataStores.FileStores.BlobStorage.Tests
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum TargetAdPlatform
    {
        Unknown = 0,
        GoogleAdWords,
        MicrosoftBing
    }

    public class CallExtensionOverride
    {
        public CallExtensionOverride(
            Guid accountId,
            TargetAdPlatform adPlatform)
        {
            AccountId = accountId;
            AdPlatform = adPlatform;
        }

        public Guid AccountId { get; }

        public TargetAdPlatform AdPlatform { get; }

        public class Key : IEquatable<Key>
        {
            public Key(
                Guid? accountId = null,
                TargetAdPlatform? adPlatform = null)
            {
                AccountId = accountId;
                AdPlatform = adPlatform;
            }

            public Guid? AccountId { get; }

            public TargetAdPlatform? AdPlatform { get; }

            #region IEquatable<Key> Members
            public bool Equals(Key other)
            {
                return other != null &&
                       EqualityComparer<Guid?>.Default.Equals(AccountId, other.AccountId) &&
                       EqualityComparer<TargetAdPlatform?>.Default.Equals(AdPlatform, other.AdPlatform);
            }
            public override bool Equals(object obj)
            {
                return Equals(obj as Key);
            }

            public override int GetHashCode()
            {
                var hashCode = 2035860012;
                hashCode = hashCode* -1521134295 + EqualityComparer<Guid?>.Default.GetHashCode(AccountId);
                hashCode = hashCode* -1521134295 + EqualityComparer<TargetAdPlatform?>.Default.GetHashCode(AdPlatform);
                return hashCode;
            }

            #endregion
        }
    }

    public class BlobFileStoreTests : UniversalIntegrationTest
    {
        readonly ITestOutputHelper _testOutputHelper;

        protected override string ConfigPrefix => "Halforbit.DataStores.FileStores.BlobStorage.Tests.";

        public BlobFileStoreTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact, Trait("Type", "Integration")]
        public void TestBlobFileStore()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new BlobFileStore(
                    GetConfig("ConnectionString"),
                    "test-kvs",
                    "application/json"),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json", 
                logger: new TestLogger(_testOutputHelper));

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        [Fact, Trait("Type", "Integration")]
        public void TestBlobFileStore_OptimisticConcurrency()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new BlobFileStore(
                    GetConfig("ConnectionString"),
                    "test-kvs",
                    "application/json"),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            dataStore
                .Upsert(testKey, o =>
                {
                    Assert.Null(o);

                    return testValueB;
                })
                .Wait();

            dataStore
                .Upsert(testKey, o =>
                {
                    Assert.Equal(testValueB.AccountId, o.AccountId);

                    Assert.Equal(testValueB.Message, o.Message);

                    return testValueA;
                })
                .Wait();
        }
        
        [Fact, Trait("Type", "Integration")]
        public async Task RunBulkApiTests()
        {
            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new BlobFileStore(
                    GetConfig("ConnectionString"),
                    "test-kvs",
                    "application/json"),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            ClearDataStore(dataStore);
     
            await TestBulkApi(dataStore,
                (keyGen, dataGen) =>
                {
                    var accountId = keyGen.ToGuid();

                    return new KeyValuePair<TestValue.Key, TestValue>(new TestValue.Key(accountId),
                        new TestValue(accountId, $"Test: {dataGen}"));
                });
        }
    }

    public class TestValue
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

    class TestLogger : ILogger
    {
        readonly ITestOutputHelper _testOutputHelper;

        public TestLogger(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            throw new NotImplementedException();
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            _testOutputHelper.WriteLine($"{logLevel}: {formatter(state, exception)}");
        }
    }
}
