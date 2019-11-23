using Halforbit.DataStores.Facets;
using Halforbit.DataStores.FileStores.Facets;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Facets;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Model;
using Halforbit.DataStores.FileStores.Ftp.Facets;
using Halforbit.DataStores.FileStores.Ftp.Implementation;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.Tests;
using Halforbit.Facets.Implementation;
using Halforbit.Facets.Interface;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Halforbit.DataStores.FileStores.Ftp.Tests
{
    public class FtpFileStoreTests : UniversalIntegrationTest
    {
        const string Username = "username";
        
        const string Host = "localhost";
        
        const string Password = "password";
        
        readonly ITestOutputHelper _output;

        public FtpFileStoreTests(
            ITestOutputHelper output)
        {
            _output = output;
        }

        protected override string ConfigPrefix => "Halforbit.DataStores.FileStores.BlobStorage.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestFtpFileStore()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new FtpFileStore(
                    host: Host,
                    username: Username,
                    password: Password),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/another folder/moar_folder/{AccountId}",
                fileExtension: ".json");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        [Fact, Trait("Type", "Integration")]
        public void TestFtpFileStore_FromRoot()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new FtpFileStore(
                    host: Host,
                    username: Username,
                    password: Password),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "{AccountId}",
                fileExtension: ".json");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        [Fact, Trait("Type", "Integration")]
        public async Task TestFtpFileStore_Concurrent()
        {
            int concurrentCount = 100;

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new FtpFileStore(
                    host: Host,
                    username: Username,
                    password: Password,
                    maxConcurrentConnections: $"{concurrentCount}"),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/another folder/moar_folder/{AccountId}",
                fileExtension: ".json");

            var tasks = Enumerable
                .Range(0, concurrentCount)
                .Select(i => Task.Run(() =>
                {
                    var testKey = new TestValue.Key(accountId: Guid.NewGuid());

                    var testValueA = new TestValue(
                        accountId: testKey.AccountId.Value,
                        message: "Hello, world!");

                    var testValueB = new TestValue(
                        accountId: testKey.AccountId.Value,
                        message: "Kthx, world!");

                    _output.WriteLine($"Starting {i}");

                    TestDataStore(
                        dataStore,
                        testKey,
                        testValueA,
                        testValueB);

                    _output.WriteLine($"Finished {i}");
                }));

            await Task.WhenAll(tasks);
        }

        [Fact, Trait("Type", "Integration")]
        public async Task TestFtpFileStore_Context()
        {
            var dataStore = new ContextFactory().Create<IFtpDataContext>().Store;

            var tasks = Enumerable
                .Range(0, 10)
                .Select(i => Task.Run(() =>
                {
                    var testKey = new TestValue.Key(accountId: Guid.NewGuid());

                    var testValueA = new TestValue(
                        accountId: testKey.AccountId.Value,
                        message: "Hello, world!");

                    var testValueB = new TestValue(
                        accountId: testKey.AccountId.Value,
                        message: "Kthx, world!");

                    _output.WriteLine($"Starting {i}");

                    TestDataStore(
                        dataStore,
                        testKey,
                        testValueA,
                        testValueB);

                    _output.WriteLine($"Finished {i}");
                }));

            await Task.WhenAll(tasks);
        }

        public interface IFtpDataContext : IContext
        {
            [Host(Host), Username(Username), Password(Password)]
            [MaxConcurrentConnections(2), RetainEmptyFolders]
            [JsonSerialization, FileExtension(".json")]
            [KeyMap("test-values/another folder/moar_folder/{AccountId}")]
            IDataStore<TestValue.Key, TestValue> Store { get; }
        }
    }

    public class TestValue
    {
        public TestValue(
            Guid accountId = default,
            string message = default)
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
