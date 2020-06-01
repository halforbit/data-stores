using Halforbit.DataStores.Facets;
using Halforbit.DataStores.FileStores.Facets;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Facets;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Sftp.Facets;
using Halforbit.DataStores.FileStores.Sftp.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.Facets.Implementation;
using Halforbit.Facets.Interface;
using Halforbit.ObjectTools.Extensions;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Halforbit.DataStores.FileStores.Sftp.Tests
{
    public class SftpFileStoreTests : UniversalIntegrationTest
    {
        const string Username = "username";
        
        const string Host = "localhost";
        
        const string Password = "password";
        
        readonly ITestOutputHelper _output;

        public SftpFileStoreTests(
            ITestOutputHelper output)
        {
            _output = output;
        }

        protected override string ConfigPrefix => "Halforbit.DataStores.FileStores.BlobStorage.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestSftpFileStore()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new SftpFileStore(
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
        public void TestSftpFileStore_FromRoot()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new SftpFileStore(
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

        [Fact, Trait("Type", "Stress")]
        public async Task TestSftpFileStore_Concurrent()
        {
            int concurrentCount = 30;

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new SftpFileStore(
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
        public async Task TestSftpFileStore_Context()
        {
            var dataStore = new ContextFactory().Create<ISftpDataContext>().Store;

            var tasks = Enumerable
                .Range(0, 3)
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

        public interface ISftpDataContext : IContext
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
