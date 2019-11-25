using Halforbit.DataStores.Facets;
using Halforbit.DataStores.FileStores.Facets;
using Halforbit.DataStores.FileStores.GoogleDrive.Facets;
using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Facets;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Model;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.Tests;
using Halforbit.Facets.Implementation;
using Halforbit.Facets.Interface;
using System;
using Xunit;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Tests
{
    public class GoogleDriveFileStoreTests : UniversalIntegrationTest
    {
        protected override string ConfigPrefix => "Halforbit.DataStores.FileStores.GoogleDrive.Tests.";

        [Fact, Trait("Type", "Integration")]
        public void TestGoogleDrive()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new FileStoreDataStore<TestValue.Key, TestValue>(
                fileStore: new GoogleDriveFileStore(
                    applicationName: GetConfig("ApplicationName"),
                    serviceAccountEmail: GetConfig("ServiceAccountEmail"),
                    serviceAccountKey: GetConfig("ServiceAccountKey"),
                    grantAccessToEmails: GetConfig("GrantAccessToEmails")),
                serializer: new JsonSerializer($"{JsonOptions.Default}"),
                keyMap: "test-values/{AccountId}",
                fileExtension: ".json");

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        [Fact, Trait("Type", "Integration")]
        public void TestGoogleDrive_Context()
        {
            var testKey = new TestValue.Key(accountId: Guid.NewGuid());

            var testValueA = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Hello, world!");

            var testValueB = new TestValue(
                accountId: testKey.AccountId.Value,
                message: "Kthx, world!");

            var dataStore = new ContextFactory(new ConfigurationProvider(k => GetConfig(k)))
                .Create<IGoogleDriveTestDataContext>().Store;

            TestDataStore(
                dataStore,
                testKey,
                testValueA,
                testValueB);
        }

        class ConfigurationProvider : IConfigurationProvider
        {
            readonly Func<string, string> _getValue;

            public ConfigurationProvider(Func<string, string> getValue)
            {
                _getValue = getValue;
            }

            public string GetValue(string key) => _getValue(key);
        }
    }

    public interface IGoogleDriveTestDataContext : IContext
    {
        [ApplicationName(configKey: "ApplicationName")]
        [ServiceAccountEmail(configKey: "ServiceAccountEmail"), ServiceAccountKey(configKey: "ServiceAccountKey")]
        [GrantAccessToEmails("jim@halforbit.com")]
        [JsonSerialization, FileExtension(".json")]
        [KeyMap("test-values/{AccountId}")]
        IDataStore<UniversalIntegrationTest.TestValue.Key, UniversalIntegrationTest.TestValue> Store { get; }
    }
}
