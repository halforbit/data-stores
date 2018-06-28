using Halforbit.DataStores.FileStores.BlobStorage.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.Tests;
using Halforbit.ObjectTools.Extensions;
using Microsoft.Extensions.Configuration;
using System;
using Xunit;

namespace Halforbit.DataStores.FileStores.BlobStorage.Tests
{
    public class AzureStorageMessageQueueTests
    {
        public AzureStorageMessageQueueTests()
        {
            Configuration = new ConfigurationBuilder()
                .AddUserSecrets<UniversalIntegrationTest>()
                .Build();
        }

        IConfigurationRoot Configuration { get; set; }

        [Fact, Trait("Type", "Integration")]
        public void TestAzureStorageMessageQueue()
        {
            var connectionString = Configuration[
                "Halforbit.DataStores.FileStores.BlobStorage.Tests.ConnectionString"];

            var queue = new AzureStorageMessageQueue<TestMessage>(
                queueConnectionString: connectionString,
                queueName: "test",
                visibilityTimeout: "15",
                serializer: new JsonSerializer());

            var content = new TestMessage(
                accountId: Guid.NewGuid(),
                message: "Hello, world!");

            Assert.Equal(0, queue.GetApproximateMessageCount().Value);

            var putMessage = queue.Put(content);

            Assert.NotNull(putMessage.Id);

            Assert.NotNull(putMessage.Receipt);

            Assert.Equal(1, queue.GetApproximateMessageCount().Value);

            var getMessage = queue.Get();

            Assert.NotNull(getMessage.Id);

            Assert.NotNull(getMessage.Receipt);

            Assert.Equal(1, queue.GetApproximateMessageCount().Value);

            queue.Delete(getMessage);

            Assert.Equal(0, queue.GetApproximateMessageCount().Value);

            getMessage = queue.Get();

            Assert.Null(getMessage);            
        }
    }

    public class TestMessage
    {
        public TestMessage(
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
