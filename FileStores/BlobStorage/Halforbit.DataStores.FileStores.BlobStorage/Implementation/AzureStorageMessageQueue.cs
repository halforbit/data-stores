using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.Model;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Text;

namespace Halforbit.DataStores.FileStores.BlobStorage.Implementation
{
    public class AzureStorageMessageQueue<TContent> : IMessageQueue<TContent>
    {
        readonly CloudQueue _cloudQueueContainer;

        readonly TimeSpan _visibilityTimeout;

        readonly ISerializer _serializer;

        readonly UTF8Encoding _utf8Encoding = new UTF8Encoding(false);

        public AzureStorageMessageQueue(
            string queueConnectionString,
            string queueName,
            string visibilityTimeout,
            ISerializer serializer)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(queueConnectionString);

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();

            _cloudQueueContainer = cloudQueueClient.GetQueueReference(queueName);

            _cloudQueueContainer.CreateIfNotExistsAsync().Wait();

            _visibilityTimeout = TimeSpan.FromMinutes(double.Parse(visibilityTimeout));

            _serializer = serializer;
        }

        public void Delete(Message<TContent> message)
        {
            _cloudQueueContainer
                .DeleteMessageAsync(new CloudQueueMessage(message.Id, message.Receipt))
                .Wait();
        }

        public Message<TContent> Get()
        {
            var message = _cloudQueueContainer.GetMessageAsync(_visibilityTimeout, null, null).Result;

            if (message == null) return null;

            return new Message<TContent>(
                id: message.Id,
                receipt: message.PopReceipt,
                content: _serializer.Deserialize<TContent>(message.AsBytes).Result);
        }

        public int? GetApproximateMessageCount()
        {
            _cloudQueueContainer.FetchAttributesAsync().Wait();

            return _cloudQueueContainer.ApproximateMessageCount;
        }

        public Message<TContent> Put(TContent content)
        {
            var message = new CloudQueueMessage(_utf8Encoding.GetString(_serializer.Serialize(content).Result));

            _cloudQueueContainer.AddMessageAsync(message).Wait();

            return new Message<TContent>(
                id: message.Id,
                receipt: message.PopReceipt,
                content: content);
        }
    }
}
