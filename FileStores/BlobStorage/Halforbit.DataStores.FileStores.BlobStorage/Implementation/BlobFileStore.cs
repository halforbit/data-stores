using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.BlobStorage.Implementation
{
    public class BlobFileStore : IFileStore
    {
        readonly CloudBlobContainer _cloudBlobContainer;

        readonly string _contentType;

        readonly string _contentEncoding;

        public BlobFileStore(
            string connectionString, 
            string containerName,
            string contentType,
            string contentEncoding = null)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            _cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

            _cloudBlobContainer.CreateIfNotExistsAsync().Wait();

            _contentType = contentType;

            _contentEncoding = contentEncoding;
        }

        public async Task<bool> Delete(string path)
        {
            await GetBlob(path).DeleteIfExistsAsync();

            return true;
        }

        public async Task<bool> Exists(string path) => await GetBlob(path).ExistsAsync();

        public async Task<IEnumerable<string>> GetFiles(
            string pathPrefix, 
            string extension)
        {
            var results = new List<string>();

            var blobContinuationToken = default(BlobContinuationToken);

            do
            {
                var resultSegment = await _cloudBlobContainer.ListBlobsSegmentedAsync(
                    pathPrefix,
                    blobContinuationToken);

                foreach(var item in resultSegment.Results)
                {
                    var cloudBlockDirectory = item as CloudBlobDirectory;

                    if(cloudBlockDirectory != null)
                    {
                        results.AddRange(await GetFiles(cloudBlockDirectory.Prefix, extension));
                    }

                    var cloudBlockBlob = item as CloudBlockBlob;

                    if(cloudBlockBlob != null)
                    {
                        results.Add(cloudBlockBlob.Name);
                    }
                }
                
                blobContinuationToken = resultSegment.ContinuationToken;
            }
            while (blobContinuationToken != null);

            return results;
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path,
            bool getETag = false)
        {
            var memoryStream = new MemoryStream();

            var blob = GetBlob(path);
            
            await blob.DownloadToStreamAsync(memoryStream);

            return new FileStoreReadAllBytesResult(
                bytes: memoryStream.ToArray(),
                eTag: getETag ? blob.Properties.ETag : null);
        }

        public async Task<bool> WriteAllBytes(
            string path, 
            byte[] contents,
            string eTag = null)
        {
            var tasks = new List<Task>();

            var blob = GetBlob(path);

            var updateProperties = false;

            if(!string.IsNullOrWhiteSpace(_contentType))
            {
                blob.Properties.ContentType = _contentType;

                updateProperties = true;
            }

            if (!string.IsNullOrWhiteSpace(_contentEncoding))
            {
                blob.Properties.ContentEncoding = _contentEncoding;

                updateProperties = true;
            }

            if (eTag == null)
            {
                await blob.UploadFromByteArrayAsync(
                    buffer: contents,
                    index: 0,
                    count: contents.Length);
            }
            else
            {
                try
                {
                    await blob.UploadFromByteArrayAsync(
                        buffer: contents,
                        index: 0,
                        count: contents.Length,
                        accessCondition: AccessCondition.GenerateIfMatchCondition(eTag),
                        options: null,
                        operationContext: null);
                }
                catch (StorageException stex)
                {
                    if (stex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    {
                        return false;
                    }

                    throw;
                }
            }

            if (updateProperties)
            {
                await blob.SetPropertiesAsync();
            }

            return true;
        }

        CloudBlockBlob GetBlob(string path) => _cloudBlobContainer.GetBlockBlobReference(path);
    }
}
