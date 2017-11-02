using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using Halforbit.DataStores.Model;
using Halforbit.Facets.Attributes;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Net;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.BlobStorage.Implementation
{
    public class BlobFileStore : IFileStore
    {
        readonly CloudBlobContainer _cloudBlobContainer;

        readonly string _contentType;

        readonly string _contentEncoding;

        readonly Lazy<IFileStoreContext> _fileStoreContext;

        public BlobFileStore(
            string connectionString, 
            string containerName,
            [Optional]string contentType = null,
            [Optional]string contentEncoding = null)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            _cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

            _cloudBlobContainer.CreateIfNotExistsAsync().Wait();

            _contentType = contentType;

            _contentEncoding = contentEncoding;

            _fileStoreContext = new Lazy<IFileStoreContext>(() => new BlobFileStoreContext(
                this, 
                Access.Full));
        }

        public IFileStoreContext FileStoreContext => _fileStoreContext.Value;

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
                    if (item is CloudBlobDirectory cloudBlockDirectory)
                    {
                        results.AddRange(await GetFiles(cloudBlockDirectory.Prefix, extension));
                    }

                    if (item is CloudBlockBlob cloudBlockBlob)
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

        class BlobFileStoreContext : IFileStoreContext
        {
            readonly BlobFileStore _blobFileStore;

            readonly Access _access;

            public BlobFileStoreContext(
                BlobFileStore blobFileStore,
                Access access)
            {
                _blobFileStore = blobFileStore;

                _access = access;
            }

            public async Task<EntityInfo> GetEntityInfo(string key)
            {
                AssertAccess(Access.Get);

                var blob = _blobFileStore.GetBlob(key);

                if (blob == null)
                {
                    return null;
                }

                return CloudBlockBlobToEntityInfo(blob);
            }

            public async Task<IReadOnlyDictionary<string, string>> GetMetadata(string key)
            {
                AssertAccess(Access.Get);

                var blob = _blobFileStore.GetBlob(key);

                return blob.Metadata as IReadOnlyDictionary<string, string>;
            }

            public async Task<Uri> GetSharedAccessUrl(
                string key, 
                DateTime expiration, 
                Access access)
            {
                AssertAccess(access);

                var blob = _blobFileStore.GetBlob(key);

                return new Uri(
                    blob.Uri.AbsoluteUri + 
                    blob.GetSharedAccessSignature(
                        new SharedAccessBlobPolicy
                        {
                            SharedAccessExpiryTime = expiration,

                            Permissions = AccessToSharedAccessPermissions(access)
                        }));
            }

            public Task<IReadOnlyDictionary<string, EntityInfo>> ListEntityInfos(
                Expression<Func<string, bool>> selector = null)
            {
                throw new NotImplementedException();
            }

            public Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> ListMetadatas(
                Expression<Func<string, bool>> selector = null)
            {
                throw new NotImplementedException();
            }

            public async Task SetEntityInfo(
                string key, 
                EntityInfo entityInfo)
            {
                var blob = _blobFileStore.GetBlob(key);

                blob.Properties.ContentType = entityInfo.ContentType;

                blob.Properties.ContentEncoding = entityInfo.ContentEncoding;

                await blob.SetPropertiesAsync();
            }

            public async Task SetMetadata(
                string key, 
                IReadOnlyDictionary<string, string> keyValues)
            {
                AssertAccess(Access.Put);

                var blob = _blobFileStore.GetBlob(key);

                blob.Metadata.Clear();

                foreach(var kv in keyValues)
                {
                    blob.Metadata[kv.Key] = kv.Value;
                }

                await blob.SetMetadataAsync();
            }

            void AssertAccess(Access access)
            {
                if ((_access & access) == Access.None)
                {
                    throw new AccessViolationException(
                        $"Repository does not have {access} access.");
                }
            }

            static EntityInfo CloudBlockBlobToEntityInfo(CloudBlockBlob cloudBlockBlob) => new EntityInfo(
                cloudBlockBlob.Name,
                cloudBlockBlob.Properties.LastModified.HasValue ?
                    cloudBlockBlob.Properties.LastModified.Value.UtcDateTime :
                    (DateTime?)null,
                cloudBlockBlob.Properties.Length,
                cloudBlockBlob.Uri.AbsoluteUri,
                cloudBlockBlob.Properties.ContentType,
                cloudBlockBlob.Properties.ContentEncoding);

            static SharedAccessBlobPermissions AccessToSharedAccessPermissions(Access access) =>
                (access.HasFlag(Access.Delete) ? SharedAccessBlobPermissions.Delete : 0) |
                (access.HasFlag(Access.Get) ? SharedAccessBlobPermissions.Read : 0) |
                (access.HasFlag(Access.List) ? SharedAccessBlobPermissions.List : 0) |
                (access.HasFlag(Access.Put) ? SharedAccessBlobPermissions.Write : 0);
        }
    }
}
