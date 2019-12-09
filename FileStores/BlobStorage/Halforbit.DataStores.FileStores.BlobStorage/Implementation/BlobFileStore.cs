using Halforbit.DataStores.Exceptions;
using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using Halforbit.DataStores.Model;
using Halforbit.Facets.Attributes;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        public async Task<bool> Exists(string path) => await GetBlob(path).ExistsAsync().ConfigureAwait(false);

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

        public async Task<bool> ReadStream(
            string path, 
            Stream contents, 
            bool getETag = false)
        {
            if (getETag) throw new NotImplementedException("eTag based optimistic concurrency is not implemented.");

            var blob = GetBlob(path);

            await blob.DownloadToStreamAsync(contents);

            return true;
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

        public async Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            if (eTag != null) throw new NotImplementedException("eTag based optimistic concurrency is not implemented for streams");

            var blob = GetBlob(path);

            var updateProperties = false;

            if (!string.IsNullOrWhiteSpace(_contentType))
            {
                blob.Properties.ContentType = _contentType;

                updateProperties = true;
            }

            if (!string.IsNullOrWhiteSpace(_contentEncoding))
            {
                blob.Properties.ContentEncoding = _contentEncoding;

                updateProperties = true;
            }

            await blob.UploadFromStreamAsync(contents);

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

                if (await _blobFileStore.Exists(key))
                {
                    var blob = _blobFileStore.GetBlob(key);

                    await blob.FetchAttributesAsync();

                    return CloudBlockBlobToEntityInfo(blob);
                }
                else
                {
                    return null;
                }
            }

            public async Task<IReadOnlyDictionary<string, string>> GetMetadata(
                string key,
                bool percentDecodeValues = true)
            {
                AssertAccess(Access.Get);

                var blob = _blobFileStore.GetBlob(key);

                if(await _blobFileStore.Exists(key))
                {
                    await blob.FetchAttributesAsync();

                    var keyValues = blob.Metadata as IReadOnlyDictionary<string, string>;

                    if (keyValues != null && percentDecodeValues)
                    {
                        keyValues = keyValues
                            .Select(kv => new KeyValuePair<string, string>(kv.Key, Uri.UnescapeDataString(kv.Value)))
                            .ToDictionary(kv => kv.Key, kv => kv.Value);
                    }

                    return keyValues;
                }
                else
                {
                    return null;
                }
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
                IReadOnlyDictionary<string, string> keyValues,
                bool percentEncodeValues = true)
            {
                AssertAccess(Access.Put);

                if (percentEncodeValues)
                {
                    keyValues = keyValues
                        .Select(kv => new KeyValuePair<string, string>(kv.Key, Uri.EscapeDataString(kv.Value)))
                        .ToDictionary(kv => kv.Key, kv => kv.Value);
                }

                var blob = _blobFileStore.GetBlob(key);

                blob.Metadata.Clear();

                foreach (var kv in keyValues)
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
                cloudBlockBlob.Properties.ContentEncoding,
                cloudBlockBlob.Properties.ContentMD5,
                ConvertLeaseState(cloudBlockBlob.Properties.LeaseState),
                cloudBlockBlob.Properties.LeaseStatus == LeaseStatus.Locked);

            static Halforbit.DataStores.Model.LeaseState ConvertLeaseState(
                Microsoft.WindowsAzure.Storage.Blob.LeaseState s)
            {
                switch(s)
                {
                    case Microsoft.WindowsAzure.Storage.Blob.LeaseState.Available:
                        return DataStores.Model.LeaseState.Available;
                    case Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking:
                        return DataStores.Model.LeaseState.Breaking;
                    case Microsoft.WindowsAzure.Storage.Blob.LeaseState.Broken:
                        return DataStores.Model.LeaseState.Broken;
                    case Microsoft.WindowsAzure.Storage.Blob.LeaseState.Expired:
                        return DataStores.Model.LeaseState.Expired;
                    case Microsoft.WindowsAzure.Storage.Blob.LeaseState.Leased:
                        return DataStores.Model.LeaseState.Leased;
                    case Microsoft.WindowsAzure.Storage.Blob.LeaseState.Unspecified:
                        return DataStores.Model.LeaseState.Unspecified;
                    default: throw new Exception($"Unhandled LeaseState of '{s}'.");
                }
            }

            static SharedAccessBlobPermissions AccessToSharedAccessPermissions(Access access) =>
                (access.HasFlag(Access.Delete) ? SharedAccessBlobPermissions.Delete : 0) |
                (access.HasFlag(Access.Get) ? SharedAccessBlobPermissions.Read : 0) |
                (access.HasFlag(Access.List) ? SharedAccessBlobPermissions.List : 0) |
                (access.HasFlag(Access.Put) ? SharedAccessBlobPermissions.Write : 0);

            public async Task<string> AcquireLease(string key, TimeSpan leaseTime)
            {
                var blob = _blobFileStore.GetBlob(key);

                try
                {
                    return await blob.AcquireLeaseAsync(leaseTime);
                }
                catch(StorageException stex)
                {
                    if (stex.RequestInformation.HttpStatusCode == 409)
                    {
                        throw new LeaseAlreadyAcquiredException();
                    }
                    else throw;
                }
            }

            public async Task RenewLease(string key, string leaseId)
            {
                var blob = _blobFileStore.GetBlob(key);

                await blob.RenewLeaseAsync(new AccessCondition { LeaseId = leaseId });
            }

            public async Task<string> ChangeLease(string key, string currentLeaseId)
            {
                var blob = _blobFileStore.GetBlob(key);

                return await blob.ChangeLeaseAsync(
                    $"{Guid.NewGuid():N}",
                    new AccessCondition { LeaseId = currentLeaseId });
            }

            public async Task ReleaseLease(string key, string leaseId)
            {
                var blob = _blobFileStore.GetBlob(key);

                await blob.ReleaseLeaseAsync(new AccessCondition { LeaseId = leaseId });
            }

            public async Task BreakLease(string key, TimeSpan breakReleaseTime)
            {
                var blob = _blobFileStore.GetBlob(key);

                await blob.BreakLeaseAsync(breakReleaseTime);
            }
        }
    }
}
