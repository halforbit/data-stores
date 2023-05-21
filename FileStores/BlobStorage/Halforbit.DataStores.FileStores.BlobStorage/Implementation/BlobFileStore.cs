using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Halforbit.Facets.Attributes;
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
        readonly Lazy<BlobContainerClient> _cloudBlobContainer;
        
        readonly string _connectionString;
        
        readonly string _containerName;
        
        readonly string _contentType;

        readonly string _contentEncoding;

        readonly Lazy<IFileStoreContext> _fileStoreContext;

        public BlobFileStore(
            string connectionString, 
            string containerName,
            [Optional]string contentType = null,
            [Optional]string contentEncoding = null)
        {
            _connectionString = connectionString;
            
            _containerName = containerName;
            
            _contentType = contentType;

            _contentEncoding = contentEncoding;

            _cloudBlobContainer = new Lazy<BlobContainerClient>(() =>
            {
                var blobServiceClient = new BlobServiceClient(_connectionString);

                var blobContainerClient = blobServiceClient.GetBlobContainerClient(_containerName);

                blobContainerClient.CreateIfNotExists();

                return blobContainerClient;
            });

            _fileStoreContext = new Lazy<IFileStoreContext>(() => new BlobFileStoreContext(
                this, 
                Access.Full));
        }

        public IFileStoreContext FileStoreContext => _fileStoreContext.Value;

        public async Task<bool> Delete(string path)
        {
            await GetBlob(path).DeleteIfExistsAsync().ConfigureAwait(false);

            return true;
        }

        public async Task<bool> Exists(string path) => await GetBlob(path).ExistsAsync().ConfigureAwait(false);

        public async Task<IEnumerable<string>> GetFiles(
            string pathPrefix, 
            string extension)
        {
            var results = new List<string>();

            var enumerator = _cloudBlobContainer.Value
                .GetBlobsAsync(prefix: pathPrefix)
                .GetAsyncEnumerator();

            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                results.Add(enumerator.Current.Name);
            }

            return results;
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path,
            bool getETag = false)
        {
            var response = await GetBlob(path).DownloadContentAsync().ConfigureAwait(false);

            return new FileStoreReadAllBytesResult(
                bytes: response.Value.Content.ToArray(),
                eTag: response.Value.Details.ETag.ToString());
        }

        public async Task<bool> ReadStream(
            string path, 
            Stream contents, 
            bool getETag = false)
        {
            if (getETag) throw new NotImplementedException("eTag based optimistic concurrency is not implemented.");

            await GetBlob(path).DownloadToAsync(contents).ConfigureAwait(false);

            return true;
        }

        public async Task<bool> WriteAllBytes(
            string path, 
            byte[] contents,
            string eTag = null)
        {
            try
            {
                await GetBlob(path)
                    .UploadAsync(
                        content: BinaryData.FromBytes(contents),
                        options: new BlobUploadOptions
                        {
                            Conditions = eTag != null ?
                                new BlobRequestConditions
                                {
                                    IfMatch = new ETag(eTag)
                                } :
                                null, 
                            HttpHeaders = new BlobHttpHeaders
                            {
                                ContentType = _contentType,
                                ContentEncoding = _contentEncoding
                            }
                        })
                    .ConfigureAwait(false);
            }
            catch (RequestFailedException rfex) 
                when (rfex.Status == (int)HttpStatusCode.PreconditionFailed) 
            {
                return false;
            }

            return true;
        }

        public async Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            if (eTag != null) throw new NotImplementedException("eTag based optimistic concurrency is not implemented for streams");

            await GetBlob(path)
                .UploadAsync(
                    content: contents,
                    options: new BlobUploadOptions
                    {
                        HttpHeaders = new BlobHttpHeaders
                        {
                            ContentType = _contentType,
                            ContentEncoding = _contentEncoding
                        }
                    })
                .ConfigureAwait(false);

            return true;
        }

        BlobClient GetBlob(string path) => _cloudBlobContainer.Value.GetBlobClient(path);

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

                if (await _blobFileStore.Exists(key).ConfigureAwait(false))
                {
                    var blob = _blobFileStore.GetBlob(key);

                    var response = await blob.GetPropertiesAsync();

                    return BlobPropertiesToEntityInfo(
                        name: blob.Name,
                        uri: blob.Uri.AbsoluteUri,
                        blobProperties: response.Value);
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

                if(await _blobFileStore.Exists(key).ConfigureAwait(false))
                {                                        
                    var properties = await blob.GetPropertiesAsync().ConfigureAwait(false);

                    var keyValues = properties.Value.Metadata as IReadOnlyDictionary<string, string>;

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

                return blob.GenerateSasUri(new BlobSasBuilder(
                    permissions: AccessToBlobContainerSasPermissions(access),
                    expiresOn: expiration));
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

                var properties = await blob.GetPropertiesAsync().ConfigureAwait(false);

                await blob.SetHttpHeadersAsync(new BlobHttpHeaders
                {
                    ContentType = entityInfo.ContentType,
                    ContentHash = properties.Value.ContentHash,
                    ContentEncoding = entityInfo.ContentEncoding,
                    ContentLanguage = properties.Value.ContentLanguage,
                    ContentDisposition = properties.Value.ContentDisposition,
                    CacheControl = properties.Value.CacheControl
                });
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

                var properties = await blob.GetPropertiesAsync();

                var metadata = properties.Value.Metadata;

                metadata.Clear();

                foreach (var kv in keyValues)
                {
                    metadata[kv.Key] = kv.Value;
                }

                await blob.SetMetadataAsync(metadata).ConfigureAwait(false);
            }

            void AssertAccess(Access access)
            {
                if ((_access & access) == Access.None)
                {
                    throw new AccessViolationException(
                        $"Repository does not have {access} access.");
                }
            }

            static EntityInfo BlobPropertiesToEntityInfo(
                string name,
                string uri,
                BlobProperties blobProperties) => new EntityInfo(
                    name: name,
                    lastModified: blobProperties.LastModified.UtcDateTime,
                    size: blobProperties.ContentLength,
                    absoluteUri: uri,
                    contentType: blobProperties.ContentType,
                    contentEncoding: blobProperties.ContentEncoding,
                    contentHash: Convert.ToBase64String(blobProperties.ContentHash),
                    leaseState: ConvertLeaseState(blobProperties.LeaseState),
                    leaseLocked: blobProperties.LeaseStatus == LeaseStatus.Locked);

            static LeaseState ConvertLeaseState(
                Azure.Storage.Blobs.Models.LeaseState s)
            {
                switch(s)
                {
                    case Azure.Storage.Blobs.Models.LeaseState.Available:
                        return LeaseState.Available;
                    case Azure.Storage.Blobs.Models.LeaseState.Breaking:
                        return LeaseState.Breaking;
                    case Azure.Storage.Blobs.Models.LeaseState.Broken:
                        return LeaseState.Broken;
                    case Azure.Storage.Blobs.Models.LeaseState.Expired:
                        return LeaseState.Expired;
                    case Azure.Storage.Blobs.Models.LeaseState.Leased:
                        return LeaseState.Leased;
                    default: throw new Exception($"Unhandled LeaseState of '{s}'.");
                }
            }

            static BlobContainerSasPermissions AccessToBlobContainerSasPermissions(Access access) =>
                (access.HasFlag(Access.Delete) ? BlobContainerSasPermissions.Delete : 0) |
                (access.HasFlag(Access.Get) ? BlobContainerSasPermissions.Read : 0) |
                (access.HasFlag(Access.List) ? BlobContainerSasPermissions.List : 0) |
                (access.HasFlag(Access.Put) ? BlobContainerSasPermissions.Write : 0);

            public async Task<string> AcquireLease(string key, TimeSpan leaseTime)
            {
                try
                {
                    return (await _blobFileStore
                        .GetBlob(key)
                        .GetBlobLeaseClient()
                        .AcquireAsync(leaseTime)
                        .ConfigureAwait(false))
                        .Value.LeaseId;
                }
                catch (RequestFailedException rfex) when (rfex.Status == 409)
                {
                    throw new LeaseAlreadyAcquiredException();
                }
            }

            public async Task RenewLease(string key, string leaseId)
            {
                await _blobFileStore
                    .GetBlob(key)
                    .GetBlobLeaseClient(leaseId)
                    .RenewAsync();
            }

            public async Task<string> ChangeLease(string key, string currentLeaseId)
            {
                return (await _blobFileStore
                    .GetBlob(key)
                    .GetBlobLeaseClient(currentLeaseId)
                    .ChangeAsync($"{Guid.NewGuid():N}")
                    .ConfigureAwait(false))
                    .Value.LeaseId;
            }

            public async Task ReleaseLease(string key, string leaseId)
            {
                await _blobFileStore
                    .GetBlob(key)
                    .GetBlobLeaseClient(leaseId)
                    .ReleaseAsync()
                    .ConfigureAwait(false);
            }

            public async Task BreakLease(string key, TimeSpan breakReleaseTime)
            {
                await _blobFileStore
                    .GetBlob(key)
                    .GetBlobLeaseClient()
                    .BreakAsync(breakReleaseTime)
                    .ConfigureAwait(false);
            }

            public Task<Uri> GetEntityUrl(string key)
            {
                var blob = _blobFileStore.GetBlob(key);

                return Task.FromResult(blob.Uri);
            }
        }
    }
}
