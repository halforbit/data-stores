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
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.BlobStorage.Implementation
{
    public class BlobFileStore : IFileStore
    {
        static readonly Regex _connectionStringAccountNameParser = new Regex(
            "AccountName=(?<AccountName>[^;]+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        readonly Lazy<BlobServiceClient> _blobServiceClient;

        readonly Lazy<BlobContainerClient> _blobContainerClient;
        
        readonly string _connectionString;
        
        readonly string _containerName;
        
        readonly string _contentType;

        readonly string _contentEncoding;

        readonly Lazy<string> _accountName;

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

            _blobServiceClient = new Lazy<BlobServiceClient>(() => new BlobServiceClient(_connectionString));

            _blobContainerClient = new Lazy<BlobContainerClient>(() =>
            {
                var blobContainerClient = _blobServiceClient.Value.GetBlobContainerClient(_containerName);

                blobContainerClient.CreateIfNotExistsAsync().Wait();

                return blobContainerClient;
            });

            _accountName = new Lazy<string>(() => _connectionStringAccountNameParser
                .Match(_connectionString)
                .Groups["AccountName"].Value); 

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

            var asyncPageable = _blobContainerClient.Value.GetBlobsAsync(
                traits: BlobTraits.None,
                prefix: pathPrefix);

            var asyncEnumerator = asyncPageable.GetAsyncEnumerator();

            while (await asyncEnumerator.MoveNextAsync().ConfigureAwait(false))
            {
                results.Add(asyncEnumerator.Current.Name);
            }

            return results;
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path,
            bool getETag = false)
        {
            var memoryStream = new MemoryStream();

            var blob = GetBlob(path);
            
            await blob.DownloadToAsync(
                destination: memoryStream).ConfigureAwait(false);

            return new FileStoreReadAllBytesResult(
                bytes: memoryStream.ToArray(),
                eTag: getETag ? (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag.ToString() : null);
        }

        public async Task<bool> ReadStream(
            string path, 
            Stream contents, 
            bool getETag = false)
        {
            if (getETag) throw new NotImplementedException("eTag based optimistic concurrency is not implemented.");

            var blob = GetBlob(path);

            await blob.DownloadToAsync(
                destination: contents).ConfigureAwait(false);

            return true;
        }

        public async Task<bool> WriteAllBytes(
            string path, 
            byte[] contents,
            string eTag = null)
        {
            var tasks = new List<Task>();

            var blob = GetBlob(path);

            if (eTag == null)
            {
                await blob
                    .UploadAsync(
                        content: new MemoryStream(contents),
                        options: new BlobUploadOptions
                        {
                            HttpHeaders = new BlobHttpHeaders
                            {
                                ContentType = _contentType,
                                ContentEncoding = _contentEncoding
                            }
                        })
                    .ConfigureAwait(false);
            }
            else
            {
                try
                {
                    await blob
                        .UploadAsync(
                            content: new MemoryStream(contents),
                            options: new BlobUploadOptions
                            { 
                                Conditions = new BlobRequestConditions 
                                { 
                                    IfMatch = new ETag(eTag) 
                                },
                                HttpHeaders = new BlobHttpHeaders
                                {
                                    ContentType = _contentType,
                                    ContentEncoding = _contentEncoding
                                }
                            })
                        .ConfigureAwait(false);
                }
                catch (RequestFailedException rfex)
                {
                    if (rfex.Status == (int)HttpStatusCode.PreconditionFailed)
                    {
                        return false;
                    }

                    throw;
                }
            }

            return true;
        }

        public async Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            if (eTag != null) throw new NotImplementedException("eTag based optimistic concurrency is not implemented for streams");

            var blob = GetBlob(path);

            await blob
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

        BlobClient GetBlob(string path) => _blobContainerClient.Value.GetBlobClient(path);

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
                    
                    var properties = (BlobProperties)await blob.GetPropertiesAsync().ConfigureAwait(false);

                    return BlobPropertiesToEntityInfo(blob, properties);
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
                    var properties = (BlobProperties)await blob.GetPropertiesAsync().ConfigureAwait(false);

                    var keyValues = properties.Metadata as IReadOnlyDictionary<string, string>;

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

                // This became more complicated.
                // https://stackoverflow.com/questions/59118346/how-to-get-a-shared-access-signature-on-a-blob-using-the-latest-azure-sdk-net-a

                var userDelegationKey = await _blobFileStore._blobServiceClient.Value
                    .GetUserDelegationKeyAsync(
                        startsOn: default,
                        expiresOn: expiration)
                    .ConfigureAwait(false);

                var sasBuilder = new BlobSasBuilder
                {
                    BlobContainerName = _blobFileStore._containerName,
                    BlobName = key,
                    Resource = "b",
                    StartsOn = DateTimeOffset.UtcNow,
                    ExpiresOn = new DateTimeOffset(expiration)
                };

                sasBuilder.SetPermissions(AccessToBlobSasPermissions(access));

                var accountName = _blobFileStore._accountName.Value;

                var sasToken = sasBuilder
                    .ToSasQueryParameters(userDelegationKey, accountName)
                    .ToString();
                
                var fullUri = new UriBuilder
                {
                    Scheme = "https",
                    Host = $"{accountName}.blob.core.windows.net",
                    Path = $"{_blobFileStore._containerName}/{key}",
                    Query = sasToken
                };

                return fullUri.Uri;
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

                await blob.SetHttpHeadersAsync(
                    new BlobHttpHeaders
                    {
                        ContentType = entityInfo.ContentType,
                        ContentEncoding = entityInfo.ContentEncoding
                    })
                    .ConfigureAwait(false);
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

                var metadata = new Dictionary<string, string>();

                foreach (var kv in keyValues)
                {
                    metadata[kv.Key] = kv.Value;
                }

                await blob.SetMetadataAsync(metadata).ConfigureAwait(false);
            }

            public async Task<string> AcquireLease(string key, TimeSpan leaseTime)
            {
                var blobLeaseClient = GetBlobLeaseClient(key);

                try
                {
                    return ((BlobLease)await blobLeaseClient
                        .AcquireAsync(duration: leaseTime)
                        .ConfigureAwait(false)).LeaseId;
                }
                catch(RequestFailedException rfex)
                {
                    if (rfex.Status == 409)
                    {
                        throw new LeaseAlreadyAcquiredException();
                    }
                    else throw;
                }
            }

            public async Task RenewLease(string key, string leaseId)
            {
                var blobLeaseClient = GetBlobLeaseClient(key, leaseId);

                await blobLeaseClient.RenewAsync().ConfigureAwait(false);
            }

            public async Task<string> ChangeLease(string key, string currentLeaseId)
            {
                var blobLeaseClient = GetBlobLeaseClient(key, currentLeaseId);

                return ((BlobLease)await blobLeaseClient
                    .ChangeAsync(proposedId: $"{Guid.NewGuid():N}")
                    .ConfigureAwait(false))
                    .LeaseId;
            }

            public async Task ReleaseLease(string key, string leaseId)
            {
                var blobLeaseClient = GetBlobLeaseClient(key, leaseId);

                await blobLeaseClient.ReleaseAsync().ConfigureAwait(false);
            }

            public async Task BreakLease(string key, TimeSpan breakReleaseTime)
            {
                var blobLeaseClient = GetBlobLeaseClient(key);

                await blobLeaseClient
                    .BreakAsync(breakPeriod: breakReleaseTime)
                    .ConfigureAwait(false);
            }

            public Task<Uri> GetEntityUrl(string key)
            {
                var blob = _blobFileStore.GetBlob(key);

                return Task.FromResult(blob.Uri);
            }

            void AssertAccess(Access access)
            {
                if ((_access & access) == Access.None)
                {
                    throw new AccessViolationException(
                        $"Repository does not have {access} access.");
                }
            }

            static EntityInfo BlobPropertiesToEntityInfo(BlobClient blobClient, BlobProperties blobProperties) => new EntityInfo(
                blobClient.Name,
                blobProperties.LastModified.UtcDateTime,
                blobProperties.ContentLength,
                blobClient.Uri.AbsoluteUri,
                blobProperties.ContentType,
                blobProperties.ContentEncoding,
                Convert.ToBase64String(blobProperties.ContentHash),
                ConvertLeaseState(blobProperties.LeaseState),
                blobProperties.LeaseStatus == LeaseStatus.Locked);

            static LeaseState ConvertLeaseState(
                Azure.Storage.Blobs.Models.LeaseState s)
            {
                switch (s)
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

            static BlobSasPermissions AccessToBlobSasPermissions(Access access) =>
                (access.HasFlag(Access.Delete) ? BlobSasPermissions.Delete : 0) |
                (access.HasFlag(Access.Get) ? BlobSasPermissions.Read : 0) |
                (access.HasFlag(Access.List) ? BlobSasPermissions.Read : 0) |
                (access.HasFlag(Access.Put) ? BlobSasPermissions.Write : 0);

            BlobLeaseClient GetBlobLeaseClient(string key, string leaseId = default) => _blobFileStore.GetBlob(key).GetBlobLeaseClient(leaseId);
        }
    }
}
