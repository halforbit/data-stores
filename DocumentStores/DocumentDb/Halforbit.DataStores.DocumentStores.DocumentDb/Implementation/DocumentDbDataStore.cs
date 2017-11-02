using Halforbit.DataStores.DocumentStores.Interface;
using Halforbit.DataStores.Interface;
using Halforbit.ObjectTools.Extensions;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;           
using System.Net;
using System.Threading.Tasks;

namespace Halforbit.DataStores.DocumentStores.DocumentDb.Implementation
{
    public class DocumentDbDataStore<TKey, TValue> :
        IDataStore<TKey, TValue>
        where TValue : IDocument
    {
        readonly DocumentClient _documentClient;

        readonly string _database;

        readonly string _collection;

        readonly StringMap<TKey> _keyMap;

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public DocumentDbDataStore(
            string endpoint,
            string authKey,
            string database,
            string collection,
            string keyMap)
        {
            _documentClient = DocumentClientFactory.GetDocumentClient(
                new Uri(endpoint), 
                authKey);

            _database = database;

            _collection = collection;

            _keyMap = keyMap;

            CreateDatabaseIfNotExistsAsync().Wait();

            CreateCollectionIfNotExistsAsync().Wait();
        }

        public async Task<bool> Create(
            TKey key, 
            TValue value)
        {
            value.Id = GetDocumentId(key);

            try
            {
                await Execute(() => _documentClient.CreateDocumentAsync(
                    GetCollectionUri(), 
                    value));

                return true;
            }
            catch(DocumentClientException dce)
            {
                if(dce.StatusCode == HttpStatusCode.Conflict)
                {
                    return false;
                }

                throw;
            }
        }

        public async Task<bool> Delete(TKey key)
        {
            var documentId = GetDocumentId(key);

            try
            {
                await Execute(() => _documentClient.DeleteDocumentAsync(
                    GetDocumentUri(documentId)));

                return true;
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }
        }

        public async Task<bool> Exists(TKey key)
        {
            return !(await Get(key)).IsDefaultValue();
        }

        public async Task<TValue> Get(TKey key)
        {
            var documentId = GetDocumentId(key);

            try
            {
                Document document = await Execute(
                    () => _documentClient.ReadDocumentAsync(
                        GetDocumentUri(documentId)));

                return (TValue)(dynamic)document;
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    return default(TValue);
                }
                else
                {
                    throw;
                }
            }
        }

        public async Task<IEnumerable<TKey>> ListKeys(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListValues(predicate)).Select(v => ParseDocumentId(v.Id));
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var extracted = predicate != null ? 
                new InvariantExtractor().ExtractInvariants(
                    predicate,
                    out Expression<Func<TKey, bool>> invariant) : 
                default(TKey);

            var keyPrefix = _keyMap
                .Map(
                    extracted,
                    allowPartialMap: true)
                .Replace('/', '|');

            IQueryable<TValue> queryable = await Execute(
                () => Task.FromResult(_documentClient
                    .CreateDocumentQuery<TValue>(
                        GetCollectionUri(),
                        new FeedOptions
                        {
                            MaxItemCount = -1,

                            EnableCrossPartitionQuery = true
                        })
                    .Where(v => v.Id.StartsWith(keyPrefix))));

            var documentQuery = queryable.AsDocumentQuery();

            var results = new List<TValue>();

            while (documentQuery.HasMoreResults)
            {
                foreach (var result in documentQuery.ExecuteNextAsync<TValue>().Result)
                {
                    results.Add(result);
                }
            }

            var keyValues = results.Select(o => new KeyValuePair<TKey, TValue>(
                ParseDocumentId(o.Id),
                o));

            var filter = predicate?.Compile();

            if(filter != null)
            {
                keyValues = keyValues.Where(kv => filter(kv.Key));
            }

            return keyValues.Select(kv => kv.Value);
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListValues(predicate))
                .Select(v => new KeyValuePair<TKey, TValue>(ParseDocumentId(v.Id), v));
        }

        public async Task<bool> Update(
            TKey key, 
            TValue value)
        {
            var documentId = GetDocumentId(key);

            try
            {
                value.Id = GetDocumentId(key);

                await Execute(() => _documentClient.ReplaceDocumentAsync(
                    GetDocumentUri(documentId), 
                    value));

                return true;
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }
        }

        public async Task<bool> Upsert(
            TKey key, 
            TValue value)
        {
            value.Id = GetDocumentId(key);

            await Execute(() => _documentClient.UpsertDocumentAsync(
                GetCollectionUri(), 
                value));

            return true;
        }

        public async Task<bool> Upsert(
            TKey key, 
            Func<TValue, TValue> mutator)
        {
            throw new NotImplementedException();
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            return new QuerySession(this);
        }

        class QuerySession : IQuerySession<TKey, TValue>
        {
            readonly DocumentDbDataStore<TKey, TValue> _dataStore;

            public QuerySession(DocumentDbDataStore<TKey, TValue> dataStore)
            {
                _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            }

            public void Dispose() { }

            public IQueryable<TValue> Query(TKey partialKey = default(TKey))
            {
                var keyPrefix = _dataStore.
                    _keyMap.Map(
                        partialKey,
                        allowPartialMap: true)
                    .Replace('/', '|');

                return Execute(
                    () => Task.FromResult(_dataStore._documentClient
                        .CreateDocumentQuery<TValue>(
                            _dataStore.GetCollectionUri(),
                            new FeedOptions
                            {
                                MaxItemCount = -1,

                                EnableCrossPartitionQuery = true
                            })
                        .Where(e => e.Id.StartsWith(keyPrefix)))).Result;
            }
        }

        async Task CreateDatabaseIfNotExistsAsync()
        {
            try
            {
                await _documentClient.ReadDatabaseAsync(GetDatabaseUri());
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    await _documentClient.CreateDatabaseAsync(new Database
                    {
                        Id = _database
                    });
                }
                else
                {
                    throw;
                }
            }
        }

        async Task CreateCollectionIfNotExistsAsync()
        {
            try
            {
                await _documentClient.ReadDocumentCollectionAsync(GetCollectionUri());
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    await _documentClient.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(_database),
                        new DocumentCollection
                        {
                            Id = _collection
                        },
                        new RequestOptions
                        {
                            OfferThroughput = 1000
                        });
                }
                else
                {
                    throw;
                }
            }
        }

        static async Task<V> Execute<V>(
            Func<Task<V>> func)
        {
            TimeSpan sleepTime = TimeSpan.Zero;

            while (true)
            {
                try
                {
                    return await func();
                }
                catch (DocumentClientException de)
                {
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }

                    sleepTime = de.RetryAfter;
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    var de = (DocumentClientException)ae.InnerException;

                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }

                    sleepTime = de.RetryAfter;
                }

                await Task.Delay(sleepTime);
            }
        }

        Uri GetDatabaseUri() => UriFactory.CreateDatabaseUri(_database);

        Uri GetCollectionUri() => UriFactory.CreateDocumentCollectionUri(
            _database,
            _collection);

        Uri GetDocumentUri(string documentId) => UriFactory.CreateDocumentUri(
            _database,
            _collection,
            documentId);

        string GetDocumentId(TKey key) => _keyMap.Map(key).Replace('/', '|');

        TKey ParseDocumentId(string id) => _keyMap.Map(id.Replace('|', '/'));
    }
}
