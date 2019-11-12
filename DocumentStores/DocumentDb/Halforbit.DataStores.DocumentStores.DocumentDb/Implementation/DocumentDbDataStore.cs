using Halforbit.DataStores.DocumentStores.Interface;
using Halforbit.DataStores.FileStores.Exceptions;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.Validation.Exceptions;
using Halforbit.DataStores.Validation.Interface;
using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.Collections;
using Halforbit.ObjectTools.Extensions;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Interface;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.IO;
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

        readonly IValidator<TKey, TValue> _validator;

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => _keyMap;

        public DocumentDbDataStore(
            string endpoint,
            string authKey,
            string database,
            string collection,
            string keyMap,
            [Optional]IValidator<TKey, TValue> validator = null)
        {
            _documentClient = DocumentClientFactory.GetDocumentClient(
                new Uri(endpoint), 
                authKey);

            _database = database;

            _collection = collection;

            _keyMap = keyMap;

            _validator = validator;

            // Disabled these because they cause a deadlock in ASP.NET.
            
            // Don't re-enable without testing that ASP.NET controller 
            // use works correctly.

            //CreateDatabaseIfNotExistsAsync().Wait();

            //CreateCollectionIfNotExistsAsync().Wait();
        }

        public async Task<bool> Create(
            TKey key, 
            TValue value)
        {
            await ValidatePut(key, value);

            value.Id = GetDocumentId(key);

            try
            {
                await Execute(() => _documentClient.CreateDocumentAsync(
                    GetCollectionUri(),
                    value)).ConfigureAwait(false);

                return true;
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.Conflict)
                {
                    return false;
                }

                throw;
            }
        }

        public async Task<bool> Delete(TKey key)
        {
            await ValidateDelete(key);

            var documentId = GetDocumentId(key);

            try
            {
                await Execute(() => _documentClient.DeleteDocumentAsync(
                    GetDocumentUri(documentId))).ConfigureAwait(false);

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
            return !(await Get(key).ConfigureAwait(false)).IsDefaultValue();
        }

        public async Task<TValue> Get(TKey key)
        {
            var documentId = GetDocumentId(key);

            try
            {
                Document document = await Execute(
                    () => _documentClient.ReadDocumentAsync(
                        GetDocumentUri(documentId))).ConfigureAwait(false);

                return (TValue)(dynamic)document;
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    return default;
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
            return (await ListValues(predicate).ConfigureAwait(false)).Select(v => ParseDocumentId(v.Id));
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var extracted = predicate != null ? 
                new InvariantExtractor().ExtractInvariantDictionary(
                    predicate,
                    out Expression<Func<TKey, bool>> invariant) : 
                EmptyReadOnlyDictionary<string, object>.Instance;

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
                    .Where(v => v.Id.StartsWith(keyPrefix)))).ConfigureAwait(false);

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
            return (await ListValues(predicate).ConfigureAwait(false))
                .Select(v => new KeyValuePair<TKey, TValue>(ParseDocumentId(v.Id), v));
        }

        public async Task<bool> Update(
            TKey key, 
            TValue value)
        {
            await ValidatePut(key, value);

            var documentId = GetDocumentId(key);

            try
            {
                value.Id = GetDocumentId(key);

                await Execute(() => _documentClient.ReplaceDocumentAsync(
                    GetDocumentUri(documentId), 
                    value)).ConfigureAwait(false);

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
            await ValidatePut(key, value);

            value.Id = GetDocumentId(key);

            await Execute(() => _documentClient.UpsertDocumentAsync(
                GetCollectionUri(), 
                value)).ConfigureAwait(false);

            return true;
        }

        public async Task<bool> Upsert(
            TKey key, 
            Func<TValue, TValue> mutator)
        {
            throw new NotImplementedException();
        }

        public Task<bool> Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            throw new NotImplementedException();
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            return new QuerySession(this);
        }

        string EvaluatePath(
            IReadOnlyDictionary<string, object> memberValues,
            bool allowPartialMap = false)
        {
            try
            {
                return _keyMap.Map(memberValues, allowPartialMap);
            }
            catch (ArgumentNullException ex)
            {
                throw new IncompleteKeyException(
                    $"Path for {typeof(TValue).Name} could not be evaluated " +
                    $"because key {typeof(TKey).Name} was missing a value for {ex.ParamName}.");
            }
        }

        class QuerySession : IQuerySession<TKey, TValue>
        {
            readonly DocumentDbDataStore<TKey, TValue> _dataStore;

            public QuerySession(DocumentDbDataStore<TKey, TValue> dataStore)
            {
                _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            }

            public void Dispose() { }

            public IQueryable<TValue> Query(
                Expression<Func<TKey, bool>> predicate = null)
            {
                var memberValues = EmptyReadOnlyDictionary<string, object>.Instance as 
                    IReadOnlyDictionary<string, object>;

                if(predicate != null)
                {
                    memberValues = new InvariantExtractor().ExtractInvariantDictionary(
                        predicate,
                        out var invariantExpression);
                }

                var keyPrefix = _dataStore
                    .EvaluatePath(memberValues, allowPartialMap: true)
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
                await _documentClient.ReadDatabaseAsync(GetDatabaseUri()).ConfigureAwait(false);
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    await _documentClient
                        .CreateDatabaseAsync(new Database
                        {
                            Id = _database
                        })
                        .ConfigureAwait(false);
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
                await _documentClient.ReadDocumentCollectionAsync(GetCollectionUri()).ConfigureAwait(false);
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    await _documentClient
                        .CreateDocumentCollectionAsync(
                            UriFactory.CreateDatabaseUri(_database),
                            new DocumentCollection
                            {
                                Id = _collection
                            },
                            new RequestOptions
                            {
                                OfferThroughput = 1000
                            })
                        .ConfigureAwait(false);
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
                    return await func().ConfigureAwait(false);
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

                await Task.Delay(sleepTime).ConfigureAwait(false);
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

        async Task ValidatePut(TKey key, TValue value)
        {
            if (_validator != null)
            {
                var validationErrors = await _validator.ValidatePut(key, value, _keyMap);

                if (validationErrors?.Any() ?? false)
                {
                    throw new ValidationException(validationErrors);
                }
            }
        }

        async Task ValidateDelete(TKey key)
        {
            if(_validator != null)
            {
                var validationErrors = await _validator.ValidateDelete(key, _keyMap);

                if (validationErrors?.Any() ?? false)
                {
                    throw new ValidationException(validationErrors);
                }
            }
        }

        public Task<bool> GetToStream(TKey key, Stream stream)
        {
            throw new NotImplementedException();
        }
    }
}
