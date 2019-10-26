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
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Halforbit.DataStores.DocumentStores.CosmosDb.Implementation
{
    public class CosmosDbDataStore<TKey, TValue> :
        IDataStore<TKey, TValue>
        where TValue : IDocument
    {
        readonly Container _container;

        readonly StringMap<TKey> _keyMap;

        readonly IValidator<TKey, TValue> _validator;

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => _keyMap;

        public CosmosDbDataStore(
            string endpoint,
            string authKey,
            string databaseId,
            string containerId,
            string keyMap,
            [Optional]IValidator<TKey, TValue> validator = null)
        {
            _container = 
                new CosmosClient(
                    accountEndpoint: endpoint,
                    authKeyOrResourceToken: authKey)
                .GetContainer(
                    databaseId: databaseId, 
                    containerId: containerId);

            _keyMap = keyMap;

            _validator = validator;
        }

        public async Task<bool> Create(
            TKey key, 
            TValue value)
        {
            await ValidatePut(key, value);

            value.Id = GetDocumentId(key);

            try
            {
                await Execute(() => _container.CreateItemAsync(
                    item: value)).ConfigureAwait(false);

                return true;
            }
            catch (CosmosException ce)
            {
                if (ce.StatusCode == HttpStatusCode.Conflict)
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
                await Execute(() => _container
                    .DeleteItemAsync<TValue>(
                        id: documentId,
                        partitionKey: PartitionKey.None))
                    .ConfigureAwait(false);

                return true;
            }
            catch (CosmosException ce)
            {
                if (ce.StatusCode == HttpStatusCode.NotFound)
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
                var item = await Execute(
                    () => _container.ReadItemAsync<TValue>(
                        id: documentId,
                        partitionKey: PartitionKey.None)).ConfigureAwait(false);

                return item;
            }
            catch (CosmosException ce)
            {
                if (ce.StatusCode == HttpStatusCode.NotFound)
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

            var queryable = _container
                .GetItemLinqQueryable<TValue>(
                    allowSynchronousQueryExecution: true,
                    requestOptions: new QueryRequestOptions
                    {
                        MaxItemCount = -1 
                    })
                .Where(v => v.Id.StartsWith(keyPrefix));

            var results = queryable.ToList();

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

                await Execute(() => _container.ReplaceItemAsync(
                    item: value,
                    id: documentId)).ConfigureAwait(false);

                return true;
            }
            catch (CosmosException ce)
            {
                if (ce.StatusCode == HttpStatusCode.NotFound)
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

            await Execute(() => _container.UpsertItemAsync(
                item: value,
                partitionKey: default)).ConfigureAwait(false);

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
            readonly CosmosDbDataStore<TKey, TValue> _dataStore;

            public QuerySession(CosmosDbDataStore<TKey, TValue> dataStore)
            {
                _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            }

            public void Dispose() { }

            public IQueryable<TValue> Query(
                Expression<Func<TKey, bool>> predicate = null)
            {
                var memberValues = EmptyReadOnlyDictionary<string, object>.Instance as
                    IReadOnlyDictionary<string, object>;

                if (predicate != null)
                {
                    memberValues = new InvariantExtractor().ExtractInvariantDictionary(
                        predicate,
                        out var invariantExpression);
                }

                var keyPrefix = _dataStore
                    .EvaluatePath(memberValues, allowPartialMap: true)
                    .Replace('/', '|');

                return _dataStore._container
                    .GetItemLinqQueryable<TValue>(
                        allowSynchronousQueryExecution: true,
                        requestOptions: new QueryRequestOptions
                        {
                            MaxItemCount = -1
                        })
                    .Where(e => e.Id.StartsWith(keyPrefix));
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
                catch (CosmosException ce)
                {
                    if ((int)ce.StatusCode != 429)
                    {
                        throw;
                    }

                    sleepTime = ce.RetryAfter ?? TimeSpan.FromSeconds(10);
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is CosmosException))
                    {
                        throw;
                    }

                    var ce = (CosmosException)ae.InnerException;

                    if ((int)ce.StatusCode != 429)
                    {
                        throw;
                    }

                    sleepTime = ce.RetryAfter ?? TimeSpan.FromSeconds(10);
                }

                await Task.Delay(sleepTime).ConfigureAwait(false);
            }
        }

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
