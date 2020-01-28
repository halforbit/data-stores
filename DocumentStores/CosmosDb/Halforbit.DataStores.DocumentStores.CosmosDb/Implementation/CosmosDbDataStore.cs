﻿using Halforbit.DataStores.DocumentStores.Interface;
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
using Newtonsoft.Json.Linq;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

// TODO:
// - Bulk transfer
// - Context SQL execution
// - Remove IDocument requirement for POCOs
// x Retry 429 and 503 with polly
// x Support JObject as TValue

namespace Halforbit.DataStores.DocumentStores.CosmosDb.Implementation
{
    public class CosmosDbDataStore<TKey, TValue> :
        IDataStore<TKey, TValue>
    {
        static readonly RetryPolicy _retryPolicy = Policy
            .Handle<CosmosException>(cex => cex.StatusCode == (HttpStatusCode)429)
            .Or<CosmosException>(cex => cex.StatusCode == HttpStatusCode.ServiceUnavailable)
            .WaitAndRetryAsync(
                retryCount: 5,
                sleepDurationProvider: (count, exception, context) =>
                {
                    return (exception as CosmosException)?.RetryAfter ?? TimeSpan.FromSeconds(Math.Pow(2, count));
                },
                onRetryAsync: (exception, timespan, count, context) => Task.CompletedTask);
        
        static readonly Regex _partitionKeyMatcher = new Regex(
            @"^\{(?<Key>[a-z0-9]+)(:.*?){0,1}\}\|", 
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        readonly Lazy<Container> _container;
        
        readonly string _connectionString;
        
        readonly string _databaseId;
        
        readonly string _containerId;
        
        readonly StringMap<TKey> _keyMap;

        readonly StringMap<TKey> _keyMapWithoutPartitionKey;

        readonly string _partitionKey;

        readonly IValidator<TKey, TValue> _validator;

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => _keyMap;

        public CosmosDbDataStore(
            string connectionString,
            string databaseId,
            string containerId,
            string keyMap,
            [Optional]IValidator<TKey, TValue> validator = null)
        {
            _connectionString = connectionString;
            
            _databaseId = databaseId;
            
            _containerId = containerId;

            _container = new Lazy<Container>(() => 
                new CosmosClient(
                    connectionString: connectionString)
                .GetContainer(
                    databaseId: databaseId,
                    containerId: containerId));

            _keyMap = keyMap;

            if (keyMap.Contains('|'))
            {
                var m = _partitionKeyMatcher.Match(keyMap);

                if (!m.Success)
                {
                    throw new ArgumentException("Unable to comprehend partition key in key map.");
                }

                _partitionKey = m.Groups["Key"].Value;

                if (typeof(IDocument).IsAssignableFrom(typeof(TValue)))
                {
                    var partitionKeyProperty = typeof(TValue).GetProperty(_partitionKey);

                    if (partitionKeyProperty == null)
                    {
                        throw new ArgumentException($"Type {typeof(TValue).Name} is missing partition key property '{_partitionKey}'");
                    }

                    if (partitionKeyProperty.PropertyType == typeof(Guid) ||
                        partitionKeyProperty.PropertyType == typeof(Guid?))
                    {
                        // Automagically set partition key guids to dashed

                        _keyMap = $"{{{_partitionKey}:D}}|{keyMap.Substring(m.Value.Length)}";
                    }
                }

                _keyMapWithoutPartitionKey = keyMap.Substring(m.Value.Length);
            }
            else
            {
                _keyMapWithoutPartitionKey = _keyMap;
            }

            _validator = validator;
        }

        public async Task<bool> Create(
            TKey key, 
            TValue value)
        {
            await ValidatePut(key, value).ConfigureAwait(false);

            var (partitionKey, documentId) = GetDocumentId(key);

            SetDocumentId(value, documentId);

            try
            {
                await Execute(() => _container.Value.CreateItemAsync(
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
            await ValidateDelete(key).ConfigureAwait(false);

            var (partitionKey, documentId) = GetDocumentId(key);

            try
            {
                await Execute(() => _container.Value
                    .DeleteItemAsync<TValue>(
                        id: documentId,
                        partitionKey: partitionKey != null ? 
                            new PartitionKey(partitionKey) : 
                            PartitionKey.None))
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
            var (partitionKey, documentId) = GetDocumentId(key);

            try
            {
                var item = await Execute(
                    () => _container.Value.ReadItemAsync<TValue>(
                        id: documentId,
                        partitionKey: partitionKey != null ? 
                            new PartitionKey(partitionKey) : 
                            PartitionKey.None))
                    .ConfigureAwait(false);

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
            var (partitionKey, idPrefix) = GetPartitionKeyAndIdPrefixFromPredicate(predicate);

            var query = _partitionKey != null ?
                $"SELECT c.id, c.{_partitionKey} AS pk FROM c" :
                "SELECT c.id FROM c";

            if (!string.IsNullOrWhiteSpace(idPrefix))
            {
                query += $@" WHERE STARTSWITH(c.id, ""{idPrefix}"")";
            }

            var iterator = _container.Value.GetItemQueryIterator<JObject>(
                queryDefinition: new QueryDefinition(query),
                requestOptions: new QueryRequestOptions
                {
                    PartitionKey = !string.IsNullOrWhiteSpace(partitionKey) ?
                        new PartitionKey(partitionKey) :
                        null as PartitionKey?,

                    MaxItemCount = -1
                });

            var results = new List<TKey>();

            while (iterator.HasMoreResults)
            {
                foreach (var item in await iterator.ReadNextAsync())
                {
                    var id = item.Value<string>("id");

                    var pk = item.Value<string>("pk");

                    var k = ParseDocumentId(pk, id);

                    results.Add(k);
                }
            }

            return results;
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListKeyValues(predicate)).Select(kv => kv.Value);
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var (partitionKey, idPrefix) = GetPartitionKeyAndIdPrefixFromPredicate(predicate);

            var query = "SELECT * FROM c";

            if (!string.IsNullOrWhiteSpace(idPrefix))
            {
                query += $@" WHERE STARTSWITH(c.id, ""{idPrefix}"")";
            }

            var results = new List<KeyValuePair<TKey, TValue>>();

            var iterator = _container.Value.GetItemQueryIterator<JObject>(
                queryDefinition: new QueryDefinition(query),
                requestOptions: new QueryRequestOptions
                {
                    PartitionKey = !string.IsNullOrWhiteSpace(partitionKey) ?
                        new PartitionKey(partitionKey) :
                        null as PartitionKey?,

                    MaxItemCount = -1
                });

            while (iterator.HasMoreResults)
            {
                foreach (var item in await iterator.ReadNextAsync())
                {
                    var id = item.Value<string>("id");

                    var pk = _partitionKey != null ? 
                        item.Value<string>(_partitionKey) : 
                        null;

                    var key = ParseDocumentId(pk, id);

                    results.Add(new KeyValuePair<TKey, TValue>( 
                        key,
                        item.ToObject<TValue>()));
                }
            }

            var filter = predicate?.Compile();

            if (filter != null)
            {
                return results
                    .Where(kv => filter(kv.Key))
                    .ToList();
            }

            return results;
        }

        public async Task<bool> Update(
            TKey key, 
            TValue value)
        {
            await ValidatePut(key, value);

            var (partitionKey, documentId) = GetDocumentId(key);

            try
            {
                SetDocumentId(value, documentId);

                await Execute(() => _container.Value.ReplaceItemAsync(
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

            var (partitionKey, documentId) = GetDocumentId(key);

            SetDocumentId(value, documentId);

            await Execute(() => _container.Value.UpsertItemAsync(
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

        public Task<bool> Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            throw new NotImplementedException();
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            if (!typeof(IDocument).IsAssignableFrom(typeof(TValue)))
            {
                throw new ArgumentException($"Type {typeof(TValue).Name} is not {nameof(IDocument)}.");
            }

            var qst = typeof(QuerySession<>).MakeGenericType(
                typeof(TKey), 
                typeof(TValue), 
                typeof(TValue));

            return Activator.CreateInstance(qst, this) as IQuerySession<TKey, TValue>;
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

        (string PartitionKey, string IdPrefix) GetPartitionKeyAndIdPrefixFromPredicate(
            Expression<Func<TKey, bool>> predicate)
        {
            var extracted = predicate != null ?
                new InvariantExtractor().ExtractInvariantDictionary(
                    predicate,
                    out _) :
                EmptyReadOnlyDictionary<string, object>.Instance;

            var partitionKey = default(string);

            if (_partitionKey != null)
            {
                var mapped = _keyMap.Map(
                    extracted,
                    allowPartialMap: true);

                partitionKey = mapped.Split('|')[0];
            }

            var idPrefix = _keyMapWithoutPartitionKey
                .Map(
                    extracted,
                    allowPartialMap: true)
                .Replace('/', '|');

            return (partitionKey, idPrefix);
        }

        static async Task<V> Execute<V>(Func<Task<V>> func) => await _retryPolicy.ExecuteAsync(func);
        
        (string PartitionKey, string DocumentId) GetDocumentId(TKey key)
        {
            var mapped = _keyMap.Map(key);

            var parts = mapped.Split('|');

            if (parts.Length == 2)
            {
                if (Guid.TryParse(parts[0], out var g))
                {
                    return ($"{g:D}", parts[1].Replace('/', '|'));
                }
                else
                {
                    return (parts[0], parts[1].Replace('/', '|'));
                }
            }
            else if(parts.Length == 1)
            {
                return (null, parts[0].Replace('/', '|'));
            }
            else
            {
                throw new ArgumentException("Unrecognized key format, too many '|' characters.");
            }
        }

        TKey ParseDocumentId(
            string partitionKey,
            string documentId)
        {
            if (partitionKey != null)
            {
                return _keyMap.Map($"{partitionKey}|{documentId.Replace('|', '/')}");
            }
            else
            {
                return _keyMap.Map(documentId.Replace('|', '/'));
            }
        }

        async Task ValidatePut(TKey key, TValue value)
        {
            if (_validator != null)
            {
                var validationErrors = await _validator.ValidatePut(key, value, _keyMap).ConfigureAwait(false);

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
                var validationErrors = await _validator.ValidateDelete(key, _keyMap).ConfigureAwait(false);

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

        static void SetDocumentId(TValue value, string documentId)
        {
            if (value is IDocument d)
            {
                d.Id = documentId;
            }
            else if (value is JObject j)
            {
                j["id"] = documentId;
            }
            else
            {
                throw new ArgumentException($"TValue {typeof(TValue).Name} is neither {nameof(IDocument)} nor {nameof(JObject)}.");
            }
        }

        class QuerySession<TResultValue> : IQuerySession<TKey, TResultValue>
            where TResultValue : IDocument
        {
            readonly CosmosDbDataStore<TKey, TValue> _dataStore;

            public QuerySession(CosmosDbDataStore<TKey, TValue> dataStore)
            {
                _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            }

            public void Dispose() { }

            public IQueryable<TResultValue> Query(
                Expression<Func<TKey, bool>> predicate = null)
            {
                var (partitionKey, idPrefix) = _dataStore.GetPartitionKeyAndIdPrefixFromPredicate(predicate);

                return _dataStore._container.Value
                    .GetItemLinqQueryable<TResultValue>(
                        allowSynchronousQueryExecution: true,
                        requestOptions: new QueryRequestOptions
                        {
                            PartitionKey = !string.IsNullOrWhiteSpace(partitionKey) ?
                                new PartitionKey(partitionKey) :
                                null as PartitionKey?,
                            
                            MaxItemCount = -1
                        })
                    .Where(e => e.Id.StartsWith(idPrefix));
            }
        }
    }
}
