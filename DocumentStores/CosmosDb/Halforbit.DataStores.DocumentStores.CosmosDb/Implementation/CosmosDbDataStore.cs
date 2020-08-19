using Halforbit.DataStores.FileStores.Exceptions;
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
using System.Threading;
using System.Threading.Tasks;

// TODO:
// x Bulk transfer (handled internally by Cosmos nuget package)
// - Context SQL execution
// - Remove IDocument requirement for POCOs
// x Retry 429 and 503 with polly
// x Support JObject as TValue

namespace Halforbit.DataStores.DocumentStores.CosmosDb.Implementation
{
    public class CosmosDbDataStore<TKey, TValue> :
        IDataStore<TKey, TValue>
    {
        private const int BulkApiMinOpsPerPartition = 50;
        private const int BulkApiMaxOpsPerRequest = 100;
        
        static readonly AsyncRetryPolicy _retryPolicy = Policy
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
        
        readonly Lazy<Container> _bulkContainer;
        
        readonly string _connectionString;

        readonly string _databaseId;	

        readonly string _containerId;
        
        readonly StringMap<TKey> _keyMap;

        readonly StringMap<TKey> _keyMapWithoutPartitionKey;

        readonly string _partitionKey;

        readonly IValidator<TKey, TValue> _validator;

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => _keyMap;

        readonly IReadOnlyList<IObserver<TKey, TValue>> _typedObservers;

        readonly IReadOnlyList<IObserver> _untypedObservers;

        readonly IReadOnlyList<IMutator<TKey, TValue>> _typedMutators;

        readonly IReadOnlyList<IMutator> _untypedMutators;

        public CosmosDbDataStore(
            string connectionString,
            string databaseId,
            string containerId,
            string keyMap,
            [Optional]IValidator<TKey, TValue> validator = null,
            [Optional]IReadOnlyList<IObserver<TKey, TValue>> typedObservers = null,
            [Optional]IReadOnlyList<IObserver> untypedObservers = null,
            [Optional]IReadOnlyList<IMutator<TKey, TValue>> typedMutators = null,
            [Optional]IReadOnlyList<IMutator> untypedMutators = null)
        {
            _connectionString = connectionString;
            
            _databaseId = databaseId;
            
            _containerId = containerId;
            
            _container = new Lazy<Container>(() =>
                new CosmosClient(
                        connectionString: connectionString,
                        clientOptions: new CosmosClientOptions
                        {
                            AllowBulkExecution = false
                        })
                    .GetContainer(databaseId: databaseId,
                        containerId: containerId));

            _bulkContainer = new Lazy<Container>(() =>
                new CosmosClient(
                        connectionString: connectionString,
                        clientOptions: new CosmosClientOptions
                        {
                            AllowBulkExecution = true
                        })
                    .GetContainer(databaseId: databaseId,
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

            _typedObservers = typedObservers ?? EmptyReadOnlyList<IObserver<TKey, TValue>>.Instance;

            _untypedObservers = untypedObservers ?? EmptyReadOnlyList<IObserver>.Instance;

            _typedMutators = typedMutators ?? EmptyReadOnlyList<IMutator<TKey, TValue>>.Instance;

            _untypedMutators = untypedMutators ?? EmptyReadOnlyList<IMutator>.Instance;
        }

        public Task<bool> Create(
            TKey key, 
            TValue value)
        {
            var (partitionKey, documentId) = GetDocumentId(key);
            return CreateInternal(_container.Value, partitionKey, documentId, key, value);
        }
        
        private async Task<bool> CreateInternal(
            Container container,
            string partitionKey,
            string documentId, 
            TKey key,
            TValue value)
        {
            value = await MutatePut(key, value);

            await ValidatePut(key, value).ConfigureAwait(false);
            
            SetDocumentId(value, documentId);

            await ObserveBeforePut(key, value);

            try
            {
                await Execute(() => container.CreateItemAsync(
                    item: value,
                    partitionKey: GetCosmosPartitionKey(partitionKey))).ConfigureAwait(false);

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

        public Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Create(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            return BulkOperation(values, CreateInternal);
        }
        
        public Task<bool> Delete(TKey key)
        {
            var (partitionKey, documentId) = GetDocumentId(key);
            return DeleteInternal(_container.Value, partitionKey, documentId, key);
        }
        
        private async Task<bool> DeleteInternal(
            Container container,
            string partitionKey,
            string documentId,
            TKey key)
        {
            await ValidateDelete(key).ConfigureAwait(false);
            
            foreach (var observer in _typedObservers) await observer.BeforeDelete(key);

            foreach (var observer in _untypedObservers) await observer.BeforeDelete(key);
            
            try
            {
                await Execute(() => container
                        .DeleteItemAsync<TValue>(
                            id: documentId,
                            partitionKey: GetCosmosPartitionKey(partitionKey) ?? PartitionKey.None))
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
        
        public Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Delete(
            IEnumerable<TKey> keys)
        {
            return BulkOperation(keys, DeleteInternal);
        }

        public async Task<bool> Exists(TKey key)
        {
            return !(await Get(key).ConfigureAwait(false)).IsDefaultValue();
        }

        public Task<TValue> Get(TKey key)
        {
            var (partitionKey, documentId) = GetDocumentId(key);
            return GetInternal(_container.Value, partitionKey, documentId, key);
        }

        private async Task<TValue> GetInternal(
            Container container,
            string partitionKey,
            string documentId,
            TKey key)
        {
            try
            {
                var item = await Execute(
                        () => container.ReadItemAsync<TValue>(
                            id: documentId,
                            partitionKey: GetCosmosPartitionKey(partitionKey) ?? PartitionKey.None))
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

        public Task<IReadOnlyList<KeyValuePair<TKey, TValue>>> Get(
            IEnumerable<TKey> keys)
        {
            return BulkOperation(keys, GetInternal);
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
                    PartitionKey = GetCosmosPartitionKey(partitionKey),
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

                    if (k != null)
                    {
                        results.Add(k);
                    }
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
                    PartitionKey = GetCosmosPartitionKey(partitionKey),
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

                    if (key != null)
                    {
                        results.Add(new KeyValuePair<TKey, TValue>( 
                            key,
                            item.ToObject<TValue>()));
                    }
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

        public Task<bool> Update(
            TKey key, 
            TValue value)
        {
            var (partitionKey, documentId) = GetDocumentId(key);
            return UpdateInternal(_container.Value, partitionKey, documentId, key, value);
        }
        
        private async Task<bool> UpdateInternal(
            Container container,
            string partitionKey,
            string documentId,
            TKey key, 
            TValue value)
        {
            value = await MutatePut(key, value);

            await ValidatePut(key, value);
            
            await ObserveBeforePut(key, value);

            try
            {
                SetDocumentId(value, documentId);

                await Execute(() => container.ReplaceItemAsync(
                        item: value,
                        id: documentId,
                        partitionKey: GetCosmosPartitionKey(partitionKey)))
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

        public Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Update(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            return BulkOperation(values, UpdateInternal);
        }

        public Task Upsert(
            TKey key, 
            TValue value)
        {
            var (partitionKey, documentId) = GetDocumentId(key);
            return UpsertInternal(_container.Value, partitionKey, documentId, key, value);
        }

        private async Task<bool> UpsertInternal(
            Container container,
            string partitionKey,
            string documentId,
            TKey key,
            TValue value)
        {
            value = await MutatePut(key, value);

            await ValidatePut(key, value);
            
            SetDocumentId(value, documentId);

            await ObserveBeforePut(key, value);

            await Execute(() => _container.Value.UpsertItemAsync(
                item: value,
                partitionKey: GetCosmosPartitionKey(partitionKey))).ConfigureAwait(false);

            return true;
        }

        public Task Upsert(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            return BulkOperation(values, UpsertInternal);
        }

        public async Task Upsert(
            TKey key, 
            Func<TValue, TValue> mutator)
        {
            throw new NotImplementedException();
        }

        public Task Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            throw new NotImplementedException();
        }

        public IQueryable<TValue> Query(Expression<Func<TKey, bool>> predicate = null)
        {
            return StartQuery().Query(predicate);
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

        static PartitionKey? GetCosmosPartitionKey(
            string partitionKey) => !string.IsNullOrWhiteSpace(partitionKey) ? new PartitionKey(partitionKey) : (PartitionKey?) null;

        async Task<TValue> MutatePut(TKey key, TValue value)
        {
            foreach (var mutator in _typedMutators) value = await mutator.MutatePut(key, value);

            foreach (var mutator in _untypedMutators) value = (TValue)await mutator.MutatePut(key, value);

            return value;
        }

        async Task ObserveBeforePut(TKey key, TValue value)
        {
            foreach (var observer in _typedObservers) await observer.BeforePut(key, value);

            foreach (var observer in _untypedObservers) await observer.BeforePut(key, value);
        }

        public async Task<IEnumerable<TResult>> BatchQuery<TItem, TResult>(
            IEnumerable<TItem> items, 
            Func<IEnumerable<TItem>, IQueryable<TValue>, IQueryable<TResult>> query,
            Expression<Func<TKey, bool>> predicate = null,
            int batchSize = 500)
        {
            var tasks = items
                .Batch(batchSize)
                .Select(batch => Task.Run(() => query(batch, Query(predicate)).AsEnumerable()))
                .ToList();

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result);
        }
        
        private static async Task<TOut> OperationWrapper<TIn, TOut>(
            SemaphoreSlim semaphore,
            TIn input,
            Func<TIn, Task<TOut>> asyncMutator)
        {
            await semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await asyncMutator(input).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Release();
            }
        }
        
        private async Task<IReadOnlyList<KeyValuePair<TKey, TOut>>> BulkOperation<TOut>(
            IEnumerable<KeyValuePair<TKey, TValue>> values,
            Func<Container, string, string, TKey, TValue, Task<TOut>> operation)
        {
            var partitionGroups = GroupByPartition(values);
            var semaphore = new SemaphoreSlim(DataStoresConcurrency.MaxOperations);
            var tasks = new List<Task<KeyValuePair<TKey,TOut>>>();
            var bulkTasks = new List<Task<KeyValuePair<TKey, TOut>[]>>();
            
            foreach (var grp in partitionGroups)
            {
                foreach (var batch in grp.Batch(BulkApiMaxOpsPerRequest))
                {
                    var useBulk = batch.Count() > BulkApiMinOpsPerPartition;
                    if (useBulk)
                    {
                        bulkTasks.Add(OperationWrapper(semaphore, batch,
                            async b =>
                            {
                                var results = b.Select(async i => new KeyValuePair<TKey, TOut>(i.Key,
                                    await operation(_bulkContainer.Value, i.PartitionKey, i.DocumentId, i.Key, i.Value)));

                                return await Task.WhenAll(results);
                            }));
                    }
                    else
                    {
                        tasks.AddRange(batch.Select(item => OperationWrapper(semaphore, item,
                            async i =>
                                new KeyValuePair<TKey, TOut>(i.Key, await operation(_container.Value, i.PartitionKey, i.DocumentId, i.Key, i.Value)))));
                    }
                }
            }

            var singleWriteResults = await Task.WhenAll(tasks);
            var bulkWriteResults = await Task.WhenAll(bulkTasks);

            return bulkWriteResults.SelectMany(x => x)
                .Concat(singleWriteResults)
                .ToArray();
        }
        
        private async Task<IReadOnlyList<KeyValuePair<TKey, TOut>>> BulkOperation<TOut>(
            IEnumerable<TKey> values,
            Func<Container, string, string, TKey, Task<TOut>> operation)
        {
            var partitionGroups = GroupByPartition(values);
            var semaphore = new SemaphoreSlim(DataStoresConcurrency.MaxOperations);
            var tasks = new List<Task<KeyValuePair<TKey,TOut>>>();
            var bulkTasks = new List<Task<KeyValuePair<TKey, TOut>[]>>();
            
            foreach (var grp in partitionGroups)
            {
                foreach (var batch in grp.Batch(BulkApiMaxOpsPerRequest))
                {
                    var useBulk = batch.Count() > BulkApiMinOpsPerPartition;
                    if (useBulk)
                    {
                        bulkTasks.Add(OperationWrapper(semaphore, batch,
                            async b =>
                            {
                                var results = b.Select(async i => new KeyValuePair<TKey, TOut>(i.Key,
                                    await operation(_bulkContainer.Value, i.PartitionKey, i.DocumentId, i.Key)));

                                return await Task.WhenAll(results);
                            }));
                    }
                    else
                    {
                        tasks.AddRange(batch.Select(item => OperationWrapper(semaphore, item,
                            async i =>
                                new KeyValuePair<TKey, TOut>(i.Key, await operation(_container.Value, i.PartitionKey, i.DocumentId, i.Key)))));
                    }
                }
            }

            var singleWriteResults = await Task.WhenAll(tasks);
            var bulkWriteResults = await Task.WhenAll(bulkTasks);

            return bulkWriteResults.SelectMany(x => x)
                .Concat(singleWriteResults)
                .ToArray();
        }

        private IEnumerable<IGrouping<string, (TKey Key, TValue Value, string PartitionKey, string DocumentId)>> GroupByPartition(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            return values.Select(v =>
            {
                var (partitionKey, documentId) = GetDocumentId(v.Key);
                return (
                    Key: v.Key,
                    Value: v.Value,
                    PartitionKey: partitionKey,
                    DocumentId: documentId
                );
            }).GroupBy(x => x.PartitionKey);
        }
        
        private IEnumerable<IGrouping<string, (TKey Key, string PartitionKey, string DocumentId)>> GroupByPartition(
            IEnumerable<TKey> keys)
        {
            return keys.Select(k =>
            {
                var (partitionKey, documentId) = GetDocumentId(k);
                return (
                    Key: k,
                    PartitionKey: partitionKey,
                    DocumentId: documentId
                );
            }).GroupBy(x => x.PartitionKey);
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
