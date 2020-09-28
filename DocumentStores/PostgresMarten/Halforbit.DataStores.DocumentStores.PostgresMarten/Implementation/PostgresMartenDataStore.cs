using Halforbit.DataStores.FileStores.Exceptions;
using Halforbit.DataStores.Internal;
using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.Collections;
using Halforbit.ObjectTools.Extensions;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Interface;
using Marten;
using Marten.Services;
using Marten.Util;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.DocumentStores.PostgresMarten
{
    public class PostgresMartenDataStore<TKey, TValue> :
        IDataStore<TKey, TValue>
        where TValue : IDocument
    {        
        readonly string _connectionString;
        
        readonly StringMap<TKey> _keyMap;

        readonly IValidator<TKey, TValue> _validator;

        readonly IReadOnlyList<IObserver<TKey, TValue>> _typedObservers;

        readonly IReadOnlyList<IObserver> _untypedObservers;

        readonly IReadOnlyList<IMutator<TKey, TValue>> _typedMutators;

        readonly IReadOnlyList<IMutator> _untypedMutators;

        readonly DocumentStore _documentStore;

        public PostgresMartenDataStore(
            string connectionString,
            string keyMap,
            [Optional]IValidator<TKey, TValue> validator = null,
            [Optional]IReadOnlyList<IObserver<TKey, TValue>> typedObservers = null,
            [Optional]IReadOnlyList<IObserver> untypedObservers = null,
            [Optional]IReadOnlyList<IMutator<TKey, TValue>> typedMutators = null,
            [Optional]IReadOnlyList<IMutator> untypedMutators = null)
        {
            _connectionString = connectionString;

            var serializer = new JsonNetSerializer();

            serializer.Customize(c =>
            {
                c.TypeNameHandling = TypeNameHandling.None;
            });

            _documentStore = DocumentStore.For(c =>
            {
                c.Connection(_connectionString);

                c.Schema.Include<MartenRegistry>();

                c.Serializer(serializer);
            });

            _keyMap = keyMap;

            _validator = validator;

            _typedObservers = typedObservers ?? EmptyReadOnlyList<IObserver<TKey, TValue>>.Instance;

            _untypedObservers = untypedObservers ?? EmptyReadOnlyList<IObserver>.Instance;

            _typedMutators = typedMutators ?? EmptyReadOnlyList<IMutator<TKey, TValue>>.Instance;

            _untypedMutators = untypedMutators ?? EmptyReadOnlyList<IMutator>.Instance;
        }

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => _keyMap;

        public async Task<bool> Create(TKey key, TValue value)
        {
            key.ThrowIfKeyIsDefaultValue();

            value = await MutatePut(key, value).ConfigureAwait(false);

            await ValidatePut(key, value).ConfigureAwait(false);

            var id = value.Id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                if (!existing.IsDefaultValue())
                {
                    return false;
                }

                await ObserveBeforePut(key, value).ConfigureAwait(false);

                session.Insert(value);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }

            return true;
        }

        public Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Create(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            foreach(var kv in values) kv.Key.ThrowIfKeyIsDefaultValue();

            return this.BulkCreate(values);
        }

        public async Task<bool> Delete(TKey key)
        {
            key.ThrowIfKeyIsDefaultValue();

            await ValidateDelete(key).ConfigureAwait(false);

            var id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                if (existing.IsDefaultValue())
                {
                    return false;
                }

                foreach (var observer in _typedObservers)
                    await observer.BeforeDelete(key).ConfigureAwait(false);

                foreach (var observer in _untypedObservers)
                    await observer.BeforeDelete(key).ConfigureAwait(false);

                session.Delete<TValue>(id);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }

            return true;
        }

        public Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Delete(
            IEnumerable<TKey> keys)
        {
            foreach (var k in keys) k.ThrowIfKeyIsDefaultValue();

            return this.BulkDelete(keys);
        }

        public async Task<bool> Exists(TKey key)
        {
            key.ThrowIfKeyIsDefaultValue();

            var id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                return !existing.IsDefaultValue();
            }
        }

        public async Task<TValue> Get(TKey key)
        {
            key.ThrowIfKeyIsDefaultValue();

            var id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                return session.Load<TValue>(id);
            }
        }

        public Task<IReadOnlyList<KeyValuePair<TKey, TValue>>> Get(
            IEnumerable<TKey> keys)
        {
            foreach (var k in keys) k.ThrowIfKeyIsDefaultValue();

            return this.BulkGet(keys);
        }

        public async Task<IEnumerable<TKey>> ListKeys(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListValues(predicate).ConfigureAwait(false))
                .Select(v => _keyMap.Map(v.Id));
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListValues(predicate).ConfigureAwait(false))
                .Select(v => new KeyValuePair<TKey, TValue>(_keyMap.Map(v.Id), v));
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var extracted = predicate != null ? 
                new InvariantExtractor().ExtractInvariantDictionary(
                    predicate,
                    out Expression<Func<TKey, bool>> invariant) : 
                EmptyReadOnlyDictionary<string, object>.Instance;

            var keyPrefix = _keyMap.Map(
                extracted, 
                allowPartialMap: true);

            using (var session = _documentStore.QuerySession())
            {
                var queryable = session.Query<TValue>() as IQueryable<TValue>;

                if(!string.IsNullOrWhiteSpace(keyPrefix))
                {
                    queryable = queryable.Where(v => v.Id.StartsWith(keyPrefix));
                }
                
                return queryable.ToList();
            }
        }

        public IQueryable<TValue> Query(Expression<Func<TKey, bool>> predicate = null)
        {
            throw new NotSupportedException("Postgres/Marten requires a querying session. Use StartQuery instead.");
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            return new QuerySession(this);
        }

        public async Task<bool> Update(TKey key, TValue value)
        {
            key.ThrowIfKeyIsDefaultValue();

            value = await MutatePut(key, value).ConfigureAwait(false);

            await ValidatePut(key, value).ConfigureAwait(false);

            var id = value.Id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                if(existing.IsDefaultValue())
                {
                    return false;
                }

                await ObserveBeforePut(key, value).ConfigureAwait(false);

                session.Update(value);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }

            return true;
        }

        public Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Update(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            foreach (var kv in values) kv.Key.ThrowIfKeyIsDefaultValue();

            return this.BulkUpdate(values);
        }

        public async Task Upsert(TKey key, TValue value)
        {
            key.ThrowIfKeyIsDefaultValue();

            value = await MutatePut(key, value).ConfigureAwait(false);

            await ValidatePut(key, value).ConfigureAwait(false);

            var id = value.Id = GetDocumentId(key);

            await ObserveBeforePut(key, value).ConfigureAwait(false);

            using (var session = _documentStore.LightweightSession())
            {
                session.Store(value);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }
        }

        public Task Upsert(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            foreach (var kv in values) kv.Key.ThrowIfKeyIsDefaultValue();
            
            return this.BulkUpsert(values);
        }

        public Task Upsert(TKey key, Func<TValue, TValue> mutator)
        {
            key.ThrowIfKeyIsDefaultValue();
            
            throw new NotImplementedException();
        }

        public Task Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            key.ThrowIfKeyIsDefaultValue();
            
            throw new NotImplementedException();
        }

        string GetDocumentId(TKey key) => _keyMap.Map(key);

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
            if (_validator != null)
            {
                var validationErrors = await _validator.ValidateDelete(key, _keyMap).ConfigureAwait(false);

                if (validationErrors?.Any() ?? false)
                {
                    throw new ValidationException(validationErrors);
                }
            }
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

        public Task<bool> GetToStream(TKey key, Stream stream)
        {
            key.ThrowIfKeyIsDefaultValue();

            throw new NotImplementedException();
        }

        async Task<TValue> MutatePut(TKey key, TValue value)
        {
            foreach (var mutator in _typedMutators)
                value = await mutator.MutatePut(key, value).ConfigureAwait(false);

            foreach (var mutator in _untypedMutators) 
                value = (TValue)await mutator.MutatePut(key, value).ConfigureAwait(false);

            return value;
        }

        async Task ObserveBeforePut(TKey key, TValue value)
        {
            foreach (var observer in _typedObservers)
                await observer.BeforePut(key, value).ConfigureAwait(false);

            foreach (var observer in _untypedObservers)
                await observer.BeforePut(key, value).ConfigureAwait(false);
        }

        public Task<IEnumerable<TResult>> BatchQuery<TItem, TResult>(
            IEnumerable<TItem> items, 
            Func<IEnumerable<TItem>, IQueryable<TValue>, IQueryable<TResult>> query,
            Expression<Func<TKey, bool>> predicate = null,
            int batchSize = 500)
        {
            throw new NotImplementedException();
        }

        class QuerySession : IQuerySession<TKey, TValue>
        {
            readonly PostgresMartenDataStore<TKey, TValue> _dataStore;

            readonly IQuerySession _querySession;

            public QuerySession(PostgresMartenDataStore<TKey, TValue> dataStore)
            {
                _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));

                _querySession = _dataStore._documentStore.QuerySession();
            }

            public void Dispose()
            {
                _querySession.Dispose();
            }

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

                var queryable = _querySession.Query<TValue>() as IQueryable<TValue>;

                var keyPrefix = _dataStore.EvaluatePath(
                    memberValues, 
                    allowPartialMap: true);
                
                if(!string.IsNullOrWhiteSpace(keyPrefix))
                {
                    queryable = queryable.Where(e => e.Id.StartsWith(keyPrefix));
                }

                return queryable;
            }
        }

        class MartenRegistry : Marten.MartenRegistry
        {
            public MartenRegistry()
            {
                For<TValue>().GinIndexJsonData();
            }
        }
    }
}
