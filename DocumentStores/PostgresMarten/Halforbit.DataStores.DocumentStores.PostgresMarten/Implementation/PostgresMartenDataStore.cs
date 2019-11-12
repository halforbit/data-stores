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
using Marten;
using Marten.Services;
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
        readonly DocumentStore _documentStore;

        readonly StringMap<TKey> _keyMap;

        readonly IValidator<TKey, TValue> _validator;

        public PostgresMartenDataStore(
            string connectionString,
            string keyMap,
            [Optional]IValidator<TKey, TValue> _validator = null)
        {
            var serializer = new JsonNetSerializer();

            serializer.Customize(c =>
            {
                c.TypeNameHandling = TypeNameHandling.None;
            });

            _documentStore = DocumentStore.For(c =>
            {
                c.Connection(connectionString);

                c.Schema.Include<MartenRegistry>();

                c.Serializer(serializer);
            });

            _keyMap = keyMap;

            this._validator = _validator;
        }

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => _keyMap;

        public async Task<bool> Create(TKey key, TValue value)
        {
            await ValidatePut(key, value);

            var id = value.Id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                if (!existing.IsDefaultValue())
                {
                    return false;
                }

                session.Insert(value);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }

            return true;
        }

        public async Task<bool> Delete(TKey key)
        {
            await ValidateDelete(key);

            var id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                if (existing.IsDefaultValue())
                {
                    return false;
                }

                session.Delete<TValue>(id);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }

            return true;
        }

        public async Task<bool> Exists(TKey key)
        {
            var id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                return !existing.IsDefaultValue();
            }
        }

        public async Task<TValue> Get(TKey key)
        {
            var id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                return session.Load<TValue>(id);
            }
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

        public IQuerySession<TKey, TValue> StartQuery()
        {
            return new QuerySession(this);
        }

        public async Task<bool> Update(TKey key, TValue value)
        {
            await ValidatePut(key, value);

            var id = value.Id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                if(existing.IsDefaultValue())
                {
                    return false;
                }

                session.Update(value);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }

            return true;
        }

        public async Task<bool> Upsert(TKey key, TValue value)
        {
            await ValidatePut(key, value);

            var id = value.Id = GetDocumentId(key);

            using (var session = _documentStore.LightweightSession())
            {
                var existing = session.Load<TValue>(id);

                session.Store(value);

                await session.SaveChangesAsync().ConfigureAwait(false);

                return !existing.IsDefaultValue();
            }
        }

        public Task<bool> Upsert(TKey key, Func<TValue, TValue> mutator)
        {
            throw new NotImplementedException();
        }

        public Task<bool> Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            throw new NotImplementedException();
        }

        string GetDocumentId(TKey key) => _keyMap.Map(key);

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
            if (_validator != null)
            {
                var validationErrors = await _validator.ValidateDelete(key, _keyMap);

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
