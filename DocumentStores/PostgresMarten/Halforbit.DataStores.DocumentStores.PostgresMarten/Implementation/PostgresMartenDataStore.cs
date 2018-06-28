using Halforbit.DataStores.DocumentStores.Interface;
using Halforbit.DataStores.Exceptions;
using Halforbit.DataStores.Interface;
using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.Extensions;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Marten;
using Marten.Services;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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

        readonly IDataActionValidator<TKey, TValue> _dataActionValidator;

        public PostgresMartenDataStore(
            string connectionString,
            string keyMap,
            [Optional]IDataActionValidator<TKey, TValue> dataActionValidator)
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

            _dataActionValidator = dataActionValidator;
        }

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public async Task<bool> Create(TKey key, TValue value)
        {
            ValidatePut(key, value);

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
            ValidateDelete(key);

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
            var extracted = new InvariantExtractor().ExtractInvariants(
                predicate,
                out Expression<Func<TKey, bool>> invariant);

            var keyPrefix = _keyMap.Map(
                extracted, 
                allowPartialMap: true);

            using (var session = _documentStore.QuerySession())
            {
                return session
                    .Query<TValue>()
                    .Where(v => v.Id.StartsWith(keyPrefix))
                    .ToList();
            }
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            return new QuerySession(this);
        }

        public async Task<bool> Update(TKey key, TValue value)
        {
            ValidatePut(key, value);

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
            ValidatePut(key, value);

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

        string GetDocumentId(TKey key) => _keyMap.Map(key);

        void ValidatePut(TKey key, TValue value)
        {
            var validationErrors = _dataActionValidator?.ValidatePut(key, value).ToList();

            if (validationErrors?.Any() ?? false)
            {
                throw new ValidationException(validationErrors);
            }
        }

        void ValidateDelete(TKey key)
        {
            var validationErrors = _dataActionValidator?.ValidateDelete(key).ToList();

            if (validationErrors?.Any() ?? false)
            {
                throw new ValidationException(validationErrors);
            }
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

            public IQueryable<TValue> Query(TKey partialKey = default(TKey))
            {
                return _querySession.Query<TValue>();
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
