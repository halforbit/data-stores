using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface IDataStore<TKey, TValue>
    {
        IDataStoreContext<TKey> Context { get; }

        Task<bool> Exists(TKey key);

        Task<bool> Create(TKey key, TValue value);

        Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Create(IEnumerable<KeyValuePair<TKey, TValue>> values);
        
        Task<bool> Delete(TKey key);

        Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Delete(IEnumerable<TKey> keys);

        Task<TValue> Get(TKey key);

        Task<IReadOnlyList<KeyValuePair<TKey,TValue>>> Get(IEnumerable<TKey> keys);

        Task<bool> GetToStream(TKey key, Stream stream);

        Task<IEnumerable<TKey>> ListKeys(Expression<Func<TKey, bool>> predicate = null);

        Task<IEnumerable<TValue>> ListValues(Expression<Func<TKey, bool>> predicate = null);

        Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(Expression<Func<TKey, bool>> predicate = null);
        
        Task<bool> Update(TKey key, TValue value);
        
        Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Update(IEnumerable<KeyValuePair<TKey, TValue>> values);

        Task Upsert(TKey key, TValue value);
        
        Task Upsert(IEnumerable<KeyValuePair<TKey, TValue>> values);

        Task Upsert(TKey key, Func<TValue, TValue> mutator);

        Task Upsert(TKey key, Func<TValue, Task<TValue>> mutator);

        IQueryable<TValue> Query(Expression<Func<TKey, bool>> predicate = null);

        IQuerySession<TKey, TValue> StartQuery();

        Task<IEnumerable<TResult>> BatchQuery<TItem, TResult>(
            IEnumerable<TItem> items,
            Func<IEnumerable<TItem>, IQueryable<TValue>, IQueryable<TResult>> query,
            Expression<Func<TKey, bool>> predicate = null,
            int batchSize = 500);

        IStringMap<TKey> KeyMap { get; }
    }

    public interface IDataStore<TValue>
    {
        IDataStoreContext<object> Context { get; }

        Task<bool> Exists();

        Task<bool> Create(TValue value);

        Task<bool> Delete();

        Task<TValue> Get();

        Task<bool> GetToStream(Stream stream);

        Task<bool> Update(TValue value);

        Task Upsert(TValue value);

        Task Upsert(Func<TValue, TValue> mutator);

        Task Upsert(Func<TValue, Task<TValue>> mutator);

        IStringMap<object> Map { get; }
    }
}
