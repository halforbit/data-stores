using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.Interface
{
    public interface IDataStore<TKey, TValue>
    {
        IDataStoreContext<TKey> Context { get; }

        Task<bool> Exists(TKey key);

        Task<bool> Create(TKey key, TValue value);

        Task<bool> Delete(TKey key);

        Task<TValue> Get(TKey key);

        Task<bool> GetToStream(TKey key, Stream stream);

        Task<IEnumerable<TKey>> ListKeys(Expression<Func<TKey, bool>> predicate = null);

        Task<IEnumerable<TValue>> ListValues(Expression<Func<TKey, bool>> predicate = null);

        Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(Expression<Func<TKey, bool>> predicate = null);
        
        Task<bool> Update(TKey key, TValue value);

        Task Upsert(TKey key, TValue value);

        Task Upsert(TKey key, Func<TValue, TValue> mutator);

        Task Upsert(TKey key, Func<TValue, Task<TValue>> mutator);

        IQuerySession<TKey, TValue> StartQuery();

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
