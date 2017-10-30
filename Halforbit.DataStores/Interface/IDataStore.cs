using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.Interface
{
    public interface IDataStore<TKey, TValue>
    {
        Task<bool> Exists(TKey key);

        Task<bool> Create(TKey key, TValue value);

        Task<bool> Delete(TKey key);

        Task<TValue> Get(TKey key);

        Task<IEnumerable<TKey>> ListKeys(Expression<Func<TKey, bool>> predicate = null);

        Task<IEnumerable<TValue>> ListValues(Expression<Func<TKey, bool>> predicate = null);

        Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(Expression<Func<TKey, bool>> predicate = null);
        
        Task<bool> Update(TKey key, TValue value);

        Task<bool> Upsert(TKey key, TValue value);

        Task<bool> Upsert(TKey key, Func<TValue, TValue> mutator);

        IQuerySession<TKey, TValue> StartQuery();
    }
}
