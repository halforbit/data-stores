using System;
using System.Collections.Concurrent;

namespace Halforbit.DataStores
{
    public class DataContext : IDataContext
    {
        readonly ConcurrentDictionary<object, object> _cache = new ConcurrentDictionary<object, object>();

        public IDataStore<TKey, TValue> Get<TKey, TValue>(
            Func<INeedsIntegration, IDataStoreDescription<TKey, TValue>> getDataStoreDescription)
        {
            if (_cache.TryGetValue(getDataStoreDescription, out var cacheHit))
            {
                return (IDataStore<TKey, TValue>)cacheHit;
            }

            var instance = getDataStoreDescription(DataStore.Describe()).Build();

            _cache[getDataStoreDescription] = instance;

            return instance;
        }
    }
}
