using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    internal static class GenericBulkOpExtensions
    {
        public static Task<IReadOnlyList<KeyValuePair<TKey, bool>>> BulkCreate<TKey, TValue>(
            this IDataStore<TKey, TValue> store,
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            return values.SelectAsync(i => Kvp(i, store.Create),
                maxPending: DataStoresConcurrency.MaxOperations);
        }

        public static Task<IReadOnlyList<KeyValuePair<TKey,bool>>> BulkDelete<TKey, TValue>(
            this IDataStore<TKey, TValue> store,
            IEnumerable<TKey> keys)
        {
            return keys.SelectAsync(i => Kvp(i, store.Delete),
                maxPending: DataStoresConcurrency.MaxOperations);
        }

        public static Task<IReadOnlyList<KeyValuePair<TKey,TValue>>> BulkGet<TKey, TValue>(
            this IDataStore<TKey, TValue> store,
            IEnumerable<TKey> keys)
        {
            return keys.SelectAsync(i => Kvp(i, store.Get),
                maxPending: DataStoresConcurrency.MaxOperations);
        }
        
        public static Task<IReadOnlyList<KeyValuePair<TKey,bool>>> BulkUpdate<TKey, TValue>(
            this IDataStore<TKey, TValue> store,
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            return values.SelectAsync(i => Kvp(i,  store.Update),
                maxPending: DataStoresConcurrency.MaxOperations);
        }
        
        public static Task BulkUpsert<TKey, TValue>(
            this IDataStore<TKey, TValue> store,
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            return values.ForEachAsync(i => store.Upsert(i.Key, i.Value),
                maxPending: DataStoresConcurrency.MaxOperations);
        }

        private static async Task<KeyValuePair<TKey, TValue>> Kvp<TKey, TValue>(TKey key, Func<TKey,Task<TValue>> valueProducer)
        {
            return new KeyValuePair<TKey, TValue>(key,
                await valueProducer(key).ConfigureAwait(false));
        }
        
        private static async Task<KeyValuePair<TKey, TReturn>> Kvp<TKey, TValue, TReturn>(KeyValuePair<TKey, TValue> pair, Func<TKey, TValue, Task<TReturn>> valueProducer)
        {
            return new KeyValuePair<TKey, TReturn>(pair.Key,
                await valueProducer(pair.Key, pair.Value).ConfigureAwait(false));
        }
    }
}
