using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public class ShardedDataStore<TShardConfig, TKey, TValue> : IDataStore<TKey, TValue>
    {
        readonly Func<TKey, string> _keyToShardId;
        readonly IReadOnlyDictionary<string, IDataStore<TKey, TValue>> _shardStores;

        public ShardedDataStore(
            Func<TKey, string> keyToShardId,
            IEnumerable<(string ShardId, TShardConfig ShardConfig)> shardConfigs,
            Func<TShardConfig, IDataStoreDescription<TKey, TValue>> describeShard)
        {
            _keyToShardId = keyToShardId;
            _shardStores = shardConfigs.ToDictionary(
                c => c.ShardId,
                c => describeShard(c.ShardConfig).Build());
        }

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => throw new NotImplementedException();

        public Task<IEnumerable<TResult>> BatchQuery<TItem, TResult>(
            IEnumerable<TItem> items, 
            Func<IEnumerable<TItem>, IQueryable<TValue>, IQueryable<TResult>> query, 
            Expression<Func<TKey, bool>> predicate = null, 
            int batchSize = 500)
        {
            throw new NotImplementedException();
        }

        public Task<bool> Create(TKey key, TValue value) => ResolveStore(key).Create(key, value);

        public async Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Create(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            var tasks = values
                .GroupBy(v => _keyToShardId(v.Key))
                .Select(g => ResolveStore(g.Key).Create(g));

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result).ToList();
        }

        public Task<bool> Delete(TKey key) => ResolveStore(key).Delete(key);

        public async Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Delete(IEnumerable<TKey> keys)
        {
            var tasks = keys
                .GroupBy(k => _keyToShardId(k))
                .Select(g => ResolveStore(g.Key).Delete(g));

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result).ToList();
        }

        public Task<bool> Exists(TKey key) => ResolveStore(key).Exists(key);

        public Task<TValue> Get(TKey key) => ResolveStore(key).Get(key);

        public async Task<IReadOnlyList<KeyValuePair<TKey, TValue>>> Get(IEnumerable<TKey> keys)
        {
            var tasks = keys
                .GroupBy(k => _keyToShardId(k))
                .Select(g => ResolveStore(g.Key).Get(g));

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result).ToList();
        }

        public Task<bool> GetToStream(TKey key, Stream stream) => ResolveStore(key).GetToStream(key, stream);

        public async Task<IEnumerable<TKey>> ListKeys(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var tasks = _shardStores
                .Select(s => s.Value.ListKeys(predicate))
                .ToList();

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result);
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var tasks = _shardStores
                .Select(s => s.Value.ListKeyValues(predicate))
                .ToList();

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result);
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var tasks = _shardStores
                .Select(s => s.Value.ListValues(predicate))
                .ToList();

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result);
        }

        public IQueryable<TValue> Query(Expression<Func<TKey, bool>> predicate = null)
        {
            throw new NotImplementedException();
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            throw new NotImplementedException();
        }

        public Task<bool> Update(
            TKey key, 
            TValue value)
        {
            return ResolveStore(key).Update(key, value);
        }

        public async Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Update(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            var tasks = values
                .GroupBy(v => _keyToShardId(v.Key))
                .Select(g => ResolveStore(g.Key).Update(g));

            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result).ToList();
        }

        public Task Upsert(
            TKey key, 
            TValue value)
        {
            return ResolveStore(key).Upsert(key, value);
        }

        public async Task Upsert(IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            var tasks = values
                .GroupBy(v => _keyToShardId(v.Key))
                .Select(g => ResolveStore(g.Key).Upsert(g));

            await Task.WhenAll(tasks);
        }

        public Task Upsert(
            TKey key, 
            Func<TValue, TValue> mutator)
        {
            return ResolveStore(key).Upsert(key, mutator);
        }

        public Task Upsert(
            TKey key, 
            Func<TValue, Task<TValue>> mutator)
        {
            return ResolveStore(key).Upsert(key, mutator);
        }

        IDataStore<TKey, TValue> ResolveStore(string shardId)
        {
            if (_shardStores.TryGetValue(
                shardId,
                out var shardStore))
            {
                return shardStore;
            }

            throw new ArgumentException($"A shard with id '{shardId}' is not configured.");
        }

        IDataStore<TKey, TValue> ResolveStore(TKey key) => ResolveStore(_keyToShardId(key));
    }
}
