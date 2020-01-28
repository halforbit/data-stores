using Halforbit.DataStores.Interface;
using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Halforbit.DataStores.Implementation
{
    public class SingletonDataStore<TValue> : IDataStore<TValue>
    {
        readonly IDataStore<object, TValue> _source;

        public SingletonDataStore(IDataStore<object, TValue> source)
        {
            _source = source;
        }

        public IDataStoreContext<object> Context => _source.Context;

        public IStringMap<object> Map => _source.KeyMap;

        public Task<bool> Create(TValue value) => _source.Create(null, value);

        public Task<bool> Delete() => _source.Delete(null);

        public Task<bool> Exists() => _source.Exists(null);

        public Task<TValue> Get() => _source.Get(null);

        public Task<bool> GetToStream(Stream stream) => _source.GetToStream(null, stream);

        public Task<bool> Update(TValue value) => _source.Update(null, value);

        public Task<bool> Upsert(TValue value) => _source.Upsert(null, value);

        public Task<bool> Upsert(Func<TValue, TValue> mutator) => _source.Upsert(null, mutator);

        public Task<bool> Upsert(Func<TValue, Task<TValue>> mutator) => _source.Upsert(null, mutator);
    }
}
