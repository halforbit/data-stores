using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public class Mutator : IMutator
    {
        public virtual Task<object> MutateGet(object key, object value)
        {
            return Task.FromResult(value);
        }

        public virtual Task<object> MutatePut(object key, object value)
        {
            return Task.FromResult(value);
        }
    }

    public class Mutator<TKey, TValue> : IMutator<TKey, TValue>
    {
        public virtual Task<TValue> MutateGet(TKey key, TValue value)
        {
            return Task.FromResult(value);
        }

        public virtual Task<TValue> MutatePut(TKey key, TValue value)
        {
            return Task.FromResult(value);
        }
    }
}
