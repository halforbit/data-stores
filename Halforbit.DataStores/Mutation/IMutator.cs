using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface IMutator
    {
        Task<object> MutateGet(object key, object value);

        Task<object> MutatePut(object key, object value);
    }

    public interface IMutator<TKey, TValue>
    {
        Task<TValue> MutateGet(TKey key, TValue value);

        Task<TValue> MutatePut(TKey key, TValue value);
    }
}
