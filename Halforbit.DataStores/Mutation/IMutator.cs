using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface IMutator
    {
        Task<object> MutatePut(object key, object value);
    }

    public interface IMutator<TKey, TValue>
    {
        Task<TValue> MutatePut(TKey key, TValue value);
    }
}
