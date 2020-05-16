using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface IObserver
    {
        Task BeforePut(object key, object value);

        Task BeforeDelete(object key);
    }

    public interface IObserver<TKey, TValue>
    {
        Task BeforePut(TKey key, TValue value);

        Task BeforeDelete(TKey key);
    }
}
