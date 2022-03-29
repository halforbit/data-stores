using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface IObserver
    {
        Task BeforePut(object key, object value);

        Task BeforeDelete(object key);

        Task AfterPut(object key, object value);

        Task AfterDelete(object key);
    }

    public interface IObserver<TKey, TValue>
    {
        Task BeforePut(TKey key, TValue value);

        Task BeforeDelete(TKey key);

        Task AfterPut(TKey key, TValue value);

        Task AfterDelete(TKey key);
    }
}
