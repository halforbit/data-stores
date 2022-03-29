using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public class Observer : IObserver
    {
        public virtual Task AfterDelete(object key)
        {
            return Task.CompletedTask;
        }

        public virtual Task AfterPut(object key, object value)
        {
            return Task.CompletedTask;
        }

        public virtual Task BeforeDelete(object key)
        {
            return Task.CompletedTask;
        }

        public virtual Task BeforePut(object key, object value)
        {
            return Task.CompletedTask;
        }
    }

    public class Observer<TKey, TValue> : IObserver<TKey, TValue>
    {
        public virtual Task AfterDelete(TKey key)
        {
            return Task.CompletedTask;
        }

        public virtual Task AfterPut(TKey key, TValue value)
        {
            return Task.CompletedTask;
        }

        public virtual Task BeforeDelete(TKey key)
        {
            return Task.CompletedTask;
        }

        public virtual Task BeforePut(TKey key, TValue value)
        {
            return Task.CompletedTask;
        }
    }
}
