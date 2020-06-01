
namespace Halforbit.DataStores
{
    public interface IDataActionObserver
    {
        void OnPut(object key, object value);

        void OnDelete(object key, object value);
    }
}
