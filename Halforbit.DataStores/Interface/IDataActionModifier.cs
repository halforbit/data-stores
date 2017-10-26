
namespace Halforbit.DataStores.Interface
{
    public interface IDataActionModifier
    {
        object ModifyPut(object key, object value);

        object ModifyQuery(object key);
    }
}
