
namespace Halforbit.DataStores
{
    public interface IDataActionModifier
    {
        object ModifyPut(object key, object value);

        object ModifyQuery(object key);
    }
}
