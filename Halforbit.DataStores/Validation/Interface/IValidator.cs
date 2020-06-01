using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface IValidator { }

    public interface IValidator<TKey, TValue> : IValidator
    {
        Task<ValidationErrors> ValidatePut(
            TKey key,
            TValue value,
            IStringMap<TKey> keyMap);

        Task<ValidationErrors> ValidateDelete(
            TKey key,
            IStringMap<TKey> keyMap);
    }
}
