using Halforbit.DataStores.Validation.Model;
using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System.Threading.Tasks;

namespace Halforbit.DataStores.Validation.Interface
{
    public interface IValidator<TKey, TValue>
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
