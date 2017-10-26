using System.Collections.Generic;

namespace Halforbit.DataStores.Interface
{
    public interface IDataActionValidator<TKey, TValue>
    {
        IEnumerable<IValidationError> ValidatePut(TKey key, TValue value);

        IEnumerable<IValidationError> ValidateDelete(TKey key, TValue value);
    }
}
