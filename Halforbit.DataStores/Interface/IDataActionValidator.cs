using Halforbit.DataStores.Exceptions;
using System.Collections.Generic;

namespace Halforbit.DataStores.Interface
{
    public interface IDataActionValidator<TKey, TValue>
    {
        IReadOnlyList<ValidationError> ValidatePut(TKey key, TValue value);

        IReadOnlyList<ValidationError> ValidateDelete(TKey key);
    }
}
