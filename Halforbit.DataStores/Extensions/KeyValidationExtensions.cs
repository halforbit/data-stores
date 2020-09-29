using System;
using System.Collections.Generic;

namespace Halforbit.DataStores.Internal
{
    public static class KeyValidationExtensions
    {
        public static void ThrowIfKeyIsDefaultValue<TKey>(this TKey key)
        {
            if (!typeof(TKey).Equals(typeof(object)) &&
                EqualityComparer<TKey>.Default.Equals(key, default))
            {
                throw new ArgumentOutOfRangeException(
                    $"The provided key is the default value of {typeof(TKey).Name}, " +
                    $"which is a reserved value and not an allowed value for keys.");
            }
        }
    }
}
