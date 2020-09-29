using System;
using System.Collections.Generic;

namespace Halforbit.DataStores.Internal
{
    public static class KeyValidationExtensions
    {
        public static void ThrowIfKeyIsDefaultValue<TKey>(this TKey key)
        {
<<<<<<< HEAD
            if (typeof(TKey).Equals(typeof(object))) return;

            if (EqualityComparer<TKey>.Default.Equals(key, default))
=======
            if (!typeof(TKey).Equals(typeof(object)) &&
                EqualityComparer<TKey>.Default.Equals(key, default))
>>>>>>> master
            {
                throw new ArgumentOutOfRangeException(
                    $"The provided key is the default value of {typeof(TKey).Name}, " +
                    $"which is a reserved value and not an allowed value for keys.");
            }
        }
        
        public static void ThrowIfKeyIsDefaultValue<TKey, TValue>(
            this IEnumerable<KeyValuePair<TKey, TValue>> keyValues)
        {
            if (typeof(TKey).Equals(typeof(object))) return;

            foreach (var keyValue in keyValues)
            {
                if (EqualityComparer<TKey>.Default.Equals(keyValue.Key, default))
                {
                    throw new ArgumentOutOfRangeException(
                        $"The provided key is the default value of {typeof(TKey).Name}, " +
                        $"which is a reserved value and not an allowed value for keys.");
                }
            }
        }

        public static void ThrowIfKeyIsDefaultValue<TKey>(this IEnumerable<TKey> keys)
        {
            if (typeof(TKey).Equals(typeof(object))) return;

            foreach (var key in keys)
            {
                if (EqualityComparer<TKey>.Default.Equals(key, default))
                {
                    throw new ArgumentOutOfRangeException(
                        $"The provided key is the default value of {typeof(TKey).Name}, " +
                        $"which is a reserved value and not an allowed value for keys.");
                }
            }
        }
    }
}
