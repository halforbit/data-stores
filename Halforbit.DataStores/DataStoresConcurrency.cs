using System;

namespace Halforbit.DataStores
{
    public static class DataStoresConcurrency
    {
        public static int MaxOperations { get; } = Math.Max(Environment.ProcessorCount * 8, 32);
    }
}
