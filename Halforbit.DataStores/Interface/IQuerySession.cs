using System;
using System.Linq;

namespace Halforbit.DataStores.Interface
{
    public interface IQuerySession<TKey, TValue> : IDisposable
    {
        IQueryable<TValue> Query(TKey partialKey = default(TKey));
    }
}
