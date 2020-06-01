using System;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public interface IRetryExecutor
    {
        Task<TResult> ExecuteWithRetry<TResult>(Func<Task<TResult>> getTask);
    }
}
