using System;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Interface
{
    public interface IRetryExecutor
    {
        Task<TResult> ExecuteWithRetry<TResult>(Func<Task<TResult>> getTask);
    }
}
