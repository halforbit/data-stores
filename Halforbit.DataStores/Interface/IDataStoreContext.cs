using Halforbit.DataStores.Model;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.Interface
{
    public interface IDataStoreContext<TKey>
    {
        Task<Uri> GetSharedAccessUrl(
            TKey key,
            DateTime expiration,
            Access access);

        Task<IReadOnlyDictionary<TKey, EntityInfo>> ListEntityInfos(
            Expression<Func<TKey, bool>> selector = null);

        Task<EntityInfo> GetEntityInfo(TKey key);

        Task SetEntityInfo(TKey key, EntityInfo entityInfo);

        Task<IReadOnlyDictionary<TKey, IReadOnlyDictionary<string, string>>> ListMetadatas(
            Expression<Func<TKey, bool>> selector = null);

        Task<IReadOnlyDictionary<string, string>> GetMetadata(
            TKey key,
            bool percentDecodeValues = true);

        Task SetMetadata(
            TKey key,
            IReadOnlyDictionary<string, string> keyValues,
            bool percentEncodeValues = true);

        Task<string> AcquireLease(TKey key, TimeSpan leaseTime);

        Task RenewLease(TKey key, string leaseId);

        Task<string> ChangeLease(TKey key, string currentLeaseId);

        Task ReleaseLease(TKey key, string leaseId);

        Task BreakLease(TKey key, TimeSpan breakReleaseTime);
    }
}
