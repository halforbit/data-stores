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

        Task<IReadOnlyDictionary<string, string>> GetMetadata(TKey key);

        Task SetMetadata(
            TKey key,
            IReadOnlyDictionary<string, string> keyValues);
    }
}
