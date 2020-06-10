using Microsoft.Azure.Cosmos.Linq;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public static class Extensions
    {
        public static async Task<IReadOnlyList<TValue>> FetchListAsync<TValue>(
            this IQueryable<TValue> queryable)
        {
            var result = new List<TValue>();

            var feedIterator = queryable.ToFeedIterator();

            do
            {
                result.AddRange((await feedIterator.ReadNextAsync()).Resource);
            }
            while (feedIterator.HasMoreResults);

            return result;
        }

        public static async Task<TValue> FetchFirstOrDefaultAsync<TValue>(
            this IQueryable<TValue> queryable)
        {
            return (await queryable
                .Take(1)
                .ToFeedIterator()
                .ReadNextAsync())
                .Resource
                .FirstOrDefault();
        }
    }
}
