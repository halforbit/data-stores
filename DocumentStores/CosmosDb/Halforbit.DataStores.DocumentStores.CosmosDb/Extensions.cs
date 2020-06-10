using Microsoft.Azure.Cosmos.Linq;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public static class Extensions
    {
        public static async Task<IReadOnlyList<TValue>> ExecuteAsync<TValue>(
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
    }
}
