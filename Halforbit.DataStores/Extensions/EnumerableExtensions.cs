using System;
using System.Collections.Generic;
using System.Linq;

namespace Halforbit.DataStores
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IEnumerable<TSource>> Batch<TSource>(
            this IEnumerable<TSource> source, 
            int size)
        {
            return Batch(source, size, x => x);
        }

        public static IEnumerable<TResult> Batch<TSource, TResult>(
            this IEnumerable<TSource> source,
            int size,
            Func<IEnumerable<TSource>, TResult> resultSelector)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (size <= 0) throw new ArgumentOutOfRangeException(nameof(size));
            if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));

            switch (source)
            {
                case ICollection<TSource> collection when collection.Count == 0:
                {
                    return Enumerable.Empty<TResult>();
                }
                case ICollection<TSource> collection when collection.Count <= size:
                {
                    return _(); IEnumerable<TResult> _()
                    {
                        var bucket = new TSource[collection.Count];
                        collection.CopyTo(bucket, 0);
                        yield return resultSelector(bucket);
                    }
                }
                case IReadOnlyList<TSource> list when list.Count == 0:
                {
                    return Enumerable.Empty<TResult>();
                }
                case IReadOnlyList<TSource> list when list.Count <= size:
                {
                    return _(); IEnumerable<TResult> _()
                    {
                        var bucket = new TSource[list.Count];
                        for (var i = 0; i < list.Count; i++)
                            bucket[i] = list[i];
                        yield return resultSelector(bucket);
                    }
                }
                case IReadOnlyCollection<TSource> collection when collection.Count <= size:
                {
                    return Batch(collection.Count);
                }
                default:
                {
                    return Batch(size);
                }

                IEnumerable<TResult> Batch(int sz)
                {
                    TSource[] bucket = null;
                    var count = 0;

                    foreach (var item in source)
                    {
                        if (bucket == null)
                            bucket = new TSource[sz];

                        bucket[count++] = item;

                        // The bucket is fully buffered before it's yielded
                        if (count != sz)
                            continue;

                        yield return resultSelector(bucket);

                        bucket = null;
                        count = 0;
                    }

                    // Return the last bucket with all remaining elements
                    if (bucket != null && count > 0)
                    {
                        Array.Resize(ref bucket, count);
                        yield return resultSelector(bucket);
                    }
                }
            }
        }
    }
}
