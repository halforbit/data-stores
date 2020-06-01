using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores.FileStores.Serialization.Delimited
{
    public static class DelimitedBuilderExtensions
    {
        public static INeedsCompression DelimitedSerialization(
            this INeedsSerialization target,
            string delimiter = Delimiter.Tab,
            bool hasHeader = true)
        {
            return new Builder(target.Root.Argument(
                "serializer",
                default(Constructable)
                    .Type(typeof(DelimitedSerializer))
                    .Argument(nameof(delimiter), delimiter)
                    .Argument(nameof(hasHeader), hasHeader)));
        }
    }
}
