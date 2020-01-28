using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.Serialization.Bond.Implementation;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    public static class BondBuilderExtensions
    {
        public static INeedsCompression BondSerialization(
            this INeedsSerialization target) 
        {
            return new Builder(target.Root.Argument(
                "serializer", 
                default(Constructable).Type(typeof(BondSimpleBinarySerializer))));
        }
    }
}
