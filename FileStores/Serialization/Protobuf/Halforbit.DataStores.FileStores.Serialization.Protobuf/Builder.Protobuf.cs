using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.Serialization.Protobuf.Implementation;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    public static class ProtobufBuilderExtensions
    {
        public static INeedsCompression ProtobufSerialization(
            this INeedsSerialization target) 
        {
            return new Builder(target.Root.Argument(
                "serializer", 
                default(Constructable).Type(typeof(ProtobufSerializer))));
        }
    }
}
