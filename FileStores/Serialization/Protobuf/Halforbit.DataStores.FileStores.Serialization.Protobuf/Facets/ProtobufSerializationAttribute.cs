using Halforbit.DataStores.Serialization.Protobuf.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.Serialization.Protobuf.Attributes
{
    public class ProtobufSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(ProtobufSerializer);
    }
}
