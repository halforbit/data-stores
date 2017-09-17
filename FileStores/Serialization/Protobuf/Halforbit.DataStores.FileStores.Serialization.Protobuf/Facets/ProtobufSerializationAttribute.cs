using Cloud.Data.Serialization.Protobuf.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Cloud.Data.Serialization.Protobuf.Attributes
{
    public class ProtobufSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(ProtobufSerializer);
    }
}
