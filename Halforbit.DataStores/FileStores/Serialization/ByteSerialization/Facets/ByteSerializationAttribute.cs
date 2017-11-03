using Halforbit.DataStores.FileStores.Serialization.ByteSerialization.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.ByteSerialization.Facets
{
    public class ByteSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(ByteSerializer); 
    }
}
