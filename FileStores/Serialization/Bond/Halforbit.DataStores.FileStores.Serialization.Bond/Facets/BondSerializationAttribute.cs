using Halforbit.DataStores.FileStores.Serialization.Bond.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.Bond.Facets
{
    public class BondSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(BondSimpleBinarySerializer);
    }
}
