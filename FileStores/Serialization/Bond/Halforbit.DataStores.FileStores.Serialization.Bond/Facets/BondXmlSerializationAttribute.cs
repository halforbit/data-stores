using Halforbit.DataStores.FileStores.Serialization.Bond.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.Bond.Facets
{
    public class BondXmlSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(BondSimpleXmlSerializer);
    }
}
