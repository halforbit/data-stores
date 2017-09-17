using Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.Yaml.Facets
{
    public class YamlSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(YamlSerializer);
    }
}
