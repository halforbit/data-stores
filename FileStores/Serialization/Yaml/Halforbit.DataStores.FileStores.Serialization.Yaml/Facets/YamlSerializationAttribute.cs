using Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Yaml.Model;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.Yaml.Facets
{
    public class YamlSerializationAttribute : FacetParameterAttribute
    {
        readonly string _value;

        public YamlSerializationAttribute()
        {
            _value = $"{YamlOptions.Default}";
        }

        public YamlSerializationAttribute(YamlOptions options)
        {
            _value = $"{options}";
        }

        public override Type TargetType => typeof(YamlSerializer);

        public override string ParameterName => "options";

        public override string Value => _value;
    }
}
