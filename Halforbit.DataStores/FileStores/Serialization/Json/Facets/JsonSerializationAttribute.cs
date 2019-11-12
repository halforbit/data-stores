using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Model;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Facets
{
    public class JsonSerializationAttribute : FacetParameterAttribute
    {
        readonly string _value;

        public JsonSerializationAttribute()
        {
            _value = $"{JsonOptions.Default}";
        }

        public JsonSerializationAttribute(JsonOptions options)
        {
            _value = $"{options}";
        }

        public override Type TargetType => typeof(JsonSerializer);

        public override string ParameterName => "options";

        public override string Value => _value;
    }
}
