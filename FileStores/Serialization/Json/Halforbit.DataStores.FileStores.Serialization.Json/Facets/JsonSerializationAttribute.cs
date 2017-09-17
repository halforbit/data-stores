using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Serialization.Json.Facets
{
    public class JsonSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(JsonSerializer);
    }
}
