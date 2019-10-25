using Halforbit.DataStores.DocumentStores.CosmosDb.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.DocumentStores.CosmosDb.Facets
{
    public class EndpointAttribute : FacetParameterAttribute
    {
        public EndpointAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override Type TargetType => typeof(DocumentDbDataStore<,>);

        public override string ParameterName => "endpoint";
    }
}
