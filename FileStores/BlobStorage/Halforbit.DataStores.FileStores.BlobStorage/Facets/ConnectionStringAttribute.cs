using Halforbit.DataStores.FileStores.BlobStorage.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.BlobStorage.Facets
{
    public class ConnectionStringAttribute : FacetParameterAttribute
    {
        public ConnectionStringAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override Type TargetType => typeof(BlobFileStore);

        public override string ParameterName => "connectionString";
    }
}
