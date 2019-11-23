using Halforbit.DataStores.FileStores.Ftp.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Ftp.Facets
{
    public class UnlimitedConcurrentConnectionsAttribute : FacetParameterAttribute
    {
        public UnlimitedConcurrentConnectionsAttribute() : base($"{0}") { }

        public override string ParameterName => "maxConcurrentConnections";

        public override Type TargetType => typeof(FtpFileStore);
    }
}
