using Halforbit.DataStores.FileStores.Sftp.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Sftp.Facets
{
    public class MaxConcurrentConnectionsAttribute : FacetParameterAttribute
    {
        public MaxConcurrentConnectionsAttribute(int value = default, string configKey = null) : 
            base($"{value}", configKey) 
        { }

        public override string ParameterName => "maxConcurrentConnections";

        public override Type TargetType => typeof(SftpFileStore);
    }
}
