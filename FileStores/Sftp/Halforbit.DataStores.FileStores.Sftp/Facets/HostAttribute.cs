using Halforbit.DataStores.FileStores.Sftp.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Sftp.Facets
{
    public class HostAttribute : FacetParameterAttribute
    {
        public HostAttribute(string value = null, string configKey = null) : 
            base(value, configKey) 
        { }

        public override string ParameterName => "host";

        public override Type TargetType => typeof(SftpFileStore);
    }
}
