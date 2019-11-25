using Halforbit.DataStores.FileStores.LegacyFtp.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.LegacyFtp.Facets
{
    public class UsernameAttribute : FacetParameterAttribute
    {
        public UsernameAttribute(string value = null, string configKey = null) :
            base(value, configKey)
        { }

        public override string ParameterName => "username";

        public override Type TargetType => typeof(FtpFileStore);
    }
}
