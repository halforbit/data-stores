using Halforbit.DataStores.FileStores.LegacyFtp.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.LegacyFtp.Facets
{
    public class PasswordAttribute : FacetParameterAttribute
    {
        public PasswordAttribute(string value = null, string configKey = null) :
            base(value, configKey)
        { }

        public override string ParameterName => "password";

        public override Type TargetType => typeof(FtpFileStore);
    }
}
