using System;
using Halforbit.Facets.Attributes;

namespace Halforbit.DataStores.FileStores.Ftp.Facets
{
    public class FtpHostAttribute : FacetParameterAttribute
    {
        public FtpHostAttribute(string value = null, string configKey = null) : base(value, configKey)
        {
        }

        public override string ParameterName => throw new NotImplementedException();

        public override Type TargetType => throw new NotImplementedException();
    }
}
