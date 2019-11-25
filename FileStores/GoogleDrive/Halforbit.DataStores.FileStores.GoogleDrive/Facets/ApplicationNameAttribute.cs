using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Facets
{
    public class ApplicationNameAttribute : FacetParameterAttribute
    {
        public ApplicationNameAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override string ParameterName => "applicationName";

        public override Type TargetType => typeof(GoogleDriveFileStore);
    }
}
