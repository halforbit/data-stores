using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Facets
{
    public class ServiceAccountKeyAttribute : FacetParameterAttribute
    {
        public ServiceAccountKeyAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override string ParameterName => "serviceAccountKey";

        public override Type TargetType => typeof(GoogleDriveFileStore);
    }
}
