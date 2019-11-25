using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Facets
{
    public class ServiceAccountEmailAttribute : FacetParameterAttribute
    {
        public ServiceAccountEmailAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override string ParameterName => "serviceAccountEmail";

        public override Type TargetType => typeof(GoogleDriveFileStore);
    }
}
