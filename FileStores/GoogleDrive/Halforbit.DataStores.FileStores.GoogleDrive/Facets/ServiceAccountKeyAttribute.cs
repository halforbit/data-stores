using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Facets
{
    public class ServiceAccountKeyAttribute : FacetParameterAttribute
    {
        public override string ParameterName => "serviceAccountKey";

        public override Type TargetType => typeof(GoogleDriveFileStore);
    }
}
