using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Facets
{
    public class ApplicationNameAttribute : FacetParameterAttribute
    {
        public override string ParameterName => "applicationName";

        public override Type TargetType => typeof(GoogleDriveFileStore);
    }
}
