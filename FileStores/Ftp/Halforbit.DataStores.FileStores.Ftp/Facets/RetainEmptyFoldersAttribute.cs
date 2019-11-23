using Halforbit.DataStores.FileStores.Ftp.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Ftp.Facets
{
    public class RetainEmptyFoldersAttribute : FacetParameterAttribute
    {
        public RetainEmptyFoldersAttribute() : base($"{false}")
        { }

        public override string ParameterName => "deleteEmptyFolders";

        public override Type TargetType => typeof(FtpFileStore);
    }
}
