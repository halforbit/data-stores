using Halforbit.DataStores.FileStores.Sftp.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Sftp.Facets
{
    public class RetainEmptyFoldersAttribute : FacetParameterAttribute
    {
        public RetainEmptyFoldersAttribute() : base($"{false}")
        { }

        public override string ParameterName => "deleteEmptyFolders";

        public override Type TargetType => typeof(SftpFileStore);
    }
}
