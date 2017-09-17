using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Facets
{
    public class FileExtensionAttribute : FacetParameterAttribute
    {
        public FileExtensionAttribute(string value = null, string configKey = null) : base(value, configKey) { }
    
        public override Type TargetType => typeof(FileStoreDataStore<,>);

        public override string ParameterName => "fileExtension";
    }
}
