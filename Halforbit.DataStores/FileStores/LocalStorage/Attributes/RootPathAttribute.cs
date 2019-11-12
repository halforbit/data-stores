using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.LocalStorage.Attributes
{
    public class RootPathAttribute : FacetParameterAttribute
    {
        public RootPathAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override Type TargetType => typeof(LocalFileStore);

        public override string ParameterName => "rootPath";

        public override Type[] ImpliedTypes => new Type[]
        {
            typeof(FileStoreDataStore<,>)
        };
    }
}
