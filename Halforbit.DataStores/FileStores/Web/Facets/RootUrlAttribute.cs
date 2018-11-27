using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Web.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Web.Facets
{
    public class RootUrlAttribute : FacetParameterAttribute
    {
        public RootUrlAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override Type TargetType => typeof(WebFileStore);

        public override string ParameterName => "rootUrl";

        public override Type[] ImpliedTypes => new Type[]
        {
            typeof(FileStoreDataStore<,>)
        };
    }
}
