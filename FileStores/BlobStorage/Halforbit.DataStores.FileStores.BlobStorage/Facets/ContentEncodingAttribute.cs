using Halforbit.DataStores.FileStores.BlobStorage.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.BlobStorage.Facets
{
    public class ContentEncodingAttribute : FacetParameterAttribute
    {
        public ContentEncodingAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override Type TargetType => typeof(BlobFileStore);

        public override string ParameterName => "contentEncoding";
    }
}
