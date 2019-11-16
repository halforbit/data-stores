using Halforbit.DataStores.FileStores.Compression.GZip.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.FileStores.Compression.Lzma.Facets
{
    public class LzmaCompressionAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(LzmaCompressor);
    }
}
