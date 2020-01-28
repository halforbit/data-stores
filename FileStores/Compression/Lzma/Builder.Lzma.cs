using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.Compression.GZip.Implementation;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    public static class LzmaBuilderExtensions
    {
        public static INeedsFileExtension LzmaCompression(
            this INeedsCompression target) 
        {
            return new Builder(target.Root.Argument(
                "compressor", 
                default(Constructable).Type(typeof(LzmaCompressor))));
        }
    }
}
