using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Yaml.Model;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    public static class YamlBuilderExtensions
    {
        public static INeedsCompression YamlSerialization(
            this INeedsSerialization target,
            YamlOptions options = YamlOptions.Default) 
        {
            return new Builder(target.Root.Argument(
                "serializer", 
                default(Constructable)
                    .Type(typeof(YamlSerializer))
                    .Argument("options", $"{options}")));
        }
    }
}
