using Halforbit.DataStores.BlobStorage;
using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.BlobStorage.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    namespace BlobStorage
    {
        public interface INeedsConnectionString : IConstructionNode { }

        public interface INeedsContainer : IConstructionNode { }

        public class Builder : 
            IConstructionNode,
            INeedsConnectionString,
            INeedsContainer
        {
            public Builder(Constructable root)
            {
                Root = root;
            }

            public Constructable Root { get; }
        }
    }

    public static class BlobStorageBuilderExtensions
    {
        public static INeedsConnectionString BlobStorage(
            this INeedsIntegration target) 
        {
            return new BlobStorage.Builder(target.Root
                .Type(typeof(FileStoreDataStore<,>))
                .Argument("fileStore", default(Constructable).Type(typeof(BlobFileStore))));
        }

        public static INeedsContainer ConnectionString(
            this INeedsConnectionString target, 
            string connectionString) 
        {
            return new BlobStorage.Builder(target.Root
                .Argument("fileStore", c => c.Argument("connectionString", connectionString)));
        }

        public static INeedsContentType Container(
            this INeedsContainer target, 
            string container) 
        {
            return new Builder(target.Root
                .Argument("fileStore", c => c.Argument("containerName", container)));
        }

        public static INeedsContentEncoding ContentType(
            this INeedsContentType target, 
            string contentType) 
        {
            return new Builder(target.Root
                .Argument("fileStore", c => c.Argument("contentType", contentType))); 
        }

        public static INeedsContentEncoding DefaultContentType(
            this INeedsContentType target) 
        { 
            return target.ContentType("application/octet-stream"); 
        }

        public static INeedsSerialization ContentEncoding(
            this INeedsContentEncoding target, 
            string contentEncoding) 
        {
            return new Builder(target.Root
                .Argument("fileStore", c => c.Argument("contentEncoding", contentEncoding)));
        }

        public static INeedsSerialization DefaultContentEncoding(
            this INeedsContentEncoding target) 
        {
            return target.ContentEncoding(string.Empty);
        }
    }
}
