using Halforbit.DataStores.AmazonS3;
using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.AmazonS3.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    namespace AmazonS3
    {
        public interface INeedsAccessKeyId : IConstructionNode { }

        public interface INeedsSecretAccessKey : IConstructionNode { }

        public interface INeedsBucketName : IConstructionNode { }

        public class Builder : 
            IConstructionNode,
            INeedsAccessKeyId,
            INeedsSecretAccessKey,
            INeedsBucketName
        {
            public Builder(Constructable root)
            {
                Root = root;
            }

            public Constructable Root { get; }
        }
    }

    public static class AmazonS3BuilderExtensions
    {
        public static INeedsAccessKeyId AmazonS3(
            this INeedsIntegration target) 
        {
            return new AmazonS3.Builder(target.Root
                .Type(typeof(FileStoreDataStore<,>))
                .Argument("fileStore", default(Constructable).Type(typeof(AmazonS3FileStore))));
        }

        public static INeedsSecretAccessKey AccessKeyId(
            this INeedsAccessKeyId target,
            string accessKeyId)
        {
            return new AmazonS3.Builder(target.Root
                .Argument("fileStore", c => c.Argument("accessKeyId", accessKeyId)));
        }

        public static INeedsBucketName SecretAccessKey(
            this INeedsSecretAccessKey target,
            string secretAccessKey)
        {
            return new AmazonS3.Builder(target.Root
                .Argument("fileStore", c => c.Argument("secretAccessKey", secretAccessKey)));
        }

        public static INeedsSerialization BucketName(
            this INeedsBucketName target,
            string bucketName)
        {
            return new Builder(target.Root
                .Argument("fileStore", c => c.Argument("bucketName", bucketName)));
        }
    }
}
