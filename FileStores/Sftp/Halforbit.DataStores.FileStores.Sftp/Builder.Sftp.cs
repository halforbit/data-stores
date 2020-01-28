using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Sftp.Implementation;
using Halforbit.DataStores.Sftp;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    namespace Sftp
    {
        public interface INeedsHost : IConstructionNode { }

        public interface INeedsUsername : IConstructionNode { }

        public interface INeedsPassword : IConstructionNode { }

        public interface INeedsPort : IConstructionNode { }

        public interface INeedsDeleteEmptyFolders : IConstructionNode { }

        public interface INeedsMaxConcurrentConnections : IConstructionNode { }

        public class Builder : 
            IConstructionNode,
            INeedsHost,
            INeedsUsername,
            INeedsPassword,
            INeedsPort,
            INeedsDeleteEmptyFolders,
            INeedsMaxConcurrentConnections
        {
            public Builder(Constructable root)
            {
                Root = root;
            }

            public Constructable Root { get; }
        }
    }

    public static class SftpBuilderExtensions
    {
        public static INeedsHost Sftp(
            this INeedsIntegration target) 
        {
            return new Sftp.Builder(target.Root
                .Type(typeof(FileStoreDataStore<,>))
                .Argument("fileStore", default(Constructable).Type(typeof(SftpFileStore))));
        }

        public static INeedsUsername Host(
            this INeedsHost target,
            string host)
        {
            return new Sftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("host", host)));
        }

        public static INeedsPassword Username(
            this INeedsUsername target,
            string username)
        {
            return new Sftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("username", username)));
        }

        public static INeedsPort Password(
            this INeedsPassword target,
            string password)
        {
            return new Sftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("password", password)));
        }

        public static INeedsDeleteEmptyFolders Port(
            this INeedsPort target,
            int port)
        {
            return new Sftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("port", $"{port}")));
        }

        public static INeedsDeleteEmptyFolders DefaultPort(
            this INeedsPort target)
        {
            return new Sftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("port", $"{21}")));
        }

        public static INeedsMaxConcurrentConnections DeleteEmptyFolders(
            this INeedsDeleteEmptyFolders target)
        {
            return new Sftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("deleteEmptyFolders", $"{true}")));
        }

        public static INeedsMaxConcurrentConnections KeepEmptyFolders(
            this INeedsDeleteEmptyFolders target)
        {
            return new Sftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("deleteEmptyFolders", $"{false}")));
        }

        public static INeedsSerialization MaxConcurrentConnections(
            this INeedsMaxConcurrentConnections target,
            int maxConcurrentConnections)
        {
            return new Builder(target.Root
                .Argument("fileStore", c => c.Argument("maxConcurrentConnections", $"{maxConcurrentConnections}")));
        }

        public static INeedsSerialization DefaultMaxConcurrentConnections(
            this INeedsMaxConcurrentConnections target)
        {
            return new Builder(target.Root
                .Argument("fileStore", c => c.Argument("maxConcurrentConnections", $"{10}")));
        }
    }
}
