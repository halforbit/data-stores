using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.Ftp.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.Ftp;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    namespace Ftp
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

    public static class FtpBuilderExtensions
    {
        public static INeedsHost Ftp(
            this INeedsIntegration target) 
        {
            return new Ftp.Builder(target.Root
                .Type(typeof(FileStoreDataStore<,>))
                .Argument("fileStore", default(Constructable).Type(typeof(FtpFileStore))));
        }

        public static INeedsUsername Host(
            this INeedsHost target,
            string host)
        {
            return new Ftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("host", host)));
        }

        public static INeedsPassword Username(
            this INeedsUsername target,
            string username)
        {
            return new Ftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("username", username)));
        }

        public static INeedsPort Password(
            this INeedsPassword target,
            string password)
        {
            return new Ftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("password", password)));
        }

        public static INeedsDeleteEmptyFolders Port(
            this INeedsPort target,
            int port)
        {
            return new Ftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("port", $"{port}")));
        }

        public static INeedsDeleteEmptyFolders DefaultPort(
            this INeedsPort target)
        {
            return new Ftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("port", $"{21}")));
        }

        public static INeedsMaxConcurrentConnections DeleteEmptyFolders(
            this INeedsDeleteEmptyFolders target)
        {
            return new Ftp.Builder(target.Root
                .Argument("fileStore", c => c.Argument("deleteEmptyFolders", $"{true}")));
        }

        public static INeedsMaxConcurrentConnections KeepEmptyFolders(
            this INeedsDeleteEmptyFolders target)
        {
            return new Ftp.Builder(target.Root
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
