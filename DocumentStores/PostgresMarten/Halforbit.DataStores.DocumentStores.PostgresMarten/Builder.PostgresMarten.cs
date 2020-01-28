using Halforbit.DataStores.DocumentStores.PostgresMarten;
using Halforbit.DataStores.PostgresMarten;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    namespace PostgresMarten
    {
        public interface INeedsConnectionString : IConstructionNode { }

        public class Builder :
            IConstructionNode,
            INeedsConnectionString
        {
            public Builder(Constructable root)
            {
                Root = root;
            }

            public Constructable Root { get; }
        }
    }

    public static class PostgresMartenBuilderExtensions
    {
        public static INeedsConnectionString PostgresMarten(
            this INeedsIntegration target) 
        {
            return new PostgresMarten.Builder(target.Root.Type(typeof(PostgresMartenDataStore<,>)));
        }

        public static INeedsDocumentMap ConnectionString(
            this INeedsConnectionString target, 
            string connectionString) 
        {
            return new Builder(target.Root.Argument("connectionString", connectionString));
        }
    }
}
