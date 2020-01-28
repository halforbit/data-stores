using Halforbit.DataStores.AzureTables;
using Halforbit.DataStores.TableStores.AzureTables.Implementation;
using Halforbit.ObjectTools.DeferredConstruction;

namespace Halforbit.DataStores
{
    namespace AzureTables
    {
        public interface INeedsConnectionString : IConstructionNode { }

        public interface INeedsTable : IConstructionNode { }

        public class Builder :
            IConstructionNode,
            INeedsConnectionString,
            INeedsTable
        {
            public Builder(Constructable root)
            {
                Root = root;
            }

            public Constructable Root { get; }
        }
    }

    public static class AzureTablesBuilderExtensions
    {
        public static INeedsConnectionString AzureTables(
            this INeedsIntegration target) 
        {
            return new AzureTables.Builder(target.Root.Type(typeof(AzureTableStore<,>)));
        }

        public static INeedsTable ConnectionString(
            this INeedsConnectionString target, 
            string connectionString) 
        {
            return new AzureTables.Builder(target.Root.Argument("connectionString", connectionString));
        }

        public static INeedsMap Table(
            this INeedsTable target, 
            string table) 
        {
            return new Builder(target.Root.Argument("tableName", table));
        }
    }
}
