using Halforbit.DataStores.Implementation;
using Halforbit.DataStores.Interface;
using Xunit;

namespace Halforbit.DataStores.IntegrationTests
{
    public class DataContextTests
    {
        [Fact, Trait("Type", "Unit")]
        public void LocalStorage()
        {
            var dc = new MyDataContext3();

            var a = dc.CoolThings;

            var b = dc.CoolThings;

            var c = dc.BadThings;
        }
    }

    public class MyDataContext3
    {
        readonly DataContext _context = new DataContext();

        public IDataStore<string, string> CoolThings => _context.Get(store => store
            .With(Location.BlobStorage.MyStorageAccount.MyContainer, "connection-string")
            .With(Format.Structured.Json)
            .Map<string, string>("cool-things/{this}"));

        public IDataStore<string, string> BadThings => _context.Get(store => store
            .With(Location.BlobStorage.MyStorageAccount.MyContainer, "connection-string")
            .With(Format.Structured.Json)
            .Map<string, string>("bad-things/{this}"));
    }

}
