using System.Text;
using Xunit;

namespace Halforbit.DataStores.FtpFileStore.Tests
{
    public class FtpFileStoresTests
    {
        [Fact, Trait("Type", "Integration")]
        public void TestFtpFileStore_Basic()
        {
            var ftpFileStore = new FileStores.Ftp.Implementation.FtpFileStore(
                "localhost",
                "test",
                "test");

            var xfiles = ftpFileStore.GetFiles("peanuts/lemons", string.Empty).Result;

            var files = ftpFileStore.GetFiles(string.Empty, string.Empty).Result;

            var xfile = ftpFileStore.ReadAllBytes("kiwis/grapefruits/limes.txt").Result;

            var x1 = ftpFileStore.Exists("apples/oranges/bananas.txt").Result;

            ftpFileStore
                .WriteAllBytes("apples/oranges/bananas.txt", Encoding.ASCII.GetBytes("Hello, world!"))
                .Wait();

            var x2 = ftpFileStore.Exists("apples/oranges/bananas.txt").Result;

            var file = Encoding.ASCII.GetString(
                ftpFileStore.ReadAllBytes("apples/oranges/bananas.txt").Result.Bytes);

            var d1 = ftpFileStore.Delete("apples/oranges/bananas.txt").Result;

            var d2 = ftpFileStore.Delete("apples/oranges/bananas.txt").Result;

            var x3 = ftpFileStore.Exists("apples/oranges/bananas.txt").Result;
        }
    }
}
