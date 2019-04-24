using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Web.Implementation
{
    public class WebFileStore : IFileStore
    {
        readonly string _rootUrl;

        public IFileStoreContext FileStoreContext => throw new NotImplementedException();

        public WebFileStore(string rootUrl)
        {
            _rootUrl = rootUrl;
        }

        public Task<bool> Delete(string path)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> Exists(string path)
        {
            var url = GetUrl(path);

            var request = WebRequest.Create(url);

            request.Method = "HEAD";

            try
            {
                var response = await request.GetResponseAsync();

                return true;
            }
            catch (WebException wex)
            {
                if (wex.Message.Contains("404")) return false;

                throw;
            }
        }

        public Task<IEnumerable<string>> GetFiles(
            string pathPrefix, 
            string extension)
        {
            throw new NotImplementedException();
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path, 
            bool getETag = false)
        {
            var url = GetUrl(path);

            var request = WebRequest.Create(url);

            try
            {
                var response = await request.GetResponseAsync();

                return new FileStoreReadAllBytesResult(
                    bytes: ReadFully(response.GetResponseStream()));
            }
            catch (WebException wex)
            {
                if (wex.Message.Contains("404")) return null;

                throw;
            }
        }

        public Task<bool> WriteAllBytes(
            string path, 
            byte[] contents, 
            string eTag = null)
        {
            throw new NotImplementedException();
        }

        static byte[] ReadFully(Stream input)
        {
            var buffer = new byte[16 * 1024];

            using (var memoryStream = new MemoryStream())
            {
                var read = 0;

                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    memoryStream.Write(buffer, 0, read);
                }

                return memoryStream.ToArray();
            }
        }

        string GetUrl(string path) => $"{_rootUrl}/{path}";

        public Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ReadStream(string path, Stream contents, bool getETag = false)
        {
            throw new NotImplementedException();
        }
    }
}
