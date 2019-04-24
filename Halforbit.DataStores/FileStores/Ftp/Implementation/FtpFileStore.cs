using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using Halforbit.ObjectTools.Collections;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Ftp.Implementation
{
    public class FtpFileStore : IFileStore
    {
        static readonly Regex _listMatcher = new Regex(
            $@"^(?<{nameof(ListEntry.Permissions)}>[a-z-]+)\s+" +
                $@"(?<{nameof(ListEntry.LinkCount)}>[0-9]+)\s+" +
                $@"(?<{nameof(ListEntry.Owner)}>[^\s]+)\s+" +
                $@"(?<{nameof(ListEntry.Group)}>[^\s]+)\s+" +
                $@"(?<{nameof(ListEntry.Size)}>[0-9]+)\s+" +
                $@"(?<{nameof(ListEntry.Month)}>[A-Za-z]+)\s+" +
                $@"(?<{nameof(ListEntry.Day)}>[0-9]+)\s+" +
                $@"((?<{nameof(ListEntry.Time)}>[0-9]{{1,2}}:[0-9]{{2}})|(?<Year>[0-9]{{4}}))\s+" +
                $@"(?<{nameof(ListEntry.Name)}>.*)$", 
            RegexOptions.Compiled);

        readonly string _host;

        readonly string _username;

        readonly string _password;

        public IFileStoreContext FileStoreContext => throw new System.NotImplementedException();

        public FtpFileStore(
            string host, 
            string username, 
            string password)
        {
            _host = host;

            _username = username;

            _password = password;
        }

        public async Task<bool> Delete(string path)
        {
            var url = $"ftp://{_host}/{path}";

            var request = GetFtpRequest(url);

            request.Method = WebRequestMethods.Ftp.DeleteFile;

            var response = default(FtpWebResponse);

            try
            {
                response = (FtpWebResponse)await request.GetResponseAsync();
            }
            catch(WebException wex)
            {
                if(wex.Message.Contains("(550)"))
                {
                    // Not found.

                    return false;
                }
                else
                {
                    throw;
                }
            }

            return true;
        }

        public async Task<bool> Exists(string path)
        {
            var folder = Path.GetDirectoryName(path).Replace('\\', '/');

            var files = await GetFiles(folder, Path.GetExtension(path));

            return files.Any(f => f == path);
        }

        public async Task<IEnumerable<string>> GetFiles(
            string pathPrefix, 
            string extension)
        {
            var url = string.IsNullOrWhiteSpace(pathPrefix) ?
                $"ftp://{_host}" :
                $"ftp://{_host}/{pathPrefix}";

            var request = GetFtpRequest(url);

            request.Method = WebRequestMethods.Ftp.ListDirectoryDetails;

            var response = default(FtpWebResponse);
            
            try
            {
                response = (FtpWebResponse)await request.GetResponseAsync();
            }
            catch(WebException wex)
            {
                if (wex.Message.Contains("(550)"))
                {
                    // Not found.

                    return EmptyReadOnlyList<string>.Instance;
                }
                else
                {
                    throw;
                }
            }

            using (var responseStream = response.GetResponseStream())
            using (var streamReader = new StreamReader(responseStream))
            {
                var responseText = await streamReader.ReadToEndAsync();

                var lines = responseText.Split(
                    new string[] { "\r\n", "\n" },
                    StringSplitOptions.RemoveEmptyEntries);

                var entries = lines
                    .Select(l => _listMatcher.Match(l))
                    .Select(m => new ListEntry(
                        permissions: m.Groups[nameof(ListEntry.Permissions)].Value,
                        linkCount: int.Parse(m.Groups[nameof(ListEntry.LinkCount)].Value),
                        owner: m.Groups[nameof(ListEntry.Owner)].Value,
                        group: m.Groups[nameof(ListEntry.Group)].Value,
                        size: long.Parse(m.Groups[nameof(ListEntry.Size)].Value),
                        month: m.Groups[nameof(ListEntry.Month)].Value,
                        day: m.Groups[nameof(ListEntry.Day)].Value,
                        time: m.Groups[nameof(ListEntry.Time)].Value,
                        name: m.Groups[nameof(ListEntry.Name)].Value))
                    .ToList();

                var files = entries
                    .Where(e => !e.IsFolder && e.Name.ToLower().EndsWith(extension ?? string.Empty))
                    .Select(e => string.IsNullOrWhiteSpace(pathPrefix) ?
                        e.Name :
                        $"{pathPrefix}/{e.Name}")
                    .ToList();

                foreach (var folder in entries.Where(e => e.IsFolder))
                {
                    files.AddRange(await GetFiles(
                        string.IsNullOrWhiteSpace(pathPrefix) ?
                            folder.Name :
                            $"{pathPrefix}/{folder.Name}",
                        extension));
                }

                return files;
            }
        }

        private FtpWebRequest GetFtpRequest(string url)
        {
            var request = (FtpWebRequest)WebRequest.Create(url);

            request.Credentials = new NetworkCredential(_username, _password);

            return request;
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path, 
            bool getETag = false)
        {
            var url = $"ftp://{_host}/{path}";

            var request = GetFtpRequest(url);

            request.Method = WebRequestMethods.Ftp.DownloadFile;

            var response = default(FtpWebResponse);

            try
            {
                response = (FtpWebResponse)await request.GetResponseAsync();
            }
            catch(WebException wex)
            {
                if(wex.Message.Contains("(550)"))
                {
                    // Not found.

                    return new FileStoreReadAllBytesResult(bytes: null);
                }
                else
                {
                    throw;
                }
            }

            using (var responseStream = response.GetResponseStream())
            {
                return new FileStoreReadAllBytesResult(
                    bytes: ReadFully(responseStream));
            }
        }

        public async Task<bool> WriteAllBytes(
            string path, 
            byte[] contents, 
            string eTag = null)
        {
            var folder = Path.GetDirectoryName(path).Replace('\\', '/');

            if(!string.IsNullOrWhiteSpace(folder))
            {
                await MakeFolder(folder);
            }

            var url = $"ftp://{_host}/{path}";

            var request = GetFtpRequest(url);

            request.Method = WebRequestMethods.Ftp.UploadFile;

            request.ContentLength = contents.Length;

            using (var requestStream = await request.GetRequestStreamAsync())
            {
                await requestStream.WriteAsync(contents, 0, contents.Length);
            }

            using (var response = (FtpWebResponse)await request.GetResponseAsync())
            {
                return true;
            }
        }

        async Task MakeFolder(string folder)
        {
            var parentFolder = Path.GetDirectoryName(folder).Replace('\\', '/');

            if(!string.IsNullOrWhiteSpace(parentFolder))
            {
                await MakeFolder(parentFolder);
            }

            var url = $"ftp://{_host}/{folder}";

            var request = GetFtpRequest(url);

            request.Method = WebRequestMethods.Ftp.MakeDirectory;

            var response = default(FtpWebResponse);

            try
            {
                response = (FtpWebResponse)await request.GetResponseAsync();
            }
            catch (WebException wex)
            {
                if (wex.Message.Contains("(550)"))
                {
                    // Already exists.

                    return;
                }
                else
                {
                    throw;
                }
            }
        }

        static byte[] ReadFully(Stream input)
        {
            var buffer = new byte[16 * 1024];

            using (var ms = new MemoryStream())
            {
                int read;

                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }

                return ms.ToArray();
            }
        }

        public Task<Stream> ReadStream(string path, bool getETag = false)
        {
            throw new NotImplementedException();
        }

        public Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            throw new NotImplementedException();
        }

        class ListEntry
        {
            public ListEntry(
                string permissions,
                int linkCount,
                string owner,
                string group,
                long size,
                string month,
                string day,
                string time,
                string name)
            {
                Permissions = permissions;

                LinkCount = linkCount;

                Owner = owner;

                Group = group;

                Size = size;

                Month = month;

                Day = day;

                Time = time;

                Name = name;
            }

            public string Permissions { get; }

            public int LinkCount { get; }

            public string Owner { get; }

            public string Group { get; }

            public long Size { get; }

            public string Month { get; }

            public string Day { get; }

            public string Time { get; }

            public string Name { get; }

            public bool IsFolder => Permissions.StartsWith("d");
        }
    }
}
