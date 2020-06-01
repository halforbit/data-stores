using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.LocalStorage.Implementation
{
    public class LocalFileStore : IFileStore
    {
        readonly string _rootPath;

        public IFileStoreContext FileStoreContext => throw new NotImplementedException();

        public LocalFileStore(string rootPath)
        {
            _rootPath = rootPath;
        }

        public Task<bool> Delete(string path)
        {
            File.Delete(LocalizePath(path));

            return Task.FromResult(true);
        }

        public Task<bool> Exists(string path)
        {
            var localPath = LocalizePath(path);

            if(!Directory.Exists(Path.GetDirectoryName(localPath)))
            {
                return Task.FromResult(false);
            }

            return Task.FromResult(File.Exists(localPath));
        }

        public Task<IEnumerable<string>> GetFiles(
            string pathPrefix, 
            string extension)
        {
            var localPath = Path.GetDirectoryName(LocalizePath(pathPrefix));

            if(!Directory.Exists(localPath))
            {
                return Task.FromResult(Enumerable.Empty<string>());
            }

            return Task.FromResult(Directory
                .GetFiles(
                    localPath, 
                    $"*{extension}", 
                    SearchOption.AllDirectories)
                .Select(f => PathToKeyString(f))
                .Where(f => f.StartsWith(pathPrefix)));
        }

        public Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path,
            bool getETag)
        {
            var localPath = LocalizePath(path);

            if(!File.Exists(localPath))
            {
                return Task.FromResult<FileStoreReadAllBytesResult>(null);
            }

            var interim = 1;

            while(interim <= 1000)
            {
                try
                {
                    var bytes = File.ReadAllBytes(localPath);

                    return Task.FromResult(new FileStoreReadAllBytesResult(
                        bytes: bytes,
                        eTag: getETag ? ComputeETag(bytes) : null));
                }
                catch (IOException ioex)
                {
                    if (ioex.Message.Contains("another process") && interim <= 1000)
                    {
                        Task.Delay(interim).Wait();

                        interim *= 2;
                    }
                    else throw;
                }
            }

            throw new Exception("Could not read file because it is locked " + localPath);
        }

        public Task<bool> WriteAllBytes(
            string path, 
            byte[] contents,
            string eTag = null)
        {
            var localPath = LocalizePath(path);

            Directory.CreateDirectory(Path.GetDirectoryName(localPath));

            if(eTag == null)
            {
                File.WriteAllBytes(
                    localPath,
                    contents);

                return Task.FromResult(true);
            }
            else
            {
                var interim = 1;

                while(interim <= 1024)
                {
                    try
                    {
                        using (var fileStream = new FileStream(
                            localPath,
                            FileMode.OpenOrCreate,
                            FileAccess.ReadWrite,
                            FileShare.None))
                        {
                            //Console.WriteLine("Locked " + localPath);

                            var destBytes = ReadFully(fileStream);

                            if (destBytes.Length > 0)
                            {
                                var destETag = ComputeETag(destBytes);

                                if (eTag != destETag)
                                {
                                    return Task.FromResult(false);
                                }
                            }

                            fileStream.SetLength(contents.Length);

                            fileStream.Seek(0, SeekOrigin.Begin);

                            fileStream.Write(contents, 0, contents.Length);

                            fileStream.Flush();

                            //Console.WriteLine("Unlocking " + localPath);

                            return Task.FromResult(true);
                        }
                    }
                    catch (IOException ioex)
                    {
                        if (ioex.Message.Contains("another process") || 
                            ioex.Message.Contains("locked"))
                        {
                            if (interim <= 2048)
                            {
                                //Console.WriteLine("Failed to lock " + localPath);

                                //Console.WriteLine("Waiting " + interim);

                                Task.Delay(interim).Wait();

                                interim *= 2;
                            }
                            else
                            {
                                throw new Exception("Timeout while waiting for lock on " + localPath);
                            }
                        }
                        else
                        {
                            throw;
                        }
                    }
                }

                throw new Exception("Could not load destination file exclusively " + localPath);
            }
        }

        string LocalizePath(string path) => Path.Combine(_rootPath, path.Replace("/", "\\"));

        string PathToKeyString(string f)
        {
            return f
                .Substring(
                    _rootPath.Length + 1,
                    f.Length - (_rootPath.Length + 1))
                .Replace("\\", "/");
        }

        string ComputeETag(byte[] bytes)
        {
            using (var sha1 = SHA1.Create())
            {
                return Convert.ToBase64String(sha1.ComputeHash(bytes));
            }
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

        public async Task<bool> WriteStream(
            string path, 
            Stream contents, 
            string eTag = null)
        {
            var localPath = LocalizePath(path);

            Directory.CreateDirectory(Path.GetDirectoryName(localPath));

            if (eTag == null)
            {
                using (var file = File.Create(localPath))
                {
                    await contents.CopyToAsync(file);
                }

                return true;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public async Task<bool> ReadStream(
            string path, 
            Stream contents, 
            bool getETag = false)
        {
            if (getETag) throw new NotImplementedException();

            var localPath = LocalizePath(path);

            if (!File.Exists(localPath)) return false;

            using (var file = File.OpenRead(path))
            {
                await file.CopyToAsync(contents);
            }

            return true;
        }
    }
}
