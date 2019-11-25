using FluentFTP;
using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using Halforbit.Facets.Attributes;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FtpException = Halforbit.DataStores.FileStores.Ftp.Exceptions.FtpException;

namespace Halforbit.DataStores.FileStores.Ftp.Implementation
{
    public class FtpFileStore : IFileStore, IRetryExecutor
    {
        const int DefaultMaxConcurrentConnections = 10;

        static readonly RetryPolicy _retryPolicy = Policy
            .Handle<FtpException>(ex => ex.IsRetryable)
            .WaitAndRetryAsync(
                retryCount: 5,
                sleepDurationProvider: (count, exception, context) =>
                {
                    return TimeSpan.FromSeconds(Math.Pow(2, count));
                },
                onRetryAsync: (exception, timespan, count, context) => Task.CompletedTask);

        readonly FtpClientPool _ftpClientPool;

        readonly bool _deleteEmptyFolders;

        public FtpFileStore(
            string host,
            string username,
            string password,
            [Optional]string port = default,
            [Optional]string deleteEmptyFolders = default,
            [Optional]string maxConcurrentConnections = default)
        {
            if (string.IsNullOrWhiteSpace(host)) throw new ArgumentException($"{nameof(host)} required.");

            if (string.IsNullOrWhiteSpace(username)) throw new ArgumentException($"{nameof(username)} required.");

            if (string.IsNullOrWhiteSpace(password)) throw new ArgumentException($"{nameof(password)} required.");

            ConnectionInfo connectionInfo;

            if (string.IsNullOrWhiteSpace(port))
            {
                connectionInfo = new ConnectionInfo(
                    host: host,
                    username: username,
                    password: password,
                    port: 21);
            }
            else
            {
                if (!int.TryParse(port, out var intPort) || intPort < 1 || intPort > 65535)
                {
                    throw new ArgumentException($"{nameof(port)} is invalid value `{port}`");
                }

                connectionInfo = new ConnectionInfo(
                    host: host,
                    username: username,
                    password: password,
                    port: intPort);
            }

            if (!bool.TryParse(deleteEmptyFolders ?? $"{true}", out _deleteEmptyFolders))
            {
                throw new ArgumentException(
                    $"{nameof(deleteEmptyFolders)} is invalid value `{deleteEmptyFolders}`");
            }

            if (!int.TryParse(
                    maxConcurrentConnections ?? $"{DefaultMaxConcurrentConnections}",
                    out var maxConcurrentConnectionsInt) ||
                maxConcurrentConnectionsInt < 0)
            {
                throw new ArgumentException(
                    $"{nameof(maxConcurrentConnections)} is invalid value `{maxConcurrentConnections}`");
            }

            _ftpClientPool = FtpClientPool.GetForHost(
                connectionInfo,
                maxConcurrentConnectionsInt);
        }

        public IFileStoreContext FileStoreContext => throw new System.NotImplementedException();

        public async Task<bool> Delete(string path)
        {
            var (folder, _) = GetFolderFilename(path);

            using (var lease = await _ftpClientPool.Lease().ConfigureAwait(false))
            {
                try
                {
                    await lease.FtpClient.DeleteFileAsync(path).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new FtpException($"FTP error on Delete: {ex.Message}", ex);
                }

                if (!string.IsNullOrEmpty(folder))
                {
                    if (_deleteEmptyFolders) await DeleteFolderIfEmpty(lease.FtpClient, folder).ConfigureAwait(false);
                }
            }

            return true;
        }

        public async Task<bool> Exists(string path)
        {
            using (var lease = await _ftpClientPool.Lease().ConfigureAwait(false))
            {
                try
                {
                    return await FileExists(lease.FtpClient, path).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new FtpException($"FTP error on Exists: {ex.Message}", ex);
                }
            }
        }

        public async Task<IEnumerable<string>> GetFiles(string pathPrefix, string extension)
        {
            var (folder, _) = GetFolderFilename(pathPrefix);

            using (var lease = await _ftpClientPool.Lease().ConfigureAwait(false))
            {
                bool exists;

                try
                {
                    exists = string.IsNullOrEmpty(folder) ? true : await FolderExists(lease.FtpClient, folder);
                }
                catch (Exception ex)
                {
                    throw new FtpException($"FTP error on Exists in GetFiles: {ex.Message}", ex);
                }

                if (!exists) return new string[0];

                return (await GetFiles(lease.FtpClient, folder).ConfigureAwait(false))
                    .Where(f => f.StartsWith(pathPrefix) && f.EndsWith(extension))
                    .ToList();
            }
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(string path, bool getETag = false)
        {
            if (getETag) throw new NotSupportedException("FTP does not support ETag retrieval or optimistic concurrency.");

            using (var lease = await _ftpClientPool.Lease().ConfigureAwait(false))
            {
                byte[] bytes;

                try
                {
                    bytes = await lease.FtpClient.DownloadAsync(path, default).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new FtpException($"FTP error on DownloadFile in ReadAllBytes: {ex.Message}", ex);
                }

                return new FileStoreReadAllBytesResult(bytes);
            }
        }

        public async Task<bool> ReadStream(string path, Stream contents, bool getETag = false)
        {
            if (getETag) throw new NotSupportedException("FTP does not support ETag retrieval or optimistic concurrency.");

            using (var lease = await _ftpClientPool.Lease().ConfigureAwait(false))
            {
                try
                {
                    await lease.FtpClient.DownloadAsync(contents, path).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new FtpException($"FTP error on DownloadFile in ReadStream: {ex.Message}", ex);
                }
            }

            return true;
        }

        public async Task<bool> WriteAllBytes(string path, byte[] contents, string eTag = null)
        {
            var (folder, _) = GetFolderFilename(path);

            using (var lease = await _ftpClientPool.Lease().ConfigureAwait(false))
            {
                await EnsureFolderExists(lease.FtpClient, folder).ConfigureAwait(false);

                using (var input = new MemoryStream(contents))
                {
                    try
                    {
                        await lease.FtpClient.UploadAsync(contents, path).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        throw new FtpException($"FTP error on UploadFile in WriteAllBytes: {ex.Message}", ex);
                    }
                }

                return false;
            }
        }

        public async Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            if (eTag != null) throw new NotSupportedException("SFTP does not support ETag retrieval or optimistic concurrency.");

            var (folder, _) = GetFolderFilename(path);

            using (var lease = await _ftpClientPool.Lease().ConfigureAwait(false))
            {
                await EnsureFolderExists(lease.FtpClient, folder).ConfigureAwait(false);

                try
                {
                    await lease.FtpClient.UploadAsync(contents, path).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new FtpException(
                        $"SFTP error on UploadFile in WriteStream: {ex.Message}",
                        ex,
                        isRetryable: false);
                }
            }

            return true;
        }

        async Task<bool> FolderExists(FtpClient ftpClient, string path)
        {
            return await ftpClient.DirectoryExistsAsync(path).ConfigureAwait(false);
        }

        async Task<bool> FileExists(FtpClient ftpClient, string path)
        {
            return await ftpClient.FileExistsAsync(path).ConfigureAwait(false);
        }

        async Task<bool> FolderOrFileExists(FtpClient ftpClient, string path)
        {
            var folderExistsTask = ftpClient.DirectoryExistsAsync(path);

            var fileExistsTask = ftpClient.FileExistsAsync(path);

            await Task.WhenAll(folderExistsTask, fileExistsTask).ConfigureAwait(false);

            return folderExistsTask.Result || fileExistsTask.Result;
        }

        async Task<IEnumerable<string>> GetFiles(FtpClient ftpClient, string folder)
        {
            var files = new List<string>();

            IEnumerable<FtpListItem> entries;

            try
            {
                entries = await ftpClient.GetListingAsync(folder).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new FtpException($"FTP error on ListDirectory in GetFiles: {ex.Message}", ex);
            }

            foreach (var entry in entries)
            {
                var fullName = entry.FullName;

                if (fullName.EndsWith("/.") || fullName.EndsWith("/.."))
                {
                    continue;
                }

                if (entry.Type == FtpFileSystemObjectType.Directory)
                {
                    foreach (var file in (await GetFiles(ftpClient, fullName).ConfigureAwait(false)))
                    {
                        files.Add(file);
                    }
                }
                else
                {
                    files.Add(fullName.StartsWith("/") ?
                        fullName.Substring(1) :
                        fullName);
                }
            }

            return files;
        }

        async Task EnsureFolderExists(FtpClient ftpClient, string folder)
        {
            var parts = folder.Split(
                new char[] { '/', '\\' },
                StringSplitOptions.RemoveEmptyEntries);

            var path = string.Empty;

            foreach (var part in parts)
            {
                path = $"{path}/{part}";

                bool exists;

                try
                {
                    exists = await FolderExists(ftpClient, path).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new FtpException($"FTP error on Exists in EnsureFolderExists: {ex.Message}", ex);
                }

                if (!exists)
                {
                    try
                    {
                        await ftpClient.CreateDirectoryAsync(path).ConfigureAwait(false);
                    }
                    catch (FtpCommandException fcex)
                    {
                        if (fcex.Message == "Directory already exists")
                        {
                            // Already created, do nothing.
                        }
                        else
                        {
                            throw;
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new FtpException($"FTP error on CreateDirectory in EnsureFolderExists: {ex.Message}", ex);
                    }
                }
            }
        }

        async Task DeleteFolderIfEmpty(FtpClient ftpClient, string folder)
        {
            var parts = new List<string>(folder.Split(
                new char[] { '/', '\\' },
                StringSplitOptions.RemoveEmptyEntries));

            while (parts.Any())
            {
                var path = string.Join("/", parts);

                bool exists;

                try
                {
                    exists = await FolderExists(ftpClient, path).ConfigureAwait(false);
                }
                //catch (FtpPermissionDeniedException spdex)
                //{
                //    // parent was already deleted, so do nothing.

                //    parts.RemoveAt(parts.Count - 1);

                //    continue;
                //}
                catch (Exception ex)
                {
                    throw new FtpException($"FTP error on Exists in DeleteFolderIfEmpty: {ex.Message}", ex);
                }

                if (exists)
                {
                    IEnumerable<string> entries;

                    try
                    {
                        entries = await ftpClient.GetNameListingAsync(path).ConfigureAwait(false);
                    }
                    //catch (SshException sshex)
                    //{
                    //    if (sshex.Message == "Bad message")
                    //    {
                    //        // already deleted, so do nothing.

                    //        parts.RemoveAt(parts.Count - 1);

                    //        continue;
                    //    }
                    //    else throw;
                    //}
                    catch (Exception ex)
                    {
                        throw new FtpException($"FTP error on ListDirectory in DeleteFolderIfEmpty: {ex.Message}", ex);
                    }

                    if (!entries.Any(f => !(f.EndsWith("/.") || f.EndsWith("/.."))))
                    {
                        try
                        {
                            ftpClient.DeleteDirectory(path);
                        }
                        catch (FtpCommandException fcex)
                        {
                            if (fcex.Message == "Directory not found")
                            {
                                // already deleted, so do nothing.
                            }
                            else
                            {
                                throw;
                            }
                        }
                        //catch (FtpPathNotFoundException spnfex)
                        //{
                        //    // already deleted, so do nothing.
                        //}
                        //catch (FtpPermissionDeniedException spdex)
                        //{
                        //    // no longer empty, so do nothing.
                        //}
                        catch (Exception ex)
                        {
                            throw new FtpException($"FTP error on DeleteDirectory in DeleteFolderIfEmpty: {ex.Message}", ex);
                        }
                    }
                }

                parts.RemoveAt(parts.Count - 1);
            }
        }

        (string Folder, string Filename) GetFolderFilename(string path)
        {
            var i = path.LastIndexOf('/');

            if (i < 0) return (string.Empty, path);

            return (path.Substring(0, i), path.Substring(i + 1));
        }

        public async Task<TResult> ExecuteWithRetry<TResult>(Func<Task<TResult>> getTask)
        {
            return await _retryPolicy.ExecuteAsync(getTask).ConfigureAwait(false);
        }

        class ConnectionInfo
        { 
            public ConnectionInfo(
                string host,
                string username,
                string password,
                int port)
            {
                Host = host;
                
                Username = username;
                
                Password = password;
                
                Port = port;
            }

            public string Host { get; }
            
            public string Username { get; }
            
            public string Password { get; }
            
            public int Port { get; }
        }


        class FtpClientLease : IDisposable
        {
            readonly FtpClientPool _ftpClientPool;

            public FtpClientLease(
                FtpClientPool ftpClientPool,
                FtpClient ftpClient)
            {
                _ftpClientPool = ftpClientPool;

                FtpClient = ftpClient;
            }

            public FtpClient FtpClient { get; }

            public void Dispose() => _ftpClientPool.Return(FtpClient);
        }

        class FtpClientPool
        {
            static readonly TimeSpan _maxLeaseAttemptTime = TimeSpan.FromMinutes(2);

            static readonly TimeSpan _maxLingerTime = TimeSpan.FromSeconds(10);

            static ConcurrentDictionary<string, FtpClientPool> _poolsByHost = new ConcurrentDictionary<string, FtpClientPool>();

            readonly ConcurrentQueue<(DateTime TimeAdded, FtpClient FtpClient)> _pooledFtpClients =
                new ConcurrentQueue<(DateTime TimeAdded, FtpClient FtpClient)>();

            readonly ConnectionInfo _connectionInfo;

            int _maxConnectionCount;

            int _leaseCount;

            FtpClientPool(
                ConnectionInfo connectionInfo,
                int maxLeaseCount)
            {
                _connectionInfo = connectionInfo;

                _maxConnectionCount = maxLeaseCount;

                _ = CleanPool();
            }

            public static FtpClientPool GetForHost(
                ConnectionInfo connectionInfo,
                int maxLeaseCount)
            {
                var host = connectionInfo.Host;

                FtpClientPool ftpClientPool;

                if (_poolsByHost.TryGetValue(host, out ftpClientPool))
                {
                    ftpClientPool._maxConnectionCount = maxLeaseCount > 0 ?
                        Math.Min(maxLeaseCount, ftpClientPool._maxConnectionCount) :
                        ftpClientPool._maxConnectionCount;

                    return ftpClientPool;
                }

                ftpClientPool = new FtpClientPool(connectionInfo, maxLeaseCount);

                _poolsByHost[host] = ftpClientPool;

                return ftpClientPool;
            }

            public async Task<FtpClientLease> Lease()
            {
                var startTime = DateTime.UtcNow;

                while (true)
                {
                    if (_leaseCount + _pooledFtpClients.Count < _maxConnectionCount)
                    {
                        while (_pooledFtpClients.TryDequeue(out var fromPool))
                        {
                            var ftpClientFromPool = fromPool.FtpClient;

                            if (ftpClientFromPool.IsConnected)
                            {
                                Interlocked.Increment(ref _leaseCount);

                                return new FtpClientLease(this, ftpClientFromPool);
                            }
                        }

                        var ftpClient = new FtpClient(_connectionInfo.Host);

                        ftpClient.Credentials = new NetworkCredential(
                            userName: _connectionInfo.Username, 
                            password: _connectionInfo.Password);

                        //try
                        //{
                            ftpClient.Connect();
                        //}
                        //catch (SshOperationTimeoutException sotex)
                        //{
                        //    throw new FtpException($"FTP error on Connect in Lease: {sotex.Message}", sotex);
                        //}

                        Interlocked.Increment(ref _leaseCount);

                        return new FtpClientLease(this, ftpClient);
                    }

                    if ((DateTime.UtcNow - startTime) > _maxLeaseAttemptTime)
                    {
                        throw new TimeoutException("Timed out while trying to leas an FTP connection");
                    }

                    await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                }
            }

            public void Return(FtpClient ftpClient)
            {
                _pooledFtpClients.Enqueue((DateTime.UtcNow, ftpClient));

                Interlocked.Decrement(ref _leaseCount);
            }

            async Task CleanPool()
            {
                while (true)
                {
                    try
                    {
                        while (_pooledFtpClients.Count > 0 && _leaseCount + _pooledFtpClients.Count > _maxConnectionCount)
                        {
                            if (_pooledFtpClients.TryDequeue(out var dequeued))
                            {
                                var (_, ftpClient) = dequeued;

                                if (ftpClient.IsConnected)
                                {
                                    dequeued.FtpClient.Disconnect();
                                }
                            }
                        }

                        while (_pooledFtpClients.TryPeek(out var peeked) &&
                            (DateTime.UtcNow - peeked.TimeAdded) > _maxLingerTime)
                        {
                            if (_pooledFtpClients.TryDequeue(out var dequeued))
                            {
                                var (_, ftpClient) = dequeued;

                                if (ftpClient.IsConnected)
                                {
                                    dequeued.FtpClient.Disconnect();
                                }
                            }
                        }
                    }
                    catch { }

                    await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                }
            }
        }
    }
}
