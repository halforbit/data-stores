using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using Halforbit.DataStores.FileStores.Sftp.Exceptions;
using Halforbit.Facets.Attributes;
using Polly;
using Polly.Retry;
using Renci.SshNet;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Sftp.Implementation
{
    public class SftpFileStore : IFileStore, IRetryExecutor
    {
        const int DefaultMaxConcurrentConnections = 10;

        static readonly RetryPolicy _retryPolicy = Policy
            .Handle<SftpException>()
            .WaitAndRetryAsync(
                retryCount: 5,
                sleepDurationProvider: (count, exception, context) =>
                {
                    return TimeSpan.FromSeconds(Math.Pow(2, count));
                },
                onRetryAsync: (exception, timespan, count, context) => Task.CompletedTask);

        readonly SftpClientPool _sftpClientPool;

        readonly bool _deleteEmptyFolders;

        public SftpFileStore(
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
                    host,
                    username,
                    new PasswordAuthenticationMethod(username, password));
            }
            else
            {
                if (!int.TryParse(port, out var intPort) || intPort < 1 || intPort > 65535)
                {
                    throw new ArgumentException($"{nameof(port)} is invalid value `{port}`");
                }

                connectionInfo = new ConnectionInfo(
                    host,
                    intPort,
                    username,
                    new PasswordAuthenticationMethod(username, password));
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

            _sftpClientPool = SftpClientPool.GetForHost(
                connectionInfo,
                maxConcurrentConnectionsInt);
        }

        public IFileStoreContext FileStoreContext => throw new System.NotImplementedException();

        public async Task<bool> Delete(string path)
        {
            var (folder, _) = GetFolderFilename(path);

            using (var lease = await _sftpClientPool.Lease().ConfigureAwait(false))
            {
                try
                {
                    lease.SftpClient.Delete(path);
                }
                catch (Exception ex)
                {
                    throw new SftpException($"SFTP error on Delete: {ex.Message}", ex);
                }

                if (!string.IsNullOrEmpty(folder))
                {
                    if (_deleteEmptyFolders) DeleteFolderIfEmpty(lease.SftpClient, folder);
                }
            }

            return true;
        }

        public async Task<bool> Exists(string path)
        {
            using (var lease = await _sftpClientPool.Lease().ConfigureAwait(false))
            {
                try
                { 
                    return lease.SftpClient.Exists(path);
                }
                catch (Exception ex)
                {
                    throw new SftpException($"SFTP error on Exists: {ex.Message}", ex);
                }
            }
        }

        public async Task<IEnumerable<string>> GetFiles(string pathPrefix, string extension)
        {
            var (folder, _) = GetFolderFilename(pathPrefix);

            using (var lease = await _sftpClientPool.Lease().ConfigureAwait(false))
            {
                bool exists;

                try
                {
                    exists = string.IsNullOrEmpty(folder) ? true : lease.SftpClient.Exists(folder);
                }
                catch (Exception ex)
                {
                    throw new SftpException($"SFTP error on Exists in GetFiles: {ex.Message}", ex);
                }

                if (!exists) return new string[0];

                return GetFiles(lease.SftpClient, folder)
                    .Where(f => f.StartsWith(pathPrefix) && f.EndsWith(extension))
                    .ToList();
            }
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(string path, bool getETag = false)
        {
            if (getETag) throw new NotSupportedException("SFTP does not support ETag retrieval or optimistic concurrency.");

            using (var lease = await _sftpClientPool.Lease().ConfigureAwait(false))
            {
                byte[] bytes;

                using (var output = new MemoryStream())
                {
                    try
                    { 
                        lease.SftpClient.DownloadFile(path, output);
                    }
                    catch (Exception ex)
                    {
                        throw new SftpException($"SFTP error on DownloadFile in ReadAllBytes: {ex.Message}", ex);
                    }

                    bytes = output.ToArray();
                }

                return new FileStoreReadAllBytesResult(bytes);
            }
        }

        public async Task<bool> ReadStream(string path, Stream contents, bool getETag = false)
        {
            throw new System.NotImplementedException();
        }

        public async Task<bool> WriteAllBytes(string path, byte[] contents, string eTag = null)
        {
            var (folder, _) = GetFolderFilename(path);

            using (var lease = await _sftpClientPool.Lease().ConfigureAwait(false))
            {
                EnsureFolderExists(lease.SftpClient, folder);

                using (var input = new MemoryStream(contents))
                {
                    try
                    {
                        lease.SftpClient.UploadFile(input, path);
                    }
                    catch (Exception ex)
                    {
                        throw new SftpException($"SFTP error on UploadFile in WriteAllBytes: {ex.Message}", ex);
                    }
                }

                return false;
            }
        }

        public async Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            throw new System.NotImplementedException();
        }

        IEnumerable<string> GetFiles(SftpClient sftpClient, string folder)
        {
            IEnumerable<SftpFile> entries;

            try
            {
                entries = sftpClient.ListDirectory(folder);
            }
            catch (Exception ex)
            {
                throw new SftpException($"SFTP error on ListDirectory in GetFiles: {ex.Message}", ex);
            }

            foreach (var entry in entries)
            {
                var fullName = entry.FullName;

                if (fullName.EndsWith("/.") || fullName.EndsWith("/.."))
                {
                    continue;
                }

                if (entry.IsDirectory)
                {
                    foreach (var file in GetFiles(sftpClient, fullName))
                    {
                        yield return file;
                    }
                }
                else
                {
                    yield return fullName.StartsWith("/") ?
                        fullName.Substring(1) :
                        fullName;
                }
            }
        }

        void EnsureFolderExists(SftpClient sftpClient, string folder)
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
                    exists = sftpClient.Exists(path);
                }
                catch (Exception ex)
                {
                    throw new SftpException($"SFTP error on Exists in EnsureFolderExists: {ex.Message}", ex);
                }

                if (!exists)
                {
                    try
                    {
                        sftpClient.CreateDirectory(path);
                    }
                    catch (Exception ex)
                    {
                        throw new SftpException($"SFTP error on CreateDirectory in EnsureFolderExists: {ex.Message}", ex);
                    }
                }
            }
        }

        void DeleteFolderIfEmpty(SftpClient sftpClient, string folder)
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
                    exists = sftpClient.Exists(path);
                }
                catch (SftpPermissionDeniedException spdex)
                {
                    // parent was already deleted, so do nothing.

                    parts.RemoveAt(parts.Count - 1);

                    continue;
                }
                catch (Exception ex)
                {
                    throw new SftpException($"SFTP error on Exists in DeleteFolderIfEmpty: {ex.Message}", ex);
                }

                if (exists)
                {
                    IEnumerable<SftpFile> entries;

                    try
                    {
                        entries = sftpClient.ListDirectory(path);
                    }
                    catch (SshException sshex)
                    {
                        if (sshex.Message == "Bad message")
                        {
                            // already deleted, so do nothing.

                            parts.RemoveAt(parts.Count - 1);

                            continue;
                        }
                        else throw;
                    }
                    catch (Exception ex)
                    {
                        throw new SftpException($"SFTP error on ListDirectory in DeleteFolderIfEmpty: {ex.Message}", ex);
                    }

                    if (!entries.Any(f => !(f.FullName.EndsWith("/.") || f.FullName.EndsWith("/.."))))
                    {
                        try
                        {
                            sftpClient.DeleteDirectory(path);
                        }
                        catch (SftpPathNotFoundException spnfex)
                        {
                            // already deleted, so do nothing.
                        }
                        catch (SftpPermissionDeniedException spdex)
                        {
                            // no longer empty, so do nothing.
                        }
                        catch (Exception ex)
                        {
                            throw new SftpException($"SFTP error on DeleteDirectory in DeleteFolderIfEmpty: {ex.Message}", ex);
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

        class SftpClientLease : IDisposable
        {
            readonly SftpClientPool _sftpClientPool;

            public SftpClientLease(
                SftpClientPool sftpClientPool,
                SftpClient sftpClient)
            {
                _sftpClientPool = sftpClientPool;

                SftpClient = sftpClient;
            }

            public SftpClient SftpClient { get; }

            public void Dispose() => _sftpClientPool.Return(SftpClient);
        }

        class SftpClientPool
        {
            static readonly TimeSpan _maxLingerTime = TimeSpan.FromSeconds(10);

            static ConcurrentDictionary<string, SftpClientPool> _poolsByHost = new ConcurrentDictionary<string, SftpClientPool>();

            readonly ConcurrentQueue<(DateTime TimeAdded, SftpClient SftpClient)> _pooledSftpClients =
                new ConcurrentQueue<(DateTime TimeAdded, SftpClient SftpClient)>();

            readonly ConnectionInfo _connectionInfo;

            int _maxConnectionCount;

            int _leaseCount;

            SftpClientPool(
                ConnectionInfo connectionInfo,
                int maxLeaseCount)
            {
                _connectionInfo = connectionInfo;

                _maxConnectionCount = maxLeaseCount;

                _ = CleanPool();
            }

            public static SftpClientPool GetForHost(
                ConnectionInfo connectionInfo, 
                int maxLeaseCount)
            {
                var host = connectionInfo.Host;

                SftpClientPool sftpClientPool;

                if (_poolsByHost.TryGetValue(host, out sftpClientPool))
                {
                    sftpClientPool._maxConnectionCount = maxLeaseCount > 0 ? 
                        Math.Min(maxLeaseCount, sftpClientPool._maxConnectionCount) :
                        sftpClientPool._maxConnectionCount;

                    return sftpClientPool;
                }

                sftpClientPool = new SftpClientPool(connectionInfo, maxLeaseCount);

                _poolsByHost[host] = sftpClientPool;

                return sftpClientPool;
            }

            public async Task<SftpClientLease> Lease()
            {
                while (true)
                {
                    if (_leaseCount < _maxConnectionCount)
                    {
                        while (_pooledSftpClients.TryDequeue(out var fromPool))
                        {
                            var sftpClientFromPool = fromPool.SftpClient;

                            if (sftpClientFromPool.IsConnected)
                            {
                                Interlocked.Increment(ref _leaseCount);

                                return new SftpClientLease(this, sftpClientFromPool);
                            }
                        }

                        var sftpClient = new SftpClient(_connectionInfo);

                        try
                        {
                            sftpClient.Connect();
                        }
                        catch (SshOperationTimeoutException sotex)
                        {
                            throw new SftpException($"SFTP error on Connect in Lease: {sotex.Message}", sotex);
                        }

                        return new SftpClientLease(this, sftpClient);
                    }

                    await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                }
            }

            public void Return(SftpClient sftpClient)
            {
                _pooledSftpClients.Enqueue((DateTime.UtcNow, sftpClient));

                Interlocked.Decrement(ref _leaseCount);
            }

            async Task CleanPool()
            {
                while (true)
                {
                    try
                    {
                        while (_pooledSftpClients.Count > 0 && _leaseCount + _pooledSftpClients.Count > _maxConnectionCount)
                        {
                            if (_pooledSftpClients.TryDequeue(out var dequeued))
                            {
                                var (_, sftpClient) = dequeued;

                                if (sftpClient.IsConnected)
                                {
                                    dequeued.SftpClient.Disconnect();
                                }
                            }
                        }

                        while (_pooledSftpClients.TryPeek(out var peeked) && 
                            (DateTime.UtcNow - peeked.TimeAdded) > _maxLingerTime)
                        {
                            if (_pooledSftpClients.TryDequeue(out var dequeued))
                            {
                                var (_, sftpClient) = dequeued;

                                if (sftpClient.IsConnected)
                                {
                                    dequeued.SftpClient.Disconnect();
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
