using Google.Apis.Auth.OAuth2;
using Google.Apis.Download;
using Google.Apis.Drive.v3;
using Google.Apis.Drive.v3.Data;
using Google.Apis.Services;
using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using File = Google.Apis.Drive.v3.Data.File;
using MemoryStream = System.IO.MemoryStream;

namespace Halforbit.DataStores.FileStores.GoogleDrive.Implementation
{
    public class GoogleDriveFileStore : IFileStore
    {
        readonly DriveService _driveService;
        
        readonly string _grantAccessToEmails;

        public GoogleDriveFileStore(
            string applicationName,
            string serviceAccountEmail,
            string serviceAccountKey,
            string grantAccessToEmails)
        {
            var serviceAccountCredential = new ServiceAccountCredential(
                new ServiceAccountCredential.Initializer(serviceAccountEmail)
                {
                    Scopes = new[] { DriveService.Scope.Drive },

                    //User = "someone@gmail.com"
                }
                .FromPrivateKey(serviceAccountKey));

            _driveService = new DriveService(new BaseClientService.Initializer
            {
                ApplicationName = applicationName,

                HttpClientInitializer = serviceAccountCredential
            });
            
            _grantAccessToEmails = grantAccessToEmails;
        }

        public IFileStoreContext FileStoreContext => throw new NotImplementedException();

        public async Task<bool> Delete(string path)
        {
            var file = new File();

            file.Trashed = true;

            var request = _driveService.Files.Update(
                file,
                await ResolveFileId(path));

            request.Fields = "id";

            var response = await request.ExecuteAsync();

            //if (response.Exception != null) throw response.Exception;

            return true;
        }

        public async Task<bool> Exists(string path)
        {
            return (await ListFiles(path)).Any();
        }

        async Task<IReadOnlyList<File>> ListFiles(string path)
        {
            var request = _driveService.Files.List();

            request.Q = $"name='{path}' and trashed=false";

            var response = await request.ExecuteAsync();

            return response.Files.ToList();
        }

        public async Task<IEnumerable<string>> GetFiles(string pathPrefix, string extension)
        {
            var request = _driveService.Files.List();

            request.Q = $"trashed=false";

            var response = await request.ExecuteAsync();

            var l = response.Files.ToList();

            return l.Select(e => e.Name);
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path,
            bool getETag = false)
        {
            var fileId = await ResolveFileId(path);

            var request = _driveService.Files.Get(fileId);

            var destStream = new MemoryStream();

            var response = await request.DownloadAsync(destStream);

            while (response.Status == DownloadStatus.NotStarted || response.Status == DownloadStatus.Downloading)
            {
                await Task.Delay(1);
            }

            if (response.Exception != null) throw response.Exception;

            return new FileStoreReadAllBytesResult(bytes: destStream.ToArray());
        }

        public async Task<bool> WriteAllBytes(
            string path,
            byte[] contents,
            string eTag = null)
        {
            if (eTag != null) throw new NotSupportedException(
                 "ETag-based optimistic concurrency is not implemented on " + GetType().Name);

            var file = (await ListFiles(path)).SingleOrDefault();

            if (file == null)
            {
                file = new File
                {
                    Name = path
                };

                var fileStream = new MemoryStream(contents);

                var request = _driveService.Files.Create(
                    file,
                    fileStream,
                    "application/octet-stream");

                request.Fields = "id";

                var response = await request.UploadAsync();

                if (response.Exception != null) throw response.Exception;

                if (!string.IsNullOrWhiteSpace(_grantAccessToEmails))
                {
                    var emails = _grantAccessToEmails
                        .Split(
                            new char[] { ';', ',', ' ' },
                            StringSplitOptions.RemoveEmptyEntries)
                        .Select(e => e.Trim());

                    var permission = new Permission
                    {
                        Type = "user",

                        Role = "writer",

                        EmailAddress = _grantAccessToEmails

                        //AllowFileDiscovery = true
                    };

                    var permissionsRequest = _driveService.Permissions.Create(
                        permission,
                        request.ResponseBody.Id);

                    permissionsRequest.Fields = "id";

                    permissionsRequest.Execute();
                }
            }
            else
            {
                var sourceStream = new MemoryStream(contents);

                var request = _driveService.Files.Update(
                    new File(),
                    file.Id,
                    sourceStream,
                    "application/octet-stream");

                request.Fields = "id";

                var response = await request.UploadAsync();

                if (response.Exception != null) throw response.Exception;
            }

            return true;
        }

        async Task<string> ResolveFileId(string path)
        {
            var request = _driveService.Files.List();

            request.Q = $"name='{path}' and trashed=false";

            var response = request.Execute();

            var l = response.Files.ToList();

            return l.Select(i => i.Id).Single();
        }

        public Task<Stream> ReadStream(string path, bool getETag = false)
        {
            throw new NotImplementedException();
        }

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
