using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.AmazonS3.Implementation
{
    // TODO: Auto-create non-extant buckets

    // TODO: Deal with the 1000 item response limit.

    public class AmazonS3FileStore : IFileStore
    {
        string _accessKeyId;

        string _secretAccessKey;

        string _bucketName;

        public AmazonS3FileStore(
            string accessKeyId,
            string secretAccessKey,
            string bucketName)
        {
            _accessKeyId = accessKeyId;

            _secretAccessKey = secretAccessKey;

            _bucketName = bucketName;
        }

        public IFileStoreContext FileStoreContext => throw new NotImplementedException();

        public async Task<bool> Delete(string path)
        {
            using (var client = GetClient())
            {
                var request = new DeleteObjectRequest
                {
                    BucketName = _bucketName,

                    Key = path
                };

                var response = await client.DeleteObjectAsync(request);

                if (response.HttpStatusCode != HttpStatusCode.NoContent)
                {
                    throw new Exception("HTTP " + response.HttpStatusCode);
                }

                return true;
            }
        }

        public async Task<bool> Exists(string path)
        {
            return (await GetFiles(path, "")).Any(f => path == f);
        }

        public async Task<IEnumerable<string>> GetFiles(
            string pathPrefix,
            string extension)
        {
            using (var client = GetClient())
            {
                var request = new ListObjectsRequest
                {
                    BucketName = _bucketName,

                    Prefix = pathPrefix
                };

                var response = await client.ListObjectsAsync(request);

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    throw new Exception("HTTP " + response.HttpStatusCode);
                }

                return response.S3Objects
                    .Select(o => o.Key)
                    .Where(k => k.EndsWith(extension));
            }
        }

        public async Task<FileStoreReadAllBytesResult> ReadAllBytes(
            string path,
            bool getETag = false)
        {
            using (var destStream = new MemoryStream())
            using (var client = GetClient())
            {
                var request = new GetObjectRequest
                {
                    BucketName = _bucketName,

                    Key = path
                };

                var response = await client.GetObjectAsync(request);

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    throw new Exception("HTTP " + response.HttpStatusCode);
                }

                response.ResponseStream.CopyTo(destStream);

                return new FileStoreReadAllBytesResult(bytes: destStream.ToArray());
            }
        }

        public Task<bool> ReadStream(string path, Stream contents, bool getETag = false)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> WriteAllBytes(
            string path,
            byte[] contents,
            string eTag = null)
        {
            if (eTag != null) throw new NotSupportedException(
                 "ETag-based optimistic concurrency is not implemented on " + GetType().Name);

            using (var sourceStream = new MemoryStream(contents))
            using (var client = GetClient())
            {
                var request = new PutObjectRequest
                {
                    BucketName = _bucketName,

                    Key = path,

                    InputStream = sourceStream
                };

                var response = await client.PutObjectAsync(request);

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    throw new Exception("HTTP " + response.HttpStatusCode);
                }

                return true;
            }
        }

        public Task<bool> WriteStream(string path, Stream contents, string eTag = null)
        {
            throw new NotImplementedException();
        }

        AmazonS3Client GetClient() => new AmazonS3Client(
            awsAccessKeyId: _accessKeyId,
            awsSecretAccessKey: _secretAccessKey,
            region: RegionEndpoint.USEast1);
    }
}
