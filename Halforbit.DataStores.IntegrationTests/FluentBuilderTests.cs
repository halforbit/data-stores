using Halforbit.DataStores.DocumentStores.CosmosDb.Implementation;
using Halforbit.DataStores.DocumentStores.Model;
using Halforbit.DataStores.DocumentStores.PostgresMarten;
using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.AmazonS3.Implementation;
using Halforbit.DataStores.FileStores.BlobStorage.Implementation;
using Halforbit.DataStores.FileStores.Compression.GZip.Implementation;
using Halforbit.DataStores.FileStores.Ftp.Implementation;
//using Halforbit.DataStores.FileStores.GoogleDrive.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Bond.Implementation;
using Halforbit.DataStores.FileStores.Serialization.ByteSerialization.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation;
using Halforbit.DataStores.FileStores.Sftp.Implementation;
using Halforbit.DataStores.Implementation;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.Serialization.Protobuf.Implementation;
using Halforbit.DataStores.TableStores.AzureTables.Implementation;
using Halforbit.DataStores.Validation.Implementation;
using Halforbit.DataStores.Validation.Interface;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Microsoft.Extensions.Configuration;
using System;
using System.Reflection;
using Xunit;

namespace Halforbit.DataStores.IntegrationTests
{
    public class FluentBuilderTests
    {
        // File Stores ////////////////////////////////////////////////////////

        [Fact, Trait("Type", "Unit")]
        public void LocalStorage()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void BlobStorage()
        {
            var dataStore = DataStore
                .Build()
                .BlobStorage()
                .ConnectionString("UseDevelopmentStorage=true")
                .Container("container")
                .ContentType("application/json")
                .DefaultContentEncoding()
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<BlobFileStore>(fileStore);

            Assert.Equal("UseDevelopmentStorage=true", fileStore.Field<string>("_connectionString"));

            Assert.Equal("container", fileStore.Field<string>("_containerName"));

            Assert.Equal("application/json", fileStore.Field<string>("_contentType"));

            Assert.Equal(string.Empty, fileStore.Field<string>("_contentEncoding"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void BlobStorage_DefaultContentType_ContentEncoding()
        {
            var dataStore = DataStore
                .Build()
                .BlobStorage()
                .ConnectionString("UseDevelopmentStorage=true")
                .Container("container")
                .DefaultContentType()
                .ContentEncoding("gzip")
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<BlobFileStore>(fileStore);

            Assert.Equal("UseDevelopmentStorage=true", fileStore.Field<string>("_connectionString"));

            Assert.Equal("container", fileStore.Field<string>("_containerName"));

            Assert.Equal("application/octet-stream", fileStore.Field<string>("_contentType"));

            Assert.Equal("gzip", fileStore.Field<string>("_contentEncoding"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void AmazonS3()
        {
            var dataStore = DataStore
                .Build()
                .AmazonS3()
                .AccessKeyId("access-key-id")
                .SecretAccessKey("secret-access-key")
                .BucketName("bucket-name")
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<AmazonS3FileStore>(fileStore);

            Assert.Equal("access-key-id", fileStore.Field<string>("_accessKeyId"));

            Assert.Equal("secret-access-key", fileStore.Field<string>("_secretAccessKey"));

            Assert.Equal("bucket-name", fileStore.Field<string>("_bucketName"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        //[Fact, Trait("Type", "Unit")]
        //public void GoogleDrive()
        //{
        //    var dataStore = DataStore
        //        .Build()
        //        .GoogleDrive()
        //        .ApplicationName("application-name")
        //        .ServiceAccountEmail("service-account-email")
        //        .ServiceAccountKey("service-account-key")
        //        .ImpersonateEmail("impersonate-email")
        //        .AddPermissionsEmails("alfa", "bravo")
        //        .JsonSerialization()
        //        .NoCompression()
        //        .FileExtension(".json")
        //        .Map<Guid, string>("my-stuff/{this}")
        //        .NoValidation();

        //    Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

        //    var fileStore = dataStore.Field<IFileStore>("_fileStore");

        //    Assert.IsType<GoogleDriveFileStore>(fileStore);

        //    Assert.Equal("application-name", fileStore.Field<string>("_applicationName"));

        //    Assert.Equal("service-account-email", fileStore.Field<string>("_serviceAccountEmail"));

        //    Assert.Equal("service-account-key", fileStore.Field<string>("_serviceAccountKey"));

        //    Assert.Equal("impersonate-email", fileStore.Field<string>("_impersonateEmail"));

        //    Assert.Equal("impersonate-email; alfa; bravo", fileStore.Field<string>("_addPermissionsEmails"));

        //    Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

        //    Assert.Null(dataStore.Field<ICompressor>("_compressor"));

        //    Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

        //    Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

        //    Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        //}

        [Fact, Trait("Type", "Unit")]
        public void Ftp()
        {
            var dataStore = DataStore
                .Build()
                .Ftp()
                .Host("host")
                .Username("username")
                .Password("password")
                .DefaultPort()
                .DeleteEmptyFolders()
                .DefaultMaxConcurrentConnections()
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<FtpFileStore>(fileStore);

            Assert.Equal("host", fileStore.Field<string>("_host"));

            Assert.Equal("username", fileStore.Field<string>("_username"));

            Assert.Equal("password", fileStore.Field<string>("_password"));

            Assert.Equal(21, fileStore.Field<int>("_port"));

            Assert.True(fileStore.Field<bool>("_deleteEmptyFolders"));

            Assert.Equal(10, fileStore.Field<int>("_maxConcurrentConnections"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void Ftp_Port_DeleteEmptyFolders_MaxConcurrentConnections()
        {
            var dataStore = DataStore
                .Build()
                .Ftp()
                .Host("host")
                .Username("username")
                .Password("password")
                .Port(22)
                .KeepEmptyFolders()
                .MaxConcurrentConnections(11)
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<FtpFileStore>(fileStore);

            Assert.Equal("host", fileStore.Field<string>("_host"));

            Assert.Equal("username", fileStore.Field<string>("_username"));

            Assert.Equal("password", fileStore.Field<string>("_password"));

            Assert.Equal(22, fileStore.Field<int>("_port"));

            Assert.False(fileStore.Field<bool>("_deleteEmptyFolders"));

            Assert.Equal(11, fileStore.Field<int>("_maxConcurrentConnections"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void Sftp()
        {
            var dataStore = DataStore
                .Build()
                .Sftp()
                .Host("host")
                .Username("username")
                .Password("password")
                .DefaultPort()
                .DeleteEmptyFolders()
                .DefaultMaxConcurrentConnections()
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<SftpFileStore>(fileStore);

            Assert.Equal("host", fileStore.Field<string>("_host"));

            Assert.Equal("username", fileStore.Field<string>("_username"));

            Assert.Equal("password", fileStore.Field<string>("_password"));

            Assert.Equal(21, fileStore.Field<int>("_port"));

            Assert.True(fileStore.Field<bool>("_deleteEmptyFolders"));

            Assert.Equal(10, fileStore.Field<int>("_maxConcurrentConnections"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void Sftp_Port_DeleteEmptyFolders_MaxConcurrentConnections()
        {
            var dataStore = DataStore
                .Build()
                .Sftp()
                .Host("host")
                .Username("username")
                .Password("password")
                .Port(22)
                .KeepEmptyFolders()
                .MaxConcurrentConnections(11)
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<SftpFileStore>(fileStore);

            Assert.Equal("host", fileStore.Field<string>("_host"));

            Assert.Equal("username", fileStore.Field<string>("_username"));

            Assert.Equal("password", fileStore.Field<string>("_password"));

            Assert.Equal(22, fileStore.Field<int>("_port"));

            Assert.False(fileStore.Field<bool>("_deleteEmptyFolders"));

            Assert.Equal(11, fileStore.Field<int>("_maxConcurrentConnections"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        // Document Stores ////////////////////////////////////////////////////

        [Fact, Trait("Type", "Unit")]
        public void CosmosDb()
        {
            var dataStore = DataStore
                .Build()
                .CosmosDb()
                .ConnectionString("connection-string")
                .Database("database")
                .Container("container")
                .Map<Guid, MyDocument>(d => d.AccountId, "key-map/{this}")
                .NoValidation();

            Assert.IsType<CosmosDbDataStore<Guid, MyDocument>>(dataStore);

            Assert.Equal("connection-string", dataStore.Field<string>("_connectionString"));

            Assert.Equal("database", dataStore.Field<string>("_databaseId"));

            Assert.Equal("container", dataStore.Field<string>("_containerId"));

            Assert.Equal("{AccountId:D}|key-map/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void PostgresMarten()
        {
            var dataStore = DataStore
                .Build()
                .PostgresMarten()
                .ConnectionString("connection-string")
                .Map<Guid, MyDocument>("key-map/{this}")
                .NoValidation();

            Assert.IsType<PostgresMartenDataStore<Guid, MyDocument>>(dataStore);

            Assert.Equal("connection-string", dataStore.Field<string>("_connectionString"));

            Assert.Equal("key-map/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        // Table Stores ///////////////////////////////////////////////////////

        [Fact, Trait("Type", "Unit")]
        public void TableStorage()
        {
            var dataStore = DataStore
                .Build()
                .AzureTables()
                .ConnectionString("connection-string")
                .Table("table")
                .Map<Guid, MyDocument>("partition-map|key-map")
                .NoValidation();

            Assert.IsType<AzureTableStore<Guid, MyDocument>>(dataStore);

            Assert.Equal("connection-string", dataStore.Field<string>("_connectionString"));

            Assert.Equal("table", dataStore.Field<string>("_tableName"));

            Assert.Equal("partition-map|key-map", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        // Serialization //////////////////////////////////////////////////////

        // JsonOptions

        // YamlOptions

        [Fact, Trait("Type", "Unit")]
        public void YamlSerialization()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .YamlSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<YamlSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void ByteSerialization()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .ByteSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<ByteSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void BondSerialization()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .BondSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<BondSimpleBinarySerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void ProtobufSerialization()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .ProtobufSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<ProtobufSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        // Compression ////////////////////////////////////////////////////////

        [Fact, Trait("Type", "Unit")]
        public void GZipCompression()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .JsonSerialization()
                .GZipCompression()
                .FileExtension(".json.gz")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.IsType<GZipCompressor>(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json.gz", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void LzmaCompression()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .JsonSerialization()
                .LzmaCompression()
                .FileExtension(".json.lzma")
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.IsType<LzmaCompressor>(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json.lzma", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        // Validation /////////////////////////////////////////////////////////

        [Fact, Trait("Type", "Unit")]
        public void Validate()
        {
            var dataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<Guid, string>("my-stuff/{this}")
                .Validation(new MyValidator());

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.IsType<MyValidator>(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        // Singleton //////////////////////////////////////////////////////////

        [Fact, Trait("Type", "Unit")]
        public void Singleton()
        {
            var singletonDataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<string>("my-stuff")
                .NoValidation();

            Assert.IsType<SingletonDataStore<string>>(singletonDataStore);

            var dataStore = singletonDataStore.Field<IDataStore<object, string>>("_source");

            Assert.IsType<FileStoreDataStore<object, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff", dataStore.Field<StringMap<object>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void Singleton_Validation()
        {
            var singletonDataStore = DataStore
                .Build()
                .LocalStorage()
                .RootPath("c:/data")
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json")
                .Map<string>("my-stuff")
                .Validation(new MySingletonValidator());

            Assert.IsType<SingletonDataStore<string>>(singletonDataStore);

            var dataStore = singletonDataStore.Field<IDataStore<object, string>>("_source");

            Assert.IsType<FileStoreDataStore<object, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff", dataStore.Field<StringMap<object>>("_keyMap").Source);

            Assert.IsType<MySingletonValidator>(dataStore.Field<IValidator<object, string>>("_validator"));
        }

        // Location & Format //////////////////////////////////////////////////

        [Fact, Trait("Type", "Unit")]
        public void FileStore_Location_Format()
        {
            var dataStore = DataStore
                .Location(Location.LocalStorage.Data)
                .Format(Format.Structured.Json)
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void FileStore_Config_Location_Format()
        {
            var dataStore = DataStore
                .Location("c:/data", Location.LocalStorage.Data)
                .Format(Format.Structured.Json)
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<LocalFileStore>(fileStore);

            Assert.Equal("c:/data", fileStore.Field<string>("_rootPath"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void BlobStore_Location_Format()
        {
            var dataStore = DataStore
                .Location(Location.BlobStorage.MyStorageAccount.MyContainer)
                .Format(Format.Structured.Json)
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<BlobFileStore>(fileStore);

            Assert.Equal("alfa", fileStore.Field<string>("_connectionString"));

            Assert.Equal("bravo", fileStore.Field<string>("_containerName"));

            Assert.Equal("application/json", fileStore.Field<string>("_contentType"));

            Assert.Equal(string.Empty, fileStore.Field<string>("_contentEncoding"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void BlobStore_Config_Location_Format()
        {
            var dataStore = DataStore
                .Location("alfa", Location.BlobStorage.MyStorageAccount.MyContainer)
                .Format(Format.Structured.Json)
                .Map<Guid, string>("my-stuff/{this}")
                .NoValidation();

            Assert.IsType<FileStoreDataStore<Guid, string>>(dataStore);

            var fileStore = dataStore.Field<IFileStore>("_fileStore");

            Assert.IsType<BlobFileStore>(fileStore);

            Assert.Equal("alfa", fileStore.Field<string>("_connectionString"));

            Assert.Equal("bravo", fileStore.Field<string>("_containerName"));

            Assert.Equal("application/json", fileStore.Field<string>("_contentType"));

            Assert.Equal(string.Empty, fileStore.Field<string>("_contentEncoding"));

            Assert.IsType<JsonSerializer>(dataStore.Field<ISerializer>("_serializer"));

            Assert.Null(dataStore.Field<ICompressor>("_compressor"));

            Assert.Equal(".json", dataStore.Field<string>("_fileExtension"));

            Assert.Equal("my-stuff/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void DocumentStore_Location_Format()
        {
            var dataStore = DataStore
                .Location(Location.CosmosDb.MyInstance.MyDatabase.MyContainer)
                .Map<Guid, MyDocument>(d => d.AccountId, "key-map/{this}")
                .NoValidation();

            Assert.IsType<CosmosDbDataStore<Guid, MyDocument>>(dataStore);

            Assert.Equal("alfa", dataStore.Field<string>("_connectionString"));

            Assert.Equal("bravo", dataStore.Field<string>("_databaseId"));

            Assert.Equal("charlie", dataStore.Field<string>("_containerId"));

            Assert.Equal("{AccountId:D}|key-map/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }

        [Fact, Trait("Type", "Unit")]
        public void DocumentStore_Config_Location_Format()
        {
            var dataStore = DataStore
                .Location("alfa", Location.CosmosDb.MyInstance.MyDatabase.MyContainer)
                .Map<Guid, MyDocument>(d => d.AccountId, "key-map/{this}")
                .NoValidation();

            Assert.IsType<CosmosDbDataStore<Guid, MyDocument>>(dataStore);

            Assert.Equal("alfa", dataStore.Field<string>("_connectionString"));

            Assert.Equal("bravo", dataStore.Field<string>("_databaseId"));

            Assert.Equal("charlie", dataStore.Field<string>("_containerId"));

            Assert.Equal("{AccountId:D}|key-map/{this}", dataStore.Field<StringMap<Guid>>("_keyMap").Source);

            Assert.Null(dataStore.Field<IValidator<Guid, string>>("_validator"));
        }
    }

    public class Location
    {
        public class LocalStorage
        {
            public static INeedsSerialization Data(string config, INeedsIntegration s) => s
                .LocalStorage()
                .RootPath(config);

            public static INeedsSerialization Data(INeedsIntegration s) => s
                .LocalStorage()
                .RootPath("c:/data");
        }
        
        public class BlobStorage
        {
            public class MyStorageAccount
            {
                public static INeedsContentType MyContainer(string config, INeedsIntegration s) => s
                    .BlobStorage()
                    .ConnectionString(config)
                    .Container("bravo");

                public static INeedsContentType MyContainer(INeedsIntegration s) => s
                    .BlobStorage()
                    .ConnectionString("alfa")
                    .Container("bravo");
            }
        }

        public class CosmosDb
        {
            public class MyInstance
            {
                public class MyDatabase
                {
                    public static INeedsDocumentMap MyContainer(string config, INeedsIntegration s) => s
                        .CosmosDb()
                        .ConnectionString(config)
                        .Database("bravo")
                        .Container("charlie");

                    public static INeedsDocumentMap MyContainer(INeedsIntegration s) => s
                        .CosmosDb()
                        .ConnectionString("alfa")
                        .Database("bravo")
                        .Container("charlie");
                }
            }
        }
    }

    public class Format
    {
        public class Structured
        {
            public static INeedsMap Json(INeedsContentType s) => s
                .ContentType("application/json")
                .DefaultContentEncoding()
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json");

            public static INeedsMap Json(INeedsSerialization s) => s
                .JsonSerialization()
                .NoCompression()
                .FileExtension(".json");
        }
    }

    public class MyDocument : Document
    {
        public Guid AccountId { get; }
    }

    public class MyValidator : ValidatorBase<Guid, string>
    {
        public MyValidator(params ValidatorBase<Guid, string>[] prerequisites) : base(prerequisites) { }
    }

    public class MySingletonValidator : ValidatorBase<object, string>
    {
        public MySingletonValidator(params ValidatorBase<object, string>[] prerequisites) : base(prerequisites) { }
    }

    public interface IMyDataContext
    {
        IDataStore<Guid, string> MyStrings { get; }

        IDataStore<string> MySingleton { get; }

        IDataStore<Guid, MyDocument> MyDocuments { get; }
    }

    public interface IResolver
    {
        TService Resolve<TService>();
    }

    public class MyDataContext : IMyDataContext
    {
        readonly IConfiguration _config;
        readonly IResolver _resolver;

        public MyDataContext(
            IConfiguration config,
            IResolver resolver)
        {
            _config = config;
            _resolver = resolver;
        }

        public IDataStore<Guid, string> MyStrings => DataStore
            .Location(_config["ConnectionString"], Location.BlobStorage.MyStorageAccount.MyContainer)
            .Format(Format.Structured.Json)
            .Map<Guid, string>("my-stuff-2/{this}")
            .Validation(_resolver.Resolve<MyValidator>());

        public IDataStore<string> MySingleton => DataStore
            .Location(_config["ConnectionString"], Location.BlobStorage.MyStorageAccount.MyContainer)
            .Format(Format.Structured.Json)
            .Map<string>("my-string")
            .NoValidation();

        public IDataStore<Guid, MyDocument> MyDocuments => DataStore
            .Location(_config["ConnectionString"], Location.CosmosDb.MyInstance.MyDatabase.MyContainer)
            .Map<Guid, MyDocument>(v => v.AccountId, "my-documents/{this}")
            .NoValidation();
    }

    public static class PeekExtensions
    { 
        public static TField Field<TField>(this object obj, string field)
        {
            return (TField)obj
                .GetType()
                .GetField(field, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(obj);
        }
    }
}
