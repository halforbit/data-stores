# Data Stores

**Data Stores** lets you store, retrieve, and query your data with a universal, low-ceremony pattern that abstracts away the implementation details of any given data store.

**Dive right in with the [Quick Start](docs/quick-start.md)**

Pluggable integrations are provided to many popular storage systems and techniques, including **Local Storage**, **DocumentDb**, **Azure Blob Storage**, **Google Drive**, **JSON**, **YAML**, **XML**, **GZIP**, as well as super-efficient **Bond** and **Protobuf** protocols. More is planned in the [Roadmap](docs/roadmap.md).

In the simplest form, Data Stores lets you use a file folder like a **NoSQL** data store. This folder can be stored on local disk, or one of many cloud file storage services. In this case, it could even be called a **NoDB** data store, since it doesn't require the installation or configuration of a database engine at all.

In the most robust form, Data Stores lets you integrate to many different kinds of data storage services, such as document databases. You can use the advanced indexing and querying abilities of these stores, while making code that is indifferent to their implementation details. 

You can easily describe and create a **context** that accesses data on many storage services using the unified `IDataStore` interface. This interface is easy to mock, making the approach friendly to unit testing as well.

## What Data Stores Can I Use?

Data Store integrations are provided for **Local Storage**, **DocumentDb**, **Blob Storage**, and **Google Drive**.

Serialization integrations are provided for **JSON**, **YAML**, **XML**, as well as super-efficient **Bond** and **Protobuf** protocols. Note that some serializers have special requirements for data classes to be serialized.

Optional compression via **GZIP** is provided as well. This can make a 10:1 difference in stored / transferred data payload size for formats like JSON.

## How Do I Get It?

Data Stores is available as a constellation of Nuget packages. You will want `Halforbit.DataStores`, and any of the other packages with the desired functionality:

[Halforbit.DataStores](https://www.nuget.org/packages/Halforbit.DataStores/)

[Halforbit.DataStores.DocumentStores.DocumentDb](https://www.nuget.org/packages/Halforbit.DataStores.DocumentStores.DocumentDb/)

[Halforbit.DataStores.FileStores.AmazonS3](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.AmazonS3/)

[Halforbit.DataStores.FileStores.BlobStorage](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.BlobStorage/)

[Halforbit.DataStores.FileStores.GoogleDrive](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.GoogleDrive/)

[Halforbit.DataStores.FileStores.Serialization.Bond](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Bond/)

[Halforbit.DataStores.FileStores.Serialization.Protobuf](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Protobuf/)

[Halforbit.DataStores.FileStores.Serialization.Yaml](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Yaml/)


[![Build status](https://ci.appveyor.com/api/projects/status/w8tliyvw96obytai?svg=true)](https://ci.appveyor.com/project/halforbit/data-stores)