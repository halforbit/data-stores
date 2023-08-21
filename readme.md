# Halforbit Data Stores

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) &nbsp;[![Build status](https://ci.appveyor.com/api/projects/status/w8tliyvw96obytai?svg=true)](https://ci.appveyor.com/project/halforbit/data-stores) &nbsp;[![Nuget Package](https://img.shields.io/nuget/v/Halforbit.DataStores.svg)](#nuget-packages)

Data Stores lets you use simple cloud storage to create serverless, distributed, highly available, low-cost databases and caches with sophisticated keying, richly structured values, and compression at-rest. 

Data Stores integrates with a wide variety of storage providers and formats, all using a single, simple abstraction. 

## Features

- **Inexpensive Serverless Databases:** Use simple cloud storage to create serverless, distributed, highly available, low-cost databases. 

- **Easy Distributed Caches:** Create persistent caches to increase performance of backend API controllers, or reduce the number of calls to expensive third-party web APIs.

- **Many Supported Storage Providers:** Including **Azure Blobs**, **Azure Tables**, **Amazon S3**, **FTP**, **SFTP**, and **Local File Storage**.

- **Many Supported Formats:** Including human-readable structured data like **JSON** and **YAML**, efficient structured binary data with **Bond** or **Protobuf**, delimited tabular data like **CSV** and **TSV**, and binary data such as **images** and **raw text**. 

- **Optional Compression:** Flip a switch and compress data payloads up to 10x with **GZip** or **LZMA**, improving response times and reducing storage and bandwidth costs.

- **One Easy Abstraction:** Trade in your error prone bare metal integrations and high ceremony repository patterns for a single, elegant `IDataStore` interface that is uniform across storage platforms and formats.

## Getting Started

1. **Install NuGet Packages:** Install the NuGet packages for your desired storage providers and formats:
    ```powershell
    Install-Package Halforbit.DataStores
    ```
    See the [NuGet Packages](#nuget-packages) section below for a list of available NuGet packages and what storage providers and formats they support.

2. **Define Your Stores:** Use the `DataStore` type to create ad-hoc stores, or define them as properties on a data context.
   
3. **Use Your Stores:** Persist and retrieve data with your stores, or inspect write times, sizes, metadata and more with the stores' `Context` property.

There are many options to choose from when creating a store. A store can:
- be defined as a property of a **data context**, or created **ad-hoc** and stored in a local variable. 
- be **keyed** with **simple** types like `Guid` or `string`, with the **properties** of a `class`, `record`, or `struct`, or be a keyless **singleton** storing a single value.
- store **values** as **simple** types like `string` or `byte[]`, or **structured** `class`, `record`, or `struct` types serialized as e.g. JSON or TSV.


## Example Usage


Let's make a simple ad-hoc store to persist a `record` value using Azure Blobs, formatted as JSON, and key it with the properties of a `record` key.

First, we define a type to represent our **value**:

```cs
public record Person(
    string Department,
    Guid PersonId,
    string FirstName,
    string LastName);
```

Next we define a type to represent our **key**:

```cs
public record PersonKey(
    string Department,
    Guid PersonId);
```

Next we create a store ad-hoc with a key type of `PersonKey`, and a value type of `Person`:

```cs
var store = DataStore
    .Describe()
    .BlobStorage()
    .ConnectionString("<connection-string-here>")
    .Container("<container-name-here>")
    .ContentType("application/json")
    .DefaultContentEncoding()
    .JsonSerialization()
    .NoCompression()
    .FileExtension(".json")
    .Map<PersonKey, Person>(k => $"people/{k.Department}/{k.PersonId}")
    .Build();
```

The `Map<,>` method lets us define a bi-directional mapping between a `PersonKey` and a `string`. 

Note that `people` here is acting similarly to a table or collection name, and the `Department` property is acting as a kind of partition key.

Now we can put a `Person` in the store with the `Upsert` method:

```cs
var key = new PersonKey(
    Department: "development",
    PersonId: Guid.NewGuid());

var value = new Person(
    Department: key.Department,
    PersonId: key.PersonId,
    FirstName: "Steve",
    LastName: "Smith");

await store.Upsert(key, value);
```

We can get a `Person` by their `PersonKey`:

```cs
var value = await store.Get(key);
```

We can delete a `Person` by their `PersonKey`:

```cs
await store.Delete(key);
```

We can list the `PersonKey`s in the store, optionally filtered by `PersonKey.Department`:

```cs
var keys = await store.ListKeys(k => k.Department == "development");
```

We can get all of the `Person`s in the store, optionally filtered by `PersonKey.Department`:

```cs
var values = await store.ListValues(k => k.Department == "development");
```

<a name="nuget-packages"></a>
## NuGet Packages

The following NuGet packages are provided, parted out by their dependencies. Install the ones that contain the storage providers and formats you wish to use.

| Storage Provider or Format | NuGet Package |
|----------------------------|---------------|
| Local File Storage, JSON Serialization, Raw Byte Serialization, GZip Compression | [`Halforbit.DataStores`](https://www.nuget.org/packages/Halforbit.DataStores) |
| Azure Blob Storage | [`Halforbit.DataStores.FileStores.BlobStorage`](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.BlobStorage) |
| Azure Table Storage | [`Halforbit.DataStores.TableStores.AzureTables`](https://www.nuget.org/packages/Halforbit.DataStores.TableStores.AzureTables) |
| Amazon S3 Storage | [`Halforbit.DataStores.FileStores.AmazonS3`](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.AmazonS3) |
| YAML Serialization | [`Halforbit.DataStores.FileStores.Serialization.Yaml`](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Yaml) |
| Protobuf Serialization | [`Halforbit.DataStores.FileStores.Serialization.Protobuf`](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Protobuf) |
| Bond Serialization | [`Halforbit.DataStores.FileStores.Serialization.Bond`](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Bond) |
| LZMA Compression | [`Halforbit.DataStores.FileStores.Compression.Lzma`](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Compression.Lzma) |

## License 

Data Stores is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
