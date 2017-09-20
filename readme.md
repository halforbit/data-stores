# Data Stores

**Data Stores** lets you store, retrieve, and query your data with a universal, low-ceremony pattern that abstracts away the implementation details of any given data store.

Pluggable integrations are provided to many popular storage systems and techniques, including **Local Storage**, **DocumentDb**, **Azure Blob Storage**, **Amazon S3**, **Google Drive**, **JSON**, **YAML**, **XML**, **GZIP**, as well as super-efficient **Bond** and **Protobuf** protocols. More is planned in the [Roadmap](docs/roadmap.md).

## What Is It?

Data Stores lets you easily define a **data context** that describes any number of **data stores**. These data stores can exist on simple file storage, or on any other supported data store, such as a document database.

In the simplest form, Data Stores lets you use a file folder like a **NoSQL** (in fact **NoDB**) data store. This folder can be stored on local disk, or one of many cloud file storage services. These file-based data store implementations are called **file stores**.

In the most robust form, Data Stores lets you integrate to many different kinds of data storage services, such as document databases. You can use the advanced indexing and querying abilities of these stores, while making code that is indifferent to their implementation details. 

## A Simple Example

### Step 0: Define Data Classes and Key Types

First make some **data classes** to hold your data. These are typically simple POCOs with no behavior, only properties:

```cs
public class Vehicle
{
    public Vehicle(string vin, int year, string make, string model)
    {
        Vin = vin;
        Year = year;
        Make = make;
        Model = model;
    }

    public string Vin { get; }
    public int Year { get; }
    public string Make { get; }
    public string Model { get; }

    public class Key
    {
        public Key(string make, string vin)
        {
            Make = make;
            Vin = vin;
        }

        public string Make { get; }
        public string Vin { get; }
    }
}
```

In this example, we have defined a nested **key type** `Vehicle.Key` for uniquely identifying and storing things of this type. The key type does not need to be a nested class, and can also be a simple type, like `int?` or `Guid?`, as long as it is nullable. 

### Step 1: Define a Data Context

Now define a **data context** that describes all of your data stores:

```cs
public interface ITestDataContext : IContext
{
    [RootPath("data"), JsonSerialization, FileExtension(".json")]
    [KeyMap("vehicles/{Make}/{Vin}")]
    IDataStore<Vehicle.Key, Vehicle> Vehicles { get; }
}
```

This is an interface, with a `IDataStore<,>` property for each data store, decorated with **facet** attributes. These facets describe aspects of our data store. In this example:

- `RootPath` declares that we want the data store to write to a local folder called `data`.
- `JsonSerialization` declares that we want the data to be serialized using JSON.
- `FileExtension` declares that we want our data files to end with `.json`.
- `KeyMap` declares that we want our entity file to be in a subfolder like `vehicles/{Make}`, with the file named with `Vin`, e.g.: 
    ```
    vehicles/Ford/1HGBH40JXMN109186.json
    ```

### Step 2: Use It!

We can now use a `ContextFactory` to create an instance of our `ITestDataContext`:

```cs
var dataContext = new ContextFactory().Create<ITestDataContext>();

var vehicle = new Vehicle("1HGBH40JXMN109186", 2017, "Ford", "Fusion");

var vehicleKey = new Vehicle.Key(vehicle.Vin, vehicle.Make);

dataContext.Vehicles.Create(vehicleKey, vehicle).Wait();

var gottenVehicle = dataContext.Vehicles.Get(vehicleKey).Result;

var queriedVehicles = dataContext.Vehicles.ListValues(k => k.Make == "Ford").Result;
```

Most `IDataStore<,>` methods are Task-oriented to allow async behavior, so use `await`, `.Result`, `.Wait()`, etc. as appropriate.

Note that the consuming code here doesn't know or care too much about which data store technology is used. This code could operate against anything from a local folder to a database server. The `IDataStore<,>` interface abstracts these details away. 

## Learn More

- **[Keying in Data Stores](docs/keying.md)**

- **[Data Contexts](docs/data-contexts.md)**

- **[File Stores]()**


## What Data Stores Can I Use?

Data Store integrations are provided for **Local Storage**, **DocumentDb**, **Azure Blob Storage**, **Amazon S3**, and **Google Drive**.

Serialization integrations are provided for **JSON**, **YAML**, **XML**, as well as super-efficient **Bond** and **Protobuf** protocols. Note that some serializers have special requirements for data classes to be serialized.

Optional compression via **GZIP** is provided as well. This can make a 10:1 difference in stored / transferred data payload size for formats like JSON.

## How Do I Get It?

Data Stores is available as a constellation of Nuget packages, broken up primarily by third party dependency requirements.

The `Halforbit.DataStores` package is always required, and includes Local Storage, JSON, and GZIP capabilities. 

Include any other packages with the functionality you desire:

[Halforbit.DataStores](https://www.nuget.org/packages/Halforbit.DataStores/)

[Halforbit.DataStores.DocumentStores.DocumentDb](https://www.nuget.org/packages/Halforbit.DataStores.DocumentStores.DocumentDb/)

[Halforbit.DataStores.FileStores.AmazonS3](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.AmazonS3/)

[Halforbit.DataStores.FileStores.BlobStorage](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.BlobStorage/)

[Halforbit.DataStores.FileStores.GoogleDrive](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.GoogleDrive/)

[Halforbit.DataStores.FileStores.Serialization.Bond](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Bond/)

[Halforbit.DataStores.FileStores.Serialization.Protobuf](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Protobuf/)

[Halforbit.DataStores.FileStores.Serialization.Yaml](https://www.nuget.org/packages/Halforbit.DataStores.FileStores.Serialization.Yaml/)

[![Build status](https://ci.appveyor.com/api/projects/status/w8tliyvw96obytai?svg=true)](https://ci.appveyor.com/project/halforbit/data-stores)