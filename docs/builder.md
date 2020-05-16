# How To Data Stores

## How to Create a Data Store

You can create an instance of a data store using the `DataStore` class. 

Here we create a data store instance against local storage:

```csharp
var dataStore = DataStore
    .Describe()
    .LocalStorage()
    .RootPath("c:/data")
    .ByteSerialization()
    .NoCompression()
    .FileExtension(".txt")
    .Map<Guid, string>("messages/{this}")
    .Build();
```

This data store will create one file per record in `c:/data/messages` named e.g. `248968fe4a7b41b0aa7cf07ac2850f1d.txt`.

Note the fluent interface. You will get only the relevant options available to you as you type.

Here is another example where we store JSON to Azure Blob Storage (using the local storage emulator):

```csharp
var dataStore = DataStore
    .Describe()
    .BlobStorage()
    .ConnectionString("UseDevelopmentStorage=true")
    .Container("my-container")
    .ContentType("application/json")
    .DefaultContentEncoding()
    .JsonSerialization()
    .NoCompression()
    .FileExtension(".json")
    .Map<Guid, string>("my-stuff/{this}")
    .Build();
```

## Keys and Maps

Data stores descriptions define a map, specifying a **key type**, a **value type**, and a **map string**. 

```csharp
    .Map<Guid, string>("my-stuff/{this}")
```

Here the key type is `Guid`, the value type is `string`, and the map string is `my-stuff/{this}`.

The map string is a two-way template used to convert between the key type and a string. 

For file stores like local storage and Azure Blob Storage, the map string (along with file extension)  determines the exact file name a key maps to in storage. For document stores like Cosmos DB and Postgres, the map string is used to determine the ID of the record.

As shown here, you can use `{this}` to inject simple key types like `string` or `Guid` into the string map. You can also use a class as a key type, and refer to its properties with e.g. `{MyProperty}`.

## Shortcuts

Consider a typical JSON blob storage data store description. First we have some location information:

```csharp
    .BlobStorage()
    .ConnectionString("UseDevelopmentStorage=true")
    .Container("my-container")
```

Then we have information about the record's format:

```csharp
    .ContentType("application/json")
    .DefaultContentEncoding()
    .JsonSerialization()
    .NoCompression()
    .FileExtension(".json")
```

If you have a lot of data stores to describe, you will find yourself repeating these quite a bit. You can make this location and format reusable between data store descriptions by declaring a **shortcut** as a static method. 

### Creating Shortcuts

Let's make a location shortcut static method. This receives an `INeedsIntegration` and returns an `INeedsContentType` further down the fluent chain:

```csharp
public class Location
{
    public class BlobStorage
    {
        public class DevelopmentStorage
        {
            public static INeedsContentType MyContainer(INeedsIntegration s) => s
                .BlobStorage()
                .ConnectionString("UseDevelopmentStorage=true")
                .Container("my-container");
        }
    }
}
```

And a format shortcut receiving `INeedsContentType` and returning `INeedsMap` further down the fluent chain:

```csharp
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
    }
}
```

As shown here you can nest classes to better describe your structure, but this is optional.

### Using Shortcuts

With our shortcuts created, we can greatly simplify our data store descriptions by referencing them using the `With()` method:

```csharp
var dataStore = DataStore
    .Describe()
    .With(Location.BlobStorage.DevelopmentStorage.MyContainer)
    .With(Format.Structured.Json)
    .Map<Guid, string>("my-stuff/{this}")
    .Build();
```

## How to Use a Data Store

## Data Contexts

## Repository Pattern

## Connection Strings and Other Sensitive Information

## Kinds of Data Stores Document, File, Table

## Formats of File Stores

## Compression of File Stores

## Validation

## Observers and Mutators

## Singletons

## Partial Keys and Predicates

## Keying Strategies

## Optimistic Concurrency Pattern

## Custom Serializers

## Sharding

## Querying