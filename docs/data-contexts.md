# Data Contexts

Data Stores lets you define **[faceted contexts]()** to describe your data stores. These are interfaces that are decorated with **facet** attributes, with properties defining your data stores:

```cs
public interface ITestDataContext : IContext
{
    [RootPath("data"), JsonSerialization, FileExtension(".json")]
    [KeyMap("vehicles/{Make}/{Vin}")]
    IDataStore<Vehicle.Key, Vehicle> Vehicles { get; }

    [RootPath("data"), JsonSerialization, FileExtension(".json")]
    [KeyMap("persons/{PersonId}")]
    IDataStore<Guid?, Person> Persons { get; }
}
```

These contexts are **implementationless** at design time. At run time, an instance can be created using a `ContextFactory`, and used to access your data stores:

```cs
var dataContext = new ContextFactory().Create<ITestDataContext>();

var vehicles = dataContext.Vehicles.ListValues().Result;
```

The `ContextFactory` uses the facet attributes to determine how to create an instance of `IDataStore<,>`. 

## Nice to Have, not Need to Have

Data contexts are a *declarative* way to describe your family of data stores. But you don't *need* to use data contexts to use Data Stores. Data store implementations and their dependencies can be constructed directly:

```cs
var dataContext = new
{
    Vehicles = new FileStoreDataStore<Vehicle.Key, Vehicle>(
        new LocalFileStore(rootPath: "data"),
        new JsonSerializer(),
        compressor: null,
        keyMap: "vehicles/{Make}/{Vin}",
        fileExtension: ".json"),

    Persons = new FileStoreDataStore<Guid?, Person>(
        new LocalFileStore(rootPath: "data"),
        new JsonSerializer(),
        compressor: null,
        keyMap: "persons/{PersonId}/{Vin}",
        fileExtension: ".json")
}
```

*Imperative* construction like this might be the simplest approach if you are only dealing with a single or a few data stores. But what about when you have 50+?

Notice the ceremonious repetition emerging around the things the data stores have in common. 

Also note that all the data stores are created up-front, incurring a construction cost whether the consuming code uses a given data store or not.

## Keep It DRY and Lazy

### Let's Simplify
We can use some features of facets to make our data context more readable, consistent, concise, and easy to change. 

First, facets can be accumulated from containing types. So we can place the facets shared by all of our data stores on their shared parent:

```cs
[RootPath("data"), JsonSerialization, FileExtension(".json")]
public interface ITestDataContext : IContext
{
    [KeyMap("vehicles/{Make}/{Vin}")]
    IDataStore<Vehicle.Key, Vehicle> Vehicles { get; }

    [KeyMap("persons/{PersonId}")]
    IDataStore<Guid?, Person> Persons { get; }
}
```

Second, facets can be accumulated from 'source' declarations, which are simple marker classes that can be hierarchical and descriptively named:

```cs
public class DataSource
{
    [RootPath("data")]
    public class LocalStore
    {
        [JsonSerialization, FileExtension(".json")]
        public class Json { }
    }
}
```

We apply these facets using a `Source` attribute:

```cs
[Source(typeof(DataSource.LocalStore.Json)]
public interface ITestDataContext : IContext
{
    [KeyMap("vehicles/{Make}/{Vin}")]
    IDataStore<Vehicle.Key, Vehicle> Vehicles { get; }

    [KeyMap("persons/{PersonId}")]
    IDataStore<Guid?, Person> Persons { get; }
}
```

Source declarations are particularly useful for consistently applying a cluster of facets to many different data stores and contexts.

### Lazy Loading

`ContextFactory` creates contexts with properties that are lazy-loaded, so only the data stores that are actually used get created.

## Sub-Contexts

You can have data contexts inside of data contexts:

```cs
[Source(typeof(DataSource.LocalStore.Json)]
public interface IInventoryDataContext : IContext
{
    [KeyMap("vehicles/{Make}/{Vin}")]
    IDataStore<Vehicle.Key, Vehicle> Vehicles { get; }
}

[Source(typeof(DataSource.LocalStore.Json)]
public interface IRootDataContext : IContext
{
    IInventoryDataContext Inventory { get; }

    [KeyMap("persons/{PersonId}")]
    IDataStore<Guid?, Person> Persons { get; }
}
```

Here we have moved the `Vehicles` data store into a sub-context called `Inventory`.

Like data store properties, sub-context properties are lazy-loaded. Make sure your context inherits from `IContext` if you intend to use it as a sub-context.

## Config Keys

As we have seen, facet parameters can be provided as literal values, but for configurable or sensitive information like connection strings or passwords, you can specify a config key to pull the value from instead:

```cs
[ConnectionString(configKey: "MyConnectionString")]
IDataStore<Guid?, Person> Persons { get; }
```

Storing and retrieving config key/values has historically has been done with a web.config or an app.config and e.g. `ConfigurationManager`, but there are several newer approaches to this as well. Choose the approach that works best for you and implement an `IConfigurationProvider` that wraps it. It has one simple method:

```cs
public class MyConfigurationProvider : IConfigurationProvider
{
    public string GetValue(string key)
    {
        // Use ConfigurationManager, etc. to get the config value here.
    }
}
```

You can then provide your `IConfigurationProvider` implementation when constructing your `ContextFactory`:

```cs
    var configurationProvider = new MyConfigurationProvider();

    var contextFactory = new ContextFactory(configurationProvider);

    var dataContext = contextFactory.Create<ITestDataContext>();
```

## Mocking and Tests

Because your data context is just an interface of interface properties, it is very straightforward to mock for a unit test with a framework such as Moq.