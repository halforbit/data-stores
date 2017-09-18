# Quick Start

Let's store and retrieve some data using local storage. 

Everything you see here can be done by referencing the `Halforbit.DataStores` nuget package.

First, we will define the data we want to store as a simple **data class**:

```cs
public class Person
{
    public Person(
        Guid personId,
        string name)
    {
        PersonId = personId;

        Name = name;
    }

    public Guid PersonId { get; }

    public string Name { get; }
}
```

Next we declare a **data context** interface with a **data store** property, and **facet** attributes describing what we want from our data store:

```cs
public interface ITestDataContext : IContext
{
    [RootPath("data"), JsonSerialization, FileExtension(".json")]
    [KeyMap("persons/{this}")]
    IDataStore<Guid?, Person> Persons { get; }
}
```

This is all we need, and our data context is ready to use:

```cs
var person = new Person(Guid.NewGuid(), "Steve Smith");

var dataContext = new ContextFactory().Create<ITestDataContext>();

dataContext.Persons.Create(person.PersonId, person).Wait();

var retrievedPerson = dataContext.Persons.Get(person.PersonId).Result;
```

Note that most `IDataStore<,>` methods are Task-oriented to allow async behavior, so don't forget to use `await`, `.Result`, `.Wait()`, etc. as appropriate.

The attributes that decorate the data store property are **facets** that describe aspects of our data store:

- The `RootPath` facet means we want the data store to write to a local folder called `data`.
- The `JsonSerialization` facet indicates that we want the data to be serialized using JSON.
- The `FileExtensions` facet indicates that we want our data files to end with `.json`.
- The `KeyMap` facet will give us one file per `Person`, named with their `PersonId` guid, and in a subfolder called `persons`, e.g.: 
    ```
    data/persons/0e464495e20544e38653327eb749eec8.json
    ```