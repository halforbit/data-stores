# Keying in Data Stores

When you declare a data store, you specify a **key type**, as well as a **key map** that describes how to turn the key type to and from a string.

These strings form [natural keys](https://en.wikipedia.org/wiki/Natural_key) that are used to uniquely store and retrieve data records from a data store. 

A key map is a string with a number of **path segments** separated by `/`, with a path segment consisting of either literal text or the name of a property to inject wrapped in `{` `}`. 

For [file store](file-stores.md) data stores, path segments will be represented as sub-folders.


### A Simple Example

Let's suppose we have a key type:

```cs
public class Vehicle
{
    // ...

    public class Key
    {
        public Key(string make = null, string vin = null)
        {
            Make = make;
            Vin = vin;
        }

        public string Make { get; }
        public string Vin { get; }
    }
}
``` 

And a data store:

```cs
[KeyMap("vehicles/{Make}/{Vin}")]
IDataStore<Vehicle.Key, Vehicle> Vehicles { get; }
```

In this example, the key type is `Vehicle.Key`, and the key map is `vehicles/{Make}/{Vin}`. This will result in keys like `vehicles/Ford/1HGBH40JXMN109186`.

### The Name Prefix

Note the literal prefix `vehicles/` in the key map. This uniquely distinguishes the data for this data store. This is analogous to a *table name*, and allows you to have any number of data stores coexisting within the same root folder or document database collection.

### The Order of Key Map Properties

Note here that the `Make` property comes before the `Vin` property in the key map. This allows us to form a partial **key prefix** that has `Make` populated but omits the more specific `Vin`, with values like `vehicles/Ford/`. Defining the key map in this way allows us to efficiently filter results from the data store. This is especially effective when operating against [file stores](file-stores.md). 

### Formatting Properties

Property injection points can include a formatting string, e.g. `daily-digests/{Date:yyyy/MM/dd}/digest`. 

This will map a property named `Date` to a path like `daily-digests/2017/09/18/digest`. 

As you can see, this technique is useful with data that is keyed off of time.

### Simple Key Types

You can use simple types such as `Guid?` and `int?` for your key type. In this case you can use `this` to refer to the key itself in your key map:

```cs
[KeyMap("persons/{this}")]
IDataStore<Guid?, Person> Persons { get; }
```
