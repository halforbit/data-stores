# Data Stores

## Release Notes

### 2020-06-01

#### 2.3.2

- Changed the namespace of `YamlOptions` to `Halforbit.DataStores`.

#### 2.3.0

- Changed the namespace of most public types to simply `Halforbit.DataStores`.
- Moved delimited serialization to a new nuget package, `Halforbit.DataStores.FileStores.Serialization.Delimited`, to better encapsulate the dependency on **CsvHelper**.

### 2020-05-27

#### 2.2.2

- Added support for **delimited** data value types like **TSV** and **CSV**. Added the `DelimitedSerialization` method to the builder for specifying this. Data stores with delimited value types should have a `TValue` of `IReadOnlyList<TRecord>` or `IEnumerable<TRecord>`, where `TRecord` is a type that can be successfully processed by the **CsvHelper** library. The default delimiter (which can be overridden) is **tab**, and by default a header row is expected/written (which can be overridden).
- Added a reference to **CsvHelper** 15.0.5

### 2020-05-21

#### 2.2.1

- Added `WebStorage` integration methods to the builder.

### 2020-05-19

#### 2.2.0

- Added **observers** and **mutators**, which allow observation of data before puts and deletes, and mutation of data before it is put, respectively. Observers can inherit from `Observer` or `Observer<TKey, TValue>`, and are specified with a new `Observer` method on the builder. Mutators can inherit from `Mutator` or `Mutator<TKey, TValue>`, and are specified with a new `Mutator` method on the builder. Any number of observers and mutators can be specified in a data store description.
- Added a new `Query` method to `IDataStore<,>` to perform queries more simply and directly than with the existing `using`/`StartQuery` method. The Postgres/Marten integration does not support this new method, as it requires a disposable query session to be used, so the existing `StartQuery` method must still be used within a `using` block for that integration only.
- Added a new `BatchQuery` method to `IDataStore<,>` to perform large queries in several smaller pieces. This is for the use case where e.g. you have a list of thousands of identifiers `ids`, and you want `queryable.Where(r => ids.Contains(r.RecordId))`. This is currently only implemented for the CosmosDb integration, to get around its limitations on the maximum size of a single query.
- Deleted the DocumentDb integration, as it is obsoleted by the CosmosDb integration, and its API is deprecated.
- Fixed a bug where key list operations on CosmosDb would return null keys when there are records in the container with the correct id format, but no partition key when one is expected.
- Updated to **Halforbit.ObjectTools** 1.1.10

### 2020-05-06

#### 2.1.14

- Removed return type from all `Upsert` methods of `IDataStore<,>` as they were widely misinterpreted and costly to fulfill within the integrations.
- Added the `IDataContext` interface and `DataContext` class which can either be inherited from or composed to build lazy-cached data contexts without e.g. dynamic dispatch or use of Moq.

### 2020-05-04

#### 2.0.12

- Made specifying validation in the builder pattern optional, and separated actual construction into a new `.Build()` method
- Added `DataContext` and `IDataContext`, for use when defining data contexts with the builder pattern.
- Updated to `Halforbit.ObjectTools` 1.1.8
- Updated to `Halforbit.Facets` 1.0.48

#### 2.0.11

- Fixed a bug in the Azure Table Storage integration where List methods, when provided with a complex selector, were not fully filtering results.
