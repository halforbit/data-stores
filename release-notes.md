# Data Stores

## Release Notes

### 2020-05-15

#### 2.2.0

- Added a new `Query` method to `IDataStore<,>` to perform queries more simply and directly than with the existing `using`/`StartQuery` method. Note that the Postgres/Marten integration does not support this new method, as it requires a disposable query session to be used, so the existing `StartQuery` method must still be used within a `using` block for that integration.
- Deleted the DocumentDb integration, as it is obsoleted by the CosmosDb integration, and its API is deprecated.
- Fixed a bug where key list operations on CosmosDb would return null keys when there are records in the container with the correct id format, but no partition key when one is expected.

### 2020-05-06

#### 2.1.14

- Removed return type from all `Upsert` methods of `IDataStore<,>` as they were widely misinterpreted and costly to fulfill within the integrations.
- Added the `IDataContext` interface and `DataContext` class which can either be inherited from or composed to build cached data contexts without e.g. dynamic dispatch or use of Moq.

### 2020-05-04

#### 2.0.12

- Made specifying validation in the builder pattern optional, and separated actual construction into a new `.Build()` method
- Added `DataContext` and `IDataContext`, for use when defining data contexts with the builder pattern.
- Updated to `Halforbit.ObjectTools` 1.1.8
- Updated to `Halforbit.Facets` 1.0.48

#### 2.0.11

- Fixed a bug in the Azure Table Storage integration where List methods, when provided with a complex selector, were not fully filtering results.
