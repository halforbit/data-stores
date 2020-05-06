# Data Stores

## Release Notes

### 2020-05-06

#### 2.1.0

- Removed return type from all `Upsert` methods of `IDataStore<,>` as they were widely misinterpreted and costly to fulfill within the integrations.
- Added the `IDataContext` interface and `DataContext` class which can either be inherited from or composed to build cached data contexts without e.g. dynamic dispatch or use of Moq.
- Builder Pattern: Created `IDataStoreDescription<TKey, TValue>` and `IDataStoreDescription<TValue>` (singleton) as the abstraction for a constructable data store, having a `.Build()` method.
- Builder Pattern: Moved validation to an optional tail of `IDataStoreDescription<,>`.

### 2020-05-04

#### 2.0.12

- Made specifying validation in the builder pattern optional, and separated actual construction into a new `.Build()` method
- Added `DataContext` and `IDataContext`, for use when defining data contexts with the builder pattern.
- Updated to `Halforbit.ObjectTools` 1.1.8
- Updated to `Halforbit.Facets` 1.0.48

#### 2.0.11

- Fixed a bug in the Azure Table Storage integration where List methods, when provided with a complex selector, were not fully filtering results.
