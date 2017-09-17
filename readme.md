# Data Stores

**Data Stores** is a data access pattern that transcends the implementation details of any given data store, with pluggable integrations to many popular storage systems and techniques.

In the simplest form, Data Stores lets you use a file folder like a **NoSQL** data store. In fact, it could be called a **NoDB** data store, since it doesn't require the installation or configuration of a database engine at all. This folder can be stored on local disk, or one of many cloud file storage services.

In the most robust form, Data Stores lets you integrate to many different kinds of data storage services, such as document databases. You can use the advanced indexing and querying abilities of these stores, while making code that is indifferent to their implementation details. 

You can easily describe and create a context that accesses data on many storage services using the unified `IDataStore` interface. This interface is easy to mock, making the approach friendly to unit testing as well.

Data Store integrations are provided for Local Storage, DocumentDb, Blob Storage, and Google Drive.

Serialization integrations are provided for JSON, YAML, XML, Protobuf, and Bond. Note that some serializers have special requirements for the data classes to be serialized.

Optional compression via GZIP is provided as well. This can make a 10:1 difference in stored / transferred data payload size for formats like JSON.

More is planned in the [roadmap](docs/roadmap.md).