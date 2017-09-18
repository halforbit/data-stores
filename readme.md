# Data Stores

**Data Stores** lets you store, retrieve, and query your data with a universal pattern that abstracts away the implementation details of any given data store.

Pluggable integrations are provided to many popular storage systems and techniques, including **Local Storage**, **DocumentDb**, **Azure Blob Storage**, **Google Drive**, **JSON**, **YAML**, **XML**, **GZIP**, as well as super-efficient **Bond** and **Protobuf** protocols. More is planned in the [Roadmap](docs/roadmap.md).

**Dive right in with the [Quick Start](docs/quick-start.md)**

In the simplest form, Data Stores lets you use a file folder like a **NoSQL** data store. In this case, it could even be called a **NoDB** data store, since it doesn't require the installation or configuration of a database engine at all. This folder can be stored on local disk, or one of many cloud file storage services.

In the most robust form, Data Stores lets you integrate to many different kinds of data storage services, such as document databases. You can use the advanced indexing and querying abilities of these stores, while making code that is indifferent to their implementation details. 

You can easily describe and create a **context** that accesses data on many storage services using the unified `IDataStore` interface. This interface is easy to mock, making the approach friendly to unit testing as well.

## What Data Stores Can I Use?

Data Store integrations are provided for **Local Storage**, **DocumentDb**, **Blob Storage**, and **Google Drive**.

Serialization integrations are provided for **JSON**, **YAML**, **XML**, as well as super-efficient **Bond** and **Protobuf** protocols. Note that some serializers have special requirements for data classes to be serialized.

Optional compression via **GZIP** is provided as well. This can make a 10:1 difference in stored / transferred data payload size for formats like JSON.






