# File Stores

File Stores are special datastore implementations that operate over simple file storage systems, offering database-like capabilities without the use of a database server.

File stores are implemented as `FileStoreDataStore<,>`, which requires a pluggable `IFileStore` and `ISerializer`, and optionally an `ICompressor`.

`IFileStore` implementations are provided for **Local Storage**, **Azure Blob Storage**, **Amazon S3**, and **Google Drive**. More [are planned](roadmap.md), and it is also very easy to implement this interface over your file storage provider of choice.

File Store Data Stores store one entity per file. 

## Pros and Cons of File Stores

Pros:

  - Database-like functionality, with no database server required.
  - The underlying file storage service deals with write concurrency, atomicity, and deciding e.g. that the last one in wins. Optimistic concurrency is provided where supported.
  - Operates against very cheap persistence such as local storage, Azure Blob Storage, Amazon S3, etc.
  - Probably the simplest, cheapest option for storing entities where deep querying is not required.

Cons: 

  - Entities are stored one-per-file, so operations against a lot of entities will require a lot of file service requests.
  - Deep querying of the data payload requires a scan and deserialize of every file in the file store, which is likely to be slow and expensive.