using System;

namespace Halforbit.DataStores.Model
{
    public class EntityInfo
    {
        public EntityInfo(
            string name,
            DateTime? lastModified,
            long size,
            string absoluteUri,
            string contentType)
        {
            Name = name;

            LastModified = lastModified;

            Size = size;

            AbsoluteUri = absoluteUri;

            ContentType = contentType;
        }

        public string Name { get; }

        public DateTime? LastModified { get; }

        public long Size { get; }

        public string AbsoluteUri { get; }

        public string ContentType { get; }
    }
}
