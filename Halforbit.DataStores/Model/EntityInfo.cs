using System;

namespace Halforbit.DataStores
{
    public class EntityInfo
    {
        public EntityInfo(
            string name,
            DateTime? lastModified,
            long size,
            string absoluteUri,
            string contentType,
            string contentEncoding,
            string contentHash,
            LeaseState leaseState,
            bool leaseLocked)
        {
            Name = name;

            LastModified = lastModified;

            Size = size;

            AbsoluteUri = absoluteUri;

            ContentType = contentType;

            ContentEncoding = contentEncoding;

            ContentHash = contentHash;

            LeaseState = leaseState;

            LeaseLocked = leaseLocked;
        }

        public string Name { get; }

        public DateTime? LastModified { get; }

        public long Size { get; }

        public string AbsoluteUri { get; }

        public string ContentType { get; }

        public string ContentEncoding { get; }

        public string ContentHash { get; }

        public LeaseState LeaseState { get; }

        public bool LeaseLocked { get; }
    }
}
