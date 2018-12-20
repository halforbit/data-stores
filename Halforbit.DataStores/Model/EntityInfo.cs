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
            string contentType,
            string contentEncoding,
            LeaseState leaseState,
            bool leaseLocked)
        {
            Name = name;

            LastModified = lastModified;

            Size = size;

            AbsoluteUri = absoluteUri;

            ContentType = contentType;

            ContentEncoding = contentEncoding;

            LeaseState = leaseState;

            LeaseLocked = leaseLocked;
        }

        public string Name { get; }

        public DateTime? LastModified { get; }

        public long Size { get; }

        public string AbsoluteUri { get; }

        public string ContentType { get; }

        public string ContentEncoding { get; }

        public LeaseState LeaseState { get; }

        public bool LeaseLocked { get; }
    }
}
