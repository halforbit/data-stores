using System;

namespace Halforbit.DataStores.Tests
{
    public static class TestUtilities
    {
        // This is a hacky hack for testing.
        public static Guid ToGuid(
            this int value)
        {
            byte[] bytes = new byte[16];
            BitConverter.GetBytes(value).CopyTo(bytes, 0);
            return new Guid(bytes);
        }
    }
}
