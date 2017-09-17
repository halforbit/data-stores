using System;

namespace Halforbit.DataStores.FileStores.Exceptions
{
    public class IncompleteKeyException : Exception
    {
        public IncompleteKeyException(string message) : base(message) { }
    }
}
