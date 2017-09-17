using System;

namespace Halforbit.DataStores.FileStores.Exceptions
{
    class DeserializationException : Exception
    {
        public DeserializationException(string message, Exception exception) : 
            base(message, exception)
        {
        }
    }
}
