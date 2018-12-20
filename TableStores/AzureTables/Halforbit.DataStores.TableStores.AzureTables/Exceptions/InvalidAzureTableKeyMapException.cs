using System;

namespace Halforbit.DataStores.TableStores.AzureTables.Exceptions 
{
    public class InvalidAzureTableKeyMapException : Exception 
    {
        public InvalidAzureTableKeyMapException(string message) 
            : base(message) { }
    }
}