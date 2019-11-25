using System;

namespace Halforbit.DataStores.FileStores.Ftp.Exceptions
{
    public class FtpException : Exception
    {
        public FtpException(
            string message, 
            Exception innerException, 
            bool isRetryable = false) : 
                base(message, innerException) 
        {
            IsRetryable = isRetryable;
        }

        public bool IsRetryable { get; }
    }
}
