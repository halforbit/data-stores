using System;

namespace Halforbit.DataStores.FileStores.Sftp.Exceptions
{
    public class SftpException : Exception
    {
        public SftpException(string message, Exception innerException) : 
            base(message, innerException) 
        { }
    }
}
