using System;

namespace Halforbit.DataStores.Model
{
    [Flags]
    public enum Access
    {
        None = 0,

        Get = 1,

        List = 2,

        Put = 4,

        Delete = 8,

        Read = Get | List,

        Write = Put | Delete,

        Full = Read | Write
    }
}
