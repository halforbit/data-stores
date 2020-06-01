using System;

namespace Halforbit.DataStores
{
    [Flags]
    public enum YamlOptions
    {
        None = 0,

        CamelCaseEnumValues = 1,

        CamelCasePropertyNames = 2,

        RemoveDefaultValues = 4,

        OmitGuidDashes = 8,

        Default = CamelCasePropertyNames | CamelCaseEnumValues | RemoveDefaultValues | OmitGuidDashes
    }
}
