using Halforbit.DataStores.Interface;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.Facets
{
    public class KeyMapAttribute : FacetParameterAttribute
    {
        public KeyMapAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override Type TargetType => typeof(IDataStore<,>);

        public override string ParameterName => "keyMap";
    }
}
