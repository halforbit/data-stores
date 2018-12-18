using Halforbit.DataStores.TableStores.AzureTables.Implementation;
using Halforbit.Facets.Attributes;
using System;

namespace Halforbit.DataStores.TableStores.AzureTables.Facets
{
    public class TableNameAttribute : FacetParameterAttribute
    {
        public TableNameAttribute(string value = null, string configKey = null) : base(value, configKey) { }

        public override Type TargetType => typeof(AzureTableStore<,>);

        public override string ParameterName => "tableName";
   } 
}
