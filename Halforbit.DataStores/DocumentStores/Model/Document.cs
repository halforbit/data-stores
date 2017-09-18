using Halforbit.DataStores.DocumentStores.Interface;
using Newtonsoft.Json;

namespace Halforbit.DataStores.DocumentStores.Model
{
    public abstract class Document : IDocument
    {
        [JsonProperty("id")]
        public string Id { get; set; }
    }
}
