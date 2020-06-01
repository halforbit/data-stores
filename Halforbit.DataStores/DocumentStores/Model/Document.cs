using Newtonsoft.Json;

namespace Halforbit.DataStores
{
    public abstract class Document : IDocument
    {
        [JsonProperty("id")]
        public string Id { get; set; }
    }
}
