using Newtonsoft.Json;

namespace Halforbit.DataStores
{
    public interface IDocument
    {
        [JsonProperty("id")]
        string Id { get; set; }
    }
}
