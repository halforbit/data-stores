using Newtonsoft.Json;

namespace Halforbit.DataStores.DocumentStores.Interface
{
    public interface IDocument
    {
        [JsonProperty("id")]
        string Id { get; set; }
    }
}
