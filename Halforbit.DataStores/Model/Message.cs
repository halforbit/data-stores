
namespace Halforbit.DataStores.Model
{
    public class Message<TContent>
    {
        public Message(
            string id,
            string receipt,
            TContent content)
        {
            Id = id;
            Receipt = receipt;
            Content = content;
        }

        public string Id { get; }

        public string Receipt { get; }

        public TContent Content { get; }

        public static implicit operator TContent(Message<TContent> m)
        {
            return m.Content;
        }
    }
}
