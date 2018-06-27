using Halforbit.DataStores.Model;

namespace Halforbit.DataStores.Interface
{
    public interface IMessageQueue<TContent>
    {
        Message<TContent> Get();

        Message<TContent> Put(TContent content);

        void Delete(Message<TContent> message);

        int? GetApproximateMessageCount();
    }
}
