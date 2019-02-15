using System.Threading.Tasks;

namespace BackgroundKafkaSubscriber.Infrastructure
{
    public interface IKafkaConsumer
    {
        Task OnMessageAsync(string message);
    }
}
