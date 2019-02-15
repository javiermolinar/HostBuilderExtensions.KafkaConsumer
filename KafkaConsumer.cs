using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


namespace BackgroundKafkaSubscriber.Infrastructure
{
    public abstract class KafkaConsumer : BackgroundService, IKafkaConsumer
    {
        protected readonly ILogger<KafkaConsumer> Logger;
        private Consumer<Ignore, string> _consumer;

        protected KafkaConsumer(IConfiguration configuration, ILogger<KafkaConsumer> logger)
        {         
            Logger = logger;
            var cc = new ConsumerConfig();
            configuration.Bind(cc);
            StartConsumer(cc, configuration.GetValue("Topic", ""));
        }

        protected KafkaConsumer( IConfiguration configuration, ILogger<KafkaConsumer> logger, string topic)
        {         
            Logger = logger;
            var cc = new ConsumerConfig();
            configuration.Bind(cc);
            StartConsumer(cc, topic);
        }

        private void StartConsumer(ConsumerConfig config, string topic)
        {
            _consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) => Logger.LogInformation($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Logger.LogInformation($"Statistics: {json}"))
                .Build();
            _consumer.Subscribe(topic);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(() =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<Ignore,string> cr = _consumer.Consume(stoppingToken);
                        OnMessageAsync(cr.Value);
                    }
                    catch (ConsumeException e)
                    {
                        Logger.LogInformation($"Error occured: {e.Error.Reason}");
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }, stoppingToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation("Broker is stopped");           
            _consumer.Close();
            _consumer.Dispose();
            return Task.CompletedTask;
        }

        public abstract Task OnMessageAsync(string message);
    }
}
