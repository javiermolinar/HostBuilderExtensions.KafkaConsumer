using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BackgroundKafkaSubscriber.Infrastructure
{
    public static class HostBuilderExtensions
    {
        /// <summary>
        /// Add Kafka consumer to the host builder
        /// </summary>
        /// <param name="hostBuilder">The <see cref="IHostBuilder"/> to configure.</param>
        /// <returns>The <see cref="IHostBuilder"/>.</returns>
        public static IHostBuilder AddKafkaConsumers(this IHostBuilder hostBuilder)
        {
            return hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddOptions(); 
                services.Configure<ConsumerConfig>(hostContext.Configuration);
                services.AddSingleton(hostContext.Configuration);
                AddKafkaConsumerClasses(services, AppDomain.CurrentDomain.GetAssemblies());
            });
        }

        private static void AddKafkaConsumerClasses(IServiceCollection services,  IEnumerable<Assembly> assembliesToScan)
        {
            if (services.Any(sd => sd.ServiceType == typeof(IKafkaConsumer)))
                return;

            assembliesToScan = assembliesToScan as Assembly[] ?? assembliesToScan.ToArray();

            var allTypes = assembliesToScan
                .Where(a => !a.IsDynamic)
                .SelectMany(a => a.DefinedTypes)
                .ToArray();

            var consumerTypeInfo = typeof(KafkaConsumer).GetTypeInfo();
            var consumers = allTypes
                .Where(t => consumerTypeInfo.IsAssignableFrom(t) && !t.IsAbstract)
                .ToArray();

            foreach (var type in consumers)
                services.Add(new ServiceDescriptor(typeof(IHostedService), type, ServiceLifetime.Transient));
        }
    }
}
