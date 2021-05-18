using System;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;

namespace Cloudsiders.Quickstep {
    public static class ClientBuilderExtensions {
        public static IClientBuilder AddFasterLogStreams(this IClientBuilder builder, string name, Action<FasterLogAdapterOptions> configureOptions) {
            builder.AddFasterLogStreams(name, b =>
                                            b.ConfigureFasterLog(ob => ob.Configure(configureOptions)));
            return builder;
        }

        public static IClientBuilder AddFasterLogStreams(this IClientBuilder builder, string name, Action<ClusterClientFasterLogStreamConfigurator> configure) {
            var configurator = new ClusterClientFasterLogStreamConfigurator(name, builder);
            configure?.Invoke(configurator);
            return builder;
        }
    }

    public class ClusterClientFasterLogStreamConfigurator : ClusterClientPersistentStreamConfigurator {
        public ClusterClientFasterLogStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, FasterLogAdapterFactory.Create) {
            builder
                .ConfigureApplicationParts(parts => {
                    parts.AddFrameworkPart(typeof(FasterLogAdapterFactory).Assembly)
                         .AddFrameworkPart(typeof(EventSequenceTokenV2).Assembly);
                })
                .ConfigureServices(services => {
                    services.ConfigureNamedOptionForLogging<FasterLogAdapterOptions>(name)
                            .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
                });
        }

        public ClusterClientFasterLogStreamConfigurator ConfigureFasterLog(Action<OptionsBuilder<FasterLogAdapterOptions>> configureOptions) {
            this.Configure(configureOptions);
            return this;
        }

        public ClusterClientFasterLogStreamConfigurator ConfigurePartitioning(int numOfparitions = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES) {
            this.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options => options.TotalQueueCount = numOfparitions));
            return this;
        }
    }
}