using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;

namespace Cloudsiders.Quickstep {
    public class FasterLogStreamConfigurator : SiloPersistentStreamConfigurator {
        public FasterLogStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate, Action<Action<IApplicationPartManager>> configureAppPartsDelegate)
            : base(name, configureServicesDelegate, FasterLogAdapterFactory.Create) {
            configureAppPartsDelegate(parts => {
                parts.AddFrameworkPart(typeof(FasterLogAdapterFactory).Assembly)
                     .AddFrameworkPart(typeof(EventSequenceTokenV2).Assembly);
            });
            
            ConfigureDelegate(services => {
                services.ConfigureNamedOptionForLogging<FasterLogAdapterOptions>(name)
                        .ConfigureNamedOptionForLogging<SimpleQueueCacheOptions>(name)
                        .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
            });
        }

        public FasterLogStreamConfigurator ConfigureFasterLog(Action<OptionsBuilder<FasterLogAdapterOptions>> configureOptions) {
            this.Configure(configureOptions);
            return this;
        }

        public FasterLogStreamConfigurator ConfigureCache(int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE) {
            this.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
            return this;
        }

        public FasterLogStreamConfigurator ConfigurePartitioning(int numOfparitions = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES) {
            this.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options => options.TotalQueueCount = numOfparitions));
            return this;
        }
    }
}