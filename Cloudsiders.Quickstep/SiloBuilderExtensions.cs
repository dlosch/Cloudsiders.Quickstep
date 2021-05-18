using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;

namespace Cloudsiders.Quickstep {
    public static class SiloBuilderExtensions {
        /// <summary>
        /// Based on SQS Sample
        /// </summary>
        public static ISiloHostBuilder AddFasterLogStreams(this ISiloHostBuilder builder, string name, Action<FasterLogAdapterOptions> configureOptions) {
            builder.AddFasterLogStreams(name, b =>
                                            b.ConfigureFasterLog(ob => ob.Configure(configureOptions)));
            return builder;
        }

        ///// <summary>
        ///// Configure silo to use SimpleMessageProvider
        ///// </summary>
        //public static ISiloHostBuilder AddSimpleMessageStreamProvider(
        //    this ISiloHostBuilder builder,
        //    string name,
        //    Action<ISimpleMessageStreamConfigurator> configureStream) {
        //    //the constructor wire up DI with all default components of the streams , so need to be called regardless of configureStream null or not
        //    var streamConfigurator = new SimpleMessageStreamConfigurator(name, configureDelegate => builder.ConfigureServices(configureDelegate));
        //    configureStream?.Invoke(streamConfigurator);
        //    return builder;
        //}
        ///// <summary>
        ///// Configure silo to use SimpleMessageProvider
        ///// </summary>
        //public static ISiloHostBuilder AddSimpleMessageStreamProvider(
        //    this ISiloHostBuilder builder,
        //    string name,
        //    Action<ISiloPersistentStreamConfigurator> configureStream) {
        //    //the constructor wire up DI with all default components of the streams , so need to be called regardless of configureStream null or not
        //    var streamConfigurator = new SiloSqlStreamConfigurator(name, configureDelegate => builder.ConfigureServices(configureDelegate));
        //    configureStream?.Invoke(streamConfigurator);
        //    return builder;
        //}


        public static ISiloBuilder AddFasterLogStreams(this ISiloBuilder builder, string name, Action<FasterLogAdapterOptions> configureOptions) {
            builder.AddFasterLogStreams(name, b =>
                                            b.ConfigureFasterLog(ob => ob.Configure(configureOptions)));
            return builder;
        }
        
        public static ISiloBuilder AddFasterLogStreams(this ISiloBuilder builder, string name, Action<FasterLogStreamConfigurator> configure) {
            var configurator = new FasterLogStreamConfigurator(name,
                                                               configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                                                               configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }

        public static ISiloHostBuilder AddFasterLogStreams(this ISiloHostBuilder builder, string name, Action<SiloFasterLogStreamConfigurator> configure) {
            var configurator = new SiloFasterLogStreamConfigurator(name,
                                                                   configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                                                                   configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }
    }


    public class SiloFasterLogStreamConfigurator : SiloPersistentStreamConfigurator {
        public SiloFasterLogStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate, Action<Action<IApplicationPartManager>> configureAppPartsDelegate)
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

        public SiloFasterLogStreamConfigurator ConfigureFasterLog(Action<OptionsBuilder<FasterLogAdapterOptions>> configureOptions) {
            this.Configure(configureOptions);


            return this;
        }

        public SiloFasterLogStreamConfigurator ConfigureCache(int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE) {
            this.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
            return this;
        }

        public SiloFasterLogStreamConfigurator ConfigurePartitioning(int numOfparitions = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES) {
            this.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options => options.TotalQueueCount = numOfparitions));
            return this;
        }
    }
}