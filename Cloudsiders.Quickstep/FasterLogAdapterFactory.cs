using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Cloudsiders.Quickstep {
    public class FasterLogAdapterFactory : IQueueAdapterFactory {
        private readonly string _providerName;

        private readonly FasterLogAdapterOptions _fasterLogAdapterOptions;

        private readonly HashRingStreamQueueMapperOptions _queueMapperOptions;

        //private readonly SimpleQueueCacheOptions _cacheOptions;
        private readonly IServiceProvider _serviceProvider;
        private readonly ClusterOptions _clusterOptions;
        private readonly FasterLogSerializationManager _serializationManager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly SimpleQueueAdapterCache _adapterCache;
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;

        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { private get; set; }

        public FasterLogAdapterFactory(
            string providerName,
            FasterLogAdapterOptions fasterLogAdapterOptions,
            HashRingStreamQueueMapperOptions queueMapperOptions,
            SimpleQueueCacheOptions cacheOptions,
            IServiceProvider serviceProvider,
            IOptions<ClusterOptions> clusterOptions,
            SerializationManager serializationManager,
            ILoggerFactory loggerFactory) {
            _providerName = providerName;
            _fasterLogAdapterOptions = fasterLogAdapterOptions;
            _queueMapperOptions = queueMapperOptions;
            //_cacheOptions = cacheOptions;
            _serviceProvider = serviceProvider;
            _clusterOptions = clusterOptions.Value;
            _serializationManager = FasterLogSerializationManager.Create(loggerFactory, serializationManager);
            _loggerFactory = loggerFactory;

            _streamQueueMapper = CreateStreamQueueMapper(queueMapperOptions, _providerName);
            _adapterCache = CreateSimpleQueueAdapterCache(cacheOptions, _providerName, _loggerFactory);
        }

        internal static HashRingBasedStreamQueueMapper CreateStreamQueueMapper(HashRingStreamQueueMapperOptions queueMapperOptions, string queueNamePrefix) => new HashRingBasedStreamQueueMapper(queueMapperOptions, queueNamePrefix);

        internal static SimpleQueueAdapterCache CreateSimpleQueueAdapterCache(SimpleQueueCacheOptions cacheOptions, string providerName, ILoggerFactory loggerFactory) => new SimpleQueueAdapterCache(cacheOptions, providerName, loggerFactory);

        internal static HashRingBasedStreamQueueMapper CreateStreamQueueMapper() => new HashRingBasedStreamQueueMapper(new HashRingStreamQueueMapperOptions(), "Quickstep");

        internal static SimpleQueueAdapterCache CreateSimpleQueueAdapterCache(ILoggerFactory loggerFactory) => new SimpleQueueAdapterCache(new SimpleQueueCacheOptions(), "Quickstep", loggerFactory);

        public static FasterLogAdapterFactory Create(IServiceProvider services, string name) {
            var fasterLogAdapterOptions = services.GetOptionsByName<FasterLogAdapterOptions>(name);
            var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
            var queueMapperOptions = services.GetOptionsByName<HashRingStreamQueueMapperOptions>(name);

            // todo 2021-04 stupid
            fasterLogAdapterOptions.QueueCount = queueMapperOptions.TotalQueueCount;

            var clusterOptions = services.GetProviderClusterOptions(name);
            var factory = ActivatorUtilities.CreateInstance<FasterLogAdapterFactory>(services, name, fasterLogAdapterOptions, cacheOptions, queueMapperOptions, clusterOptions);
            factory.Init();
            return factory;
        }

        public virtual void Init() {
            // todo 2021-04-21 Needs implementation
            StreamFailureHandlerFactory ??= queueId => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
        }

        public Task<IQueueAdapter> CreateAdapter() {
            var adapter = new FasterLogAdapter(_serializationManager, _streamQueueMapper, _loggerFactory, _fasterLogAdapterOptions, _clusterOptions.ServiceId, _providerName);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;

        public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) => StreamFailureHandlerFactory(queueId);
    }
}