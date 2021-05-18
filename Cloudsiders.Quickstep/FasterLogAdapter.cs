using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Streams;

namespace Cloudsiders.Quickstep {
    public class FasterLogAdapter : IQueueAdapter {
        private readonly FasterLogSerializationManager _serializationManager;
        private readonly /*IConsistentRingStreamQueueMapper*/ HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly ILoggerFactory _loggerFactory;
        private readonly FasterLogAdapterOptions _fasterLogAdapterOptions;
        private readonly string _serviceId;
        private readonly string _providerName;

        // todo initial size
        // todo 20210424 concurrency level
        private readonly ConcurrentDictionary<QueueId, FasterQueue> _queues = new ConcurrentDictionary<QueueId, FasterQueue>(HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES, HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES);
        private readonly FasterLogStorage _fasterLogStorage;
        private readonly ILogger<FasterLogAdapter> _logger;

        public FasterLogAdapter(
            FasterLogSerializationManager serializationManager
            , HashRingBasedStreamQueueMapper streamQueueMapper
            , ILoggerFactory loggerFactory
            , FasterLogAdapterOptions fasterLogAdapterOptions
            , string serviceId
            , string providerName) {
            fasterLogAdapterOptions.Validate();
            if (string.IsNullOrEmpty(serviceId)) throw new ArgumentNullException(nameof(serviceId));

            _serializationManager = serializationManager;
            _streamQueueMapper = streamQueueMapper;
            _loggerFactory = loggerFactory;
            _logger = _loggerFactory.CreateLogger<FasterLogAdapter>();
            _fasterLogAdapterOptions = fasterLogAdapterOptions;
            _serviceId = serviceId;
            _providerName = providerName;

            _fasterLogStorage = new FasterLogStorage(streamQueueMapper, loggerFactory, fasterLogAdapterOptions, serviceId);
        }


        public IQueueAdapterReceiver CreateReceiver(QueueId queueId) => CreateFasterQueue(queueId);

        private FasterQueue CreateFasterQueueNoInit(QueueId queueId) => new FasterQueue(_serializationManager, _loggerFactory, queueId, _fasterLogAdapterOptions, _serviceId, _fasterLogStorage, false);

        private FasterQueue CreateFasterQueue(QueueId queueId) {
            var queue = new FasterQueue(_serializationManager, _loggerFactory, queueId, _fasterLogAdapterOptions, _serviceId, _fasterLogStorage);
            if (!_queues.TryAdd(queueId, queue)) {
                // todo handle error
                throw new Exception($"{nameof(FasterLogAdapter)}::{nameof(CreateReceiver)} failed to add queue queueid={queueId?.ToString()}");
            }

            return queue;
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext) {
            // todo 20210424 sequencetoken management

            var queue = ResolveQueue(streamGuid, streamNamespace);
            var msg = LogMessageFactory.ToMessage(_serializationManager, streamGuid, streamNamespace, events, requestContext, token);
            return queue.AddMessage(msg);
        }

        private FasterQueue ResolveQueue(Guid streamGuid, string streamNamespace) {
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);

            // https://gist.github.com/davidfowl/3dac8f7b3d141ae87abf770d5781feed
            var queue = _queues.GetOrAdd(queueId, CreateFasterQueueNoInit(queueId));
            if (null == queue) throw new Exception($"{nameof(CreateFasterQueueNoInit)} returned null");
            if (!queue.IsInitialized) queue.LazyInitialize();
            return queue;
        }

        public string Name => _providerName;

        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;
    }
}