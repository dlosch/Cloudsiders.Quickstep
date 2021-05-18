using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Cloudsiders.Quickstep.Serialization;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Cloudsiders.Quickstep {
    internal class FasterQueue : IQueueAdapterReceiver, IFasterQueueSubscription {
        private readonly FasterLogSerializationManager _serializationManager;
        private readonly QueueId _queueId;
        private readonly FasterLogAdapterOptions _fasterLogAdapterOptions;
        private readonly string _serviceId;
        private readonly FasterLogStorage _fasterLogStorage;
        private readonly ILogger<FasterQueue> _logger;
        internal bool IsInitialized => _channel != null;
        private readonly object _initLock = new object();
        private Channel<LogMessage> _channel;
        private readonly bool _logDebugEnabled;
        private ChannelWriter<LogMessage> ChannelWriter => _channel.Writer;
        public uint StreamId => _queueId.GetNumericId();

        public FasterQueue(FasterLogSerializationManager serializationManager, ILoggerFactory loggerFactory, QueueId queueId, FasterLogAdapterOptions fasterLogAdapterOptions, string serviceId, FasterLogStorage fasterLogStorage, bool initChannelImmediately = true) {
            _serializationManager = serializationManager;
            _queueId = queueId;
            _fasterLogAdapterOptions = fasterLogAdapterOptions;
            _serviceId = serviceId;
            _fasterLogStorage = fasterLogStorage;
            _logger = loggerFactory.CreateLogger<FasterQueue>();

            _logDebugEnabled = _logger.IsEnabled(LogLevel.Debug);
            
            if (initChannelImmediately) LazyInitialize();
        }


        internal void LazyInitialize() {
            if (IsInitialized) return;
            lock (_initLock) {
                if (IsInitialized) return;

                if (_fasterLogAdapterOptions.ChannelBacklogSize <= 0) {
                    _logger.LogInformation("{source} create unbounded channel ...", nameof(FasterQueue));
                    // todo
                    var unbounded = new UnboundedChannelOptions {
                        AllowSynchronousContinuations = true,
                        SingleReader = false,
                        SingleWriter = false,
                    };
                    _channel = Channel.CreateUnbounded<LogMessage>(unbounded);
                }
                else {
                    _logger.LogInformation("{source} create bounded channel with backlog size {size} ...", nameof(FasterQueue), _fasterLogAdapterOptions.ChannelBacklogSize);

                    var bounded = new BoundedChannelOptions(_fasterLogAdapterOptions.ChannelBacklogSize) {
                        SingleWriter = false,
                        SingleReader = false,
                        FullMode = BoundedChannelFullMode.Wait,
                        AllowSynchronousContinuations = true,
                    };
                    _channel = Channel.CreateBounded<LogMessage>(bounded);
                }
            }
        }

        internal async Task AddMessage(LogMessage message) {
            // todo possibly estimate the size of the message
            // todo is using await using when available always better?
            await using var memory = new MemorySegmentBasedBuffer();
            var bytesWritten = _serializationManager.OrleansSerializerSerialize(memory, message);

            // todo HIGH 202104 The way FASTER handles IReadOnlySpanBatch is it places n messages in its log. The messages would need to be appended manually on reading
            // todo HIGH 20210413 better memory allocation this one copies
            await _fasterLogStorage.AddMessage(memory);
        }

        public ValueTask Accept(Guid streamId, string streamNamespace, uint sequence, MemorySegment[] messageParts, int cMessageParts, long currentAddress, int cbSize) {
            var memorySegmentReader = new MemorySegmentBasedBufferReader(messageParts);

            try {
                var msg = _serializationManager.OrleansSerializerDeserialize(memorySegmentReader);
                if (null == msg) {
                    _logger.LogWarning("{source} Deserializer returned null, streamId={streamId}, {streamNamespace}, currentAddress={currentAddress}", nameof(Accept), streamId, streamNamespace, currentAddress);
                    return new ValueTask();
                }

                msg.FasterLogAddress = currentAddress;
                msg.FasterLogLength = cbSize;
                msg.StreamGuid = streamId;
                msg.StreamNamespace = streamNamespace;

                return ChannelWriter.WriteAsync(msg);
            }
            catch (Exception e) {
                _logger.LogError(e, "{source} (multiple message parts) Deserialization failed ...", nameof(Accept));

                return new ValueTask();
            }
        }

        // todo the locking blocking behaviour of this code ... not sure if it is good or bad
        // messagepump, single thread, scaniterator overFasterlog 
        // demux to n FasterQueue
        // FasterQueue decouple from the Faster Log by using in memory channelWriter/ChannelReader
        // at the end ... still a pull model
        // the orleans runtime issues a ("blocking") GetQueueMessagesAsync call on each queue 
        public ValueTask Accept(Guid streamId, string streamNamespace, uint sequence, ReadOnlyMemory<byte> readOnlyMemory, long currentAddress, int cbSize) {
            // todo 20210503 Logger.IsEnabled only makes sense when caching the boolean and doing if (cachedBool) ... otherwise, IsEnabled and Log will do the check twice

            try {
                var msg = _serializationManager.OrleansSerializerDeserialize(readOnlyMemory);

                msg.FasterLogAddress = currentAddress;
                msg.FasterLogLength = cbSize;
                msg.StreamGuid = streamId;
                msg.StreamNamespace = streamNamespace;

                return ChannelWriter.WriteAsync(msg);
            }
            catch (Exception e) {
                _logger.LogError(e, "{source} (single message parts) Deserialization failed ...", nameof(Accept));

                return new ValueTask();
            }
        }

        public Task Initialize(TimeSpan timeout) => _fasterLogStorage.InitQueueAsync(this, timeout);
        
        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount) {
            var canRead = await _channel.Reader.WaitToReadAsync();
            if (!canRead) {
                if (_logDebugEnabled) _logger.LogDebug("{source} Channel.Reader.WaitToReadAsync returned false.", nameof(GetQueueMessagesAsync));
                return null;
            }

            // todo possibly limit the maxcounyt
            var list = new List<IBatchContainer>(maxCount);

            for (var idx = 0; idx < maxCount; idx++) {
                var msgAvailable = _channel.Reader.TryRead(out var msg);
                if (!msgAvailable) break;

                if (null == msg) break;

                // i dont think i need to demux here / alle messages from one batchcontainer should be in the same FasterLog-Entry

                // todo events and sequencetoken should not be null
                // sequencetoken may be null if sender didnt specify one ...
                list.Add(msg);
            }

            return list;
        }

        // todo 20210408 lets make this dirty by always committing until the HIGHEST message address in the List of Batch Containers.
        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages) {
            if (null == messages || !messages.Any()) return Task.CompletedTask;
            var maxLogAddress = messages.Cast<LogMessage>().Max(bc => bc.FasterLogAddress);

            _logger.LogInformation("{source} Streamid={streamId} {maxSequence}", nameof(MessagesDeliveredAsync), StreamId, maxLogAddress);

            return _fasterLogStorage.HighWatermarkAsync(StreamId, maxLogAddress);
        }
        public void Unsubscribed() { }

        public Task Shutdown(TimeSpan timeout) => _fasterLogStorage.UnsubscribeQueue(this, timeout);
    }
}