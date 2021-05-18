using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Cloudsiders.Quickstep.Serialization;
using FASTER.core;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Cloudsiders.Quickstep {
    internal class FasterLogStorage : IAsyncDisposable {
        private readonly ConcurrentDictionary<uint, IFasterQueueSubscription> _subscriptions;

        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        
        private readonly ILogger<FasterLogStorage> _logger;

        private readonly FasterLogAdapterOptions _fasterLogOptions;

        private readonly string _serviceId;

        private readonly FasterLog _fasterLog;

        private readonly CancellationTokenSource _commitAndFlushTaskCancellationTokenSource;

        private readonly string _instanceLogPrefix;

        public FasterLogStorage(HashRingBasedStreamQueueMapper streamQueueMapper, ILoggerFactory loggerFactory, FasterLogAdapterOptions fasterLogOptions, string serviceId) {
            // todo 20210503 concurrency level and initial size. read only from messagePump ... do I even need concurrent dict?
            _subscriptions = new ConcurrentDictionary<uint, IFasterQueueSubscription>();
            _streamQueueMapper = streamQueueMapper;
            _fasterLogOptions = fasterLogOptions;
            _serviceId = serviceId;
            _logger = loggerFactory.CreateLogger<FasterLogStorage>();

            _fasterLog = InitializeFasterLog(fasterLogOptions);

            _instanceLogPrefix = $"{serviceId}-{Guid.NewGuid()}";

            _useWatermarks = fasterLogOptions.UseWatermarkForCommits;
            if (_useWatermarks) InitWatermarks();

            _commitAndFlushTaskCancellationTokenSource = new CancellationTokenSource();
            _commitAndFlushTask = InitializeCommitAndFlushTask(fasterLogOptions.CommitPeriod, _commitAndFlushTaskCancellationTokenSource);
        }

        private Task InitializeCommitAndFlushTask(TimeSpan commitPeriod, CancellationTokenSource cancellationTokenSource) {
            async Task Action() {
                var dbgLog = _logger.IsEnabled(LogLevel.Debug);
                var infoLog = _logger.IsEnabled(LogLevel.Information);

                while (!cancellationTokenSource.IsCancellationRequested) {
                    try {
                        await Task.Delay(commitPeriod, cancellationTokenSource.Token);

                        if (_useWatermarks) {
                            var lngCommitAddress = GetCommitWatermark();
                            if (infoLog) _logger.LogInformation("{source}-{instance}: Using watermarks, highest new commit address for scan iterator {newAddress}, completed until before {oldAddress}", nameof(InitializeCommitAndFlushTask), _instanceLogPrefix, lngCommitAddress, _scanIterator?.CompletedUntilAddress);

                            // todo 202104 lock on scanIIterator, may be disposed here
                            if (null != _scanIterator) {
                                if (lngCommitAddress > 0L && _scanIterator.CompletedUntilAddress < lngCommitAddress) {
                                    _scanIterator.CompleteUntil(lngCommitAddress);
                                }
                            }
                        }

                        await _fasterLog.CommitAsync(cancellationTokenSource.Token);
                        if (dbgLog) _logger.LogDebug("{source}-{instance}: FASTER log committed.", nameof(InitializeCommitAndFlushTask), _instanceLogPrefix);
                        if (infoLog) _logger.LogInformation("{source}-{instance}: FASTER log committed. Message Count {msg}, delivered {msgDelivered}", nameof(InitializeCommitAndFlushTask), _instanceLogPrefix, _messageCount, _messageCountDelivered);

                        // todo periodic truncate
                        //await _fasterLog.TruncateUntilPageStart()
                    }
                    catch (OperationCanceledException operationCanceledException) {
                        _logger.LogInformation("{source}-{instance} Operation was cancelled. {operationCanceledException}", nameof(FasterLogStorage), _instanceLogPrefix, operationCanceledException.Message);
                    }
                }

                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                try {
                    await _fasterLog.CommitAsync(cts.Token);
                }
                catch (OperationCanceledException) { }
            }

            // todo 20200926
            return Task.Factory.StartNew(Action,
                                         cancellationTokenSource.Token,
                                         TaskCreationOptions.LongRunning,
                                         TaskScheduler.Default);
        }

        internal static FasterLog InitializeFasterLog(FasterLogAdapterOptions fasterLogOptions) {
            var device = Devices.CreateLogDevice(fasterLogOptions.LogFile, fasterLogOptions.PreallocateLogFile, !fasterLogOptions.UsePersistentLog, recoverDevice: fasterLogOptions.RecoverDevice);

            var fasterLogSettings = new FasterLogSettings {
                LogDevice = device,
                SegmentSizeBits = fasterLogOptions.LogSegmentSizeBits,
                // todo check for 0, wird bei 0 zu klein initialisiert
                //MemorySizeBits = fasterLogOptions.LogMemorySizeBits,
                //PageSizeBits = fasterLogOptions.LogPageSizeBits,
            };

            var log = new FasterLog(fasterLogSettings);

            return log;
        }

        private readonly object _pumpTaskLock = new object();
        private bool _pumpTaskCreated = false;
        private Task _pumpTask;
        private readonly Task _commitAndFlushTask;

        private FasterLogScanIterator _scanIterator;
        private CancellationTokenSource _scanIteratorCancellationTokenSource;


        private int _messageCount;
        private int _messageCountDelivered;

        // todo 20210407 ensure exactly one scaniterator per Logfile, as multiple scaniterators will iterate over the same data
        // probably makes more sense Queue 1:1 FasterLog not 8:1
        // or partition the logfile and use multiple scaniterators non overlapping with different start address
        private async Task MessagePump(CancellationToken cancellationToken) {
            var logDebug = _logger.IsEnabled(LogLevel.Debug);

            if (_logger.IsEnabled(LogLevel.Information)) {
                _logger.LogInformation("{source}-{instance} starting message pump from {logFile} ...", nameof(MessagePump), _instanceLogPrefix, _fasterLogOptions.LogFile);
            }

            try {
                var disposeMemowners = false;
                // allocated once and reused
                var messageParts = new MemorySegment[] {
                    new MemorySegment { },
                    new MemorySegment { },
                    new MemorySegment { },
                    new MemorySegment { },
                    new MemorySegment { },
                    new MemorySegment { },
                    new MemorySegment { },
                    new MemorySegment { },
                };
                
                while (!cancellationToken.IsCancellationRequested) {
                    if (await _scanIterator.WaitAsync(cancellationToken)) {
                        
                        // todo potentially use custom memory pool
                        var memoryPool = MemoryPool<byte>.Shared;
                        var lastAddress = default(long);
                        try {
                            await foreach (var (memoryOwner, cbSize, currentAddress, nextAddressUnused) in _scanIterator
                                                                                                           .GetAsyncEnumerable(memoryPool, cancellationToken)
                                                                                                           .WithCancellation(cancellationToken))
                            {
                                var highestAddress = currentAddress;
                                try {
                                
                                    // streamId must be parsed here for demux to correct queue
                                    if (cbSize > FasterLogMessageConstants.MessageHeaderSizeMin) {
                                        var cbHeader = MessageHeader.DeconstructMessageHeader(memoryOwner.Memory, cbSize, out var streamId, out var streamGuid, out var streamNamespace, out var sequence, out var msgSize, out var msgParts);
                                        _messageCount++;
                                        _logger.LogInformation("{source}-{instance}: message streamGuid={streamGuid}, namespace={streamNamespace}. parts {parts}, size {size}", nameof(MessagePump), _instanceLogPrefix, streamGuid, streamNamespace, msgParts, msgSize);

                                        var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);

                                        if (msgParts > FasterLogMessageConstants.MaxMessageParts) {
                                            _logger.LogWarning("{source}-{instance}: message has more than max number of supported message parts {count} with streamId={streamId}, streamGuid={streamGuid}, namespace={streamNamespace}. Message will be discarded.", nameof(MessagePump), _instanceLogPrefix, msgParts, streamId, streamGuid, streamNamespace);

                                            SkipFwdToNextMessage(memoryPool, msgParts);
                                        }
                                        else if (!SubscriptionFromStreamId(queueId.GetNumericId(), out var sub)) {
                                            _logger.LogWarning("{source}-{instance}: no active subscription found for message with streamId={streamId}, streamGuid={streamGuid}, namespace={streamNamespace}. Message will be discarded.", nameof(MessagePump), _instanceLogPrefix, streamId, streamGuid, streamNamespace);

                                            SkipFwdToNextMessage(memoryPool, msgParts);
                                        }
                                        else {
                                            _logger.LogInformation("{source}-{instance}: delivering message queueId={queueId}, streamGuid={streamGuid}, namespace={streamNamespace}. parts {parts}, size {size}", nameof(MessagePump), _instanceLogPrefix, queueId, streamGuid, streamNamespace, msgSize, msgParts);

                                            if (msgParts > 1) {
                                                if (logDebug) _logger.Log(LogLevel.Debug, "{source}-{instance} Quickstep message split over multiple {msgCount}/{msgSize} log messages.", nameof(MessagePump), _instanceLogPrefix, msgParts, msgSize);

                                                messageParts[0].MemoryOwner = memoryOwner;
                                                messageParts[0].Memory = memoryOwner.Memory.Slice(cbHeader, cbSize - cbHeader);
                                                messageParts[0].BytesWritten = cbSize - cbHeader;
                                                
                                                for (var idx = 0; idx < msgParts - 1; idx++) {
                                                    if (_scanIterator.GetNext(memoryPool, out var memoryOwnerPart, out var cbPart, out var partAddress)) {
                                                        messageParts[idx + 1].MemoryOwner = memoryOwnerPart;
                                                        messageParts[idx + 1].Memory = memoryOwnerPart.Memory[0..cbPart];
                                                        messageParts[idx + 1].BytesWritten = cbPart;

                                                        disposeMemowners = true;
                                                        highestAddress = partAddress;
                                                    }
                                                    else {
                                                        _logger.LogWarning("{source}-{instance} Quickstep message split over multiple {msgCount} log messages. Error reading {idx} message.", nameof(MessagePump), _instanceLogPrefix, msgParts, idx);
                                                    }
                                                }
                                            }

                                            _messageCountDelivered++;
                                            if(_useWatermarks) LowWatermark(queueId.GetNumericId(), highestAddress);

                                            // on error resume next :s
                                            try {
                                                if (disposeMemowners) {
                                                    await sub.Accept(streamGuid, streamNamespace, sequence, /*memoryOwner.Memory.Slice(cbHeader, cbSize - cbHeader), */messageParts, msgParts, currentAddress, cbSize);
                                                }
                                                else {
                                                    await sub.Accept(streamGuid, streamNamespace, sequence, memoryOwner.Memory.Slice(cbHeader, cbSize - cbHeader), currentAddress, cbSize);
                                                }
                                            }
                                            catch (Exception xcptn) {
                                                _logger.LogError(xcptn, "{source} fatal error delivering message.", nameof(MessagePump));
                                            }
                                        }
                                    }
                                }
                                finally {
                                    if (disposeMemowners) {
                                        for (var i = 0; i < messageParts.Length; i++) {
                                            if (null == messageParts[i].MemoryOwner) break;
                                            messageParts[i].MemoryOwner.Dispose();
                                            messageParts[i].MemoryOwner = null;
                                            messageParts[i].BytesWritten = 0;
                                        }

                                        disposeMemowners = false;
                                    }
                                    else {
                                        memoryOwner.Dispose();
                                    }

                                    lastAddress = currentAddress;
                                }
                            }
                        }
                        catch (OperationCanceledException opCancelled) {
                            var persistIterator = false;
                            _logger.LogWarning(opCancelled, "{source}-{instance} The message pump OP has been cancelled ... exiting loop. Persist last iterator address? {persistIterator}", nameof(MessagePump), _instanceLogPrefix, persistIterator);

                            // 20210407 persist iterator state not here but when message is marked as deliverd
                            if (persistIterator) {
                                // todo 20210424 use watermark if enabled
                                _scanIterator.CompleteUntil(lastAddress);
                                _fasterLog.Commit();
                            }
                        }
                    }
                }
            }
            catch ( /*TaskCanceledException*/OperationCanceledException taskCanceled) {
                _logger.LogWarning(taskCanceled, "{source}-{instance} The message pump task has been cancelled ... exiting outer loop/wait.", nameof(MessagePump), _instanceLogPrefix);
            }
            catch (Exception e) {
                _logger.LogError(e, "{source}-{instance} unhandled exception.", nameof(MessagePump), _instanceLogPrefix);
            }
            finally {
                using (_scanIterator) { }

                _scanIterator = default;
            }

            void SkipFwdToNextMessage(MemoryPool<byte> memoryPool, int msgParts) {
                if (msgParts > 1) {
                    for (var idx = 0; idx < msgParts - 1; idx++) {
                        if (_scanIterator.GetNext(memoryPool, out var memoryOwnerPart, out var cbPart, out var partAddress)) {
                            _logger.LogWarning("{source}-{instance}: no active subscription found for message. Message split over multiple {msgCount} parts, ignoring part {part}.", nameof(MessagePump), _instanceLogPrefix, msgParts, idx);
                            using (memoryOwnerPart) { }
                        }
                    }
                }
            }
        }

        private bool SubscriptionFromStreamId(uint streamId, out IFasterQueueSubscription sub) {
            if (_subscriptions.TryGetValue(streamId, out sub)) return true;
            sub = _subscriptions.First().Value;
            return sub != null;
        }

        private bool _shouldInitPumpTask = false;

        internal Task InitQueueAsync(IFasterQueueSubscription subscription, TimeSpan timespan) {
            if (!_subscriptions.TryAdd(subscription.StreamId, subscription)) {
                throw new Exception($"cannot add subscription {subscription.StreamId} to concurrent dict");
            }

            if (!_pumpTaskCreated && null == _pumpTask) {
                lock (_pumpTaskLock) {
                    if (!_pumpTaskCreated && null == _pumpTask && !_shouldInitPumpTask) {
                        _shouldInitPumpTask = true;
                    }
                }

                if (_shouldInitPumpTask) {
                    _pumpTaskCreated = true;

                    _logger.LogInformation("{source}-{instance} Create and start the PumpTask ...", nameof(InitQueueAsync), _instanceLogPrefix);

                    // _serviceid max 20 char
                    _scanIterator = _fasterLog.Scan(
                                                    // wenn der iterator recovered, wird die Adresse aus der Datei verwendet und nicht dieser Wert
                                                    _fasterLog.BeginAddress
                                                    // unbegrenzt
                                                    , long.MaxValue
                                                    // eindeutiger Name, max 20
                                                    , _serviceId,
                                                    // , null,
                                                    true);

                    // stupid
                    _scanIteratorCancellationTokenSource = new CancellationTokenSource();

                    _shouldInitPumpTask = false;

                    // todo 202004 HIGH this may actually initi the pumpTask multiple times
                    return Task.Factory.StartNew(async () => {
                        _pumpTask = MessagePump(_scanIteratorCancellationTokenSource.Token);
                        await _pumpTask;
                    }, _scanIteratorCancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                }
            }

            return Task.CompletedTask;
        }

        public Task UnsubscribeQueue([NotNull] IFasterQueueSubscription fasterQueueQueue, in TimeSpan timeout) {
            if (_subscriptions.TryRemove(fasterQueueQueue.StreamId, out var channel)) {
                channel.Unsubscribed();
            }

            // todo 202104 boom needs a thread exclusive lock!
            if (_subscriptions.Count == 0 && !_scanIteratorCancellationTokenSource.IsCancellationRequested) {
                // todo 202004 stop pump
                if (null != _pumpTask) {
                    lock (_pumpTaskLock) {
                        if (null != _pumpTask) {
                            var pumpTask = _pumpTask;
                            _pumpTask = null;
                            _logger.LogWarning("{source}-{instance} no more pending subscriptions ... stopping message pump.", nameof(UnsubscribeQueue), _instanceLogPrefix);

                            // this is fine inside the lock ...
                            return CancelMessagePump(pumpTask);
                        }
                    }
                }
            }

            return Task.CompletedTask;
        }

        private Task CancelMessagePump(Task pumpTask) {
            if (_scanIteratorCancellationTokenSource.IsCancellationRequested) return Task.CompletedTask;

            _scanIteratorCancellationTokenSource.Cancel();
            return pumpTask;
        }

        public async ValueTask DisposeAsync() {
            _logger.LogWarning("{source}-{instance}", nameof(DisposeAsync), _instanceLogPrefix);

            foreach (var fasterReceiverSubscription in _subscriptions) {
                fasterReceiverSubscription.Value.Unsubscribed();
            }

            Task pumpTask = default;
            if (null != _pumpTask) {
                lock (_pumpTaskLock) {
                    if (null != _pumpTask) {
                        pumpTask = _pumpTask;
                        _pumpTask = null;
                    }
                }
            }

            if (pumpTask != null) {
                // todo cancel pump
                await CancelMessagePump(pumpTask);
            }

            _subscriptions.Clear();

            if (null != _fasterLog) {
                //await _fasterLog.CommitAsync();
                _fasterLog.Dispose();
            }

            _commitAndFlushTaskCancellationTokenSource.Cancel();
            await _commitAndFlushTask;
        }

        private async ValueTask<long> AddMessageAndCommit(Memory<byte> buffer) {
            var lng = await _fasterLog.EnqueueAsync(buffer, CancellationToken.None);
            await _fasterLog.CommitAsync(CancellationToken.None);
            return lng;
        }

        private async ValueTask<long> AddMessageAndCommit(IReadOnlySpanBatch buffer) {
            var lng = await _fasterLog.EnqueueAsync(buffer, CancellationToken.None);
            await _fasterLog.CommitAsync(CancellationToken.None);
            return lng;
        }

        internal ValueTask<long> AddMessage(Memory<byte> buffer, bool commit = false) => commit ? AddMessageAndCommit(buffer) : _fasterLog.EnqueueAsync(buffer, CancellationToken.None);

        internal ValueTask<long> AddMessage(IReadOnlySpanBatch buffer, bool commit = false) => commit ? AddMessageAndCommit(buffer) : _fasterLog.EnqueueAsync(buffer, CancellationToken.None);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LowWatermark(uint id, long address) {
            if (!_useWatermarks) return;

            if (id == Watermark.ReservedStreamId) throw new Exception($"{nameof(FasterLogStorage)}::{nameof(LowWatermark)} QueueId == Watermark.ReservedStreamId 0x{Watermark.ReservedStreamId:x}");

            var idx = id; // % _watermarks.Length;
            for (; idx < id + _watermarks.Length; idx++) {
                var cur = idx % _watermarks.Length;

                // this should work since we always start to look for queueId x at the same index
                if (_watermarks[cur].StreamId == Watermark.ReservedStreamId) {
                    _logger.LogDebug("{source}-{instance} Low watermark {id}, index: {cur}, low address {address}", nameof(LowWatermark), _instanceLogPrefix, id, cur, address);
                    _watermarks[cur].StreamId = id;
                    _watermarks[cur].LowWatermark = address;
                    return;
                }
                else if (_watermarks[cur].StreamId == id) {
                    _watermarks[cur].LowWatermark = address;
                    return;
                }
            }
        }

        private Watermark[] _watermarks;

        internal class Watermark {
            internal const uint ReservedStreamId = (uint)0xFFFFFFFF; // should be uint.Max or if cast to int: -1

            internal uint StreamId = ReservedStreamId;
            internal long LowWatermark; // highest read address for queue
            internal long HighWatermark; // "highest" commit address for queue ... technically lower than the lowwatermark. nice naming!
        }

        private readonly bool _useWatermarks;

        internal long GetCommitWatermark() {
            var retVal = long.MaxValue;
            for (var i = 0; i < _watermarks.Length; i++) {
                if (_watermarks[i].StreamId != Watermark.ReservedStreamId && _watermarks[i].LowWatermark > 0L && _watermarks[i].HighWatermark > 0L) {
                    retVal = Math.Min(retVal, _watermarks[i].HighWatermark);
                }
            }

            return retVal == long.MaxValue ? 0L : retVal;
        }

        internal Task HighWatermarkAsync(uint id, long address) {
            if (!_useWatermarks) return Task.CompletedTask;

            if (id == Watermark.ReservedStreamId) throw new Exception($"{nameof(FasterLogStorage)}::{nameof(LowWatermark)} QueueId == Watermark.ReservedStreamId 0x{Watermark.ReservedStreamId:x}");

            var isDebug = _logger.IsEnabled(LogLevel.Debug);
            if (isDebug) _logger.Log(LogLevel.Debug, "{source}-{instance} queue id: {id}, high address {address}", nameof(HighWatermarkAsync), _instanceLogPrefix, id, address);

            var idx = id; // % _watermarks.Length;
            for (; idx < id + _watermarks.Length; idx++) {
                var cur = idx % _watermarks.Length;
                if (_watermarks[cur].StreamId == id) {
                    _watermarks[cur].HighWatermark = address;

                    break;
                }
            }

            return Task.CompletedTask;
        }

        private void InitWatermarks() {
            if (!_fasterLogOptions.QueueCount.HasValue) throw new Exception($"{nameof(FasterLogStorage)}::{nameof(InitWatermarks)} The queue count on the {nameof(FasterLogAdapterOptions)} has not been set.");
            var queueCount = _fasterLogOptions.QueueCount.Value;

            _watermarks = new Watermark[queueCount];
            for (var idx = 0; idx < queueCount; idx++) {
                _watermarks[idx] = new Watermark();
            }
        }
    }
}