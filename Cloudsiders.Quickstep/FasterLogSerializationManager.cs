using System;
using System.Buffers;
using Cloudsiders.Quickstep.Serialization;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Cloudsiders.Quickstep {
    public class FasterLogSerializationManager {
        private readonly ILogger<FasterLogSerializationManager> _logger;
        private readonly SerializationManager _serializationManager;

        internal int OrleansSerializerSerialize(MemorySegmentBasedBuffer memoryOwner, LogMessage value) {
            int SerNew2(MemorySegmentBasedBuffer memoryOwner, LogMessage value, int cbHeader) {
                var writer = new BinaryTokenStreamWriter3(memoryOwner);
                _serializationManager.Serialize(value, writer);
                return writer.CurrentOffset;
            }

            var headerMemPtr = memoryOwner.Memory(64);
            var cbHeader = MessageHeader.ConstructMessageHeader(headerMemPtr, value.StreamId, value.StreamGuid, value.StreamNamespace, value.Sequence, 0u);
            memoryOwner.Advance(cbHeader);

            var cbWritten = SerNew2(memoryOwner, value, cbHeader);
            MessageHeader.UpdateMessageSize(headerMemPtr, cbWritten, memoryOwner.TotalEntries());

            return cbWritten;
        }

        internal LogMessage OrleansSerializerDeserialize(ReadOnlyMemory<byte> buffer) {
            var sequence = new ReadOnlySequence<byte>(buffer);
            var reader = new BinaryTokenStreamReader2(sequence);
            var messsge = _serializationManager.Deserialize<LogMessage>(reader);
            return messsge;
        }

        internal LogMessage OrleansSerializerDeserialize(MemorySegmentBasedBufferReader segmentBasedBufferReader) {
            var reader = new BinaryTokenStreamReader3(segmentBasedBufferReader);
            var messsge = _serializationManager.Deserialize<LogMessage>(reader);
            return messsge;
        }

        public FasterLogSerializationManager(ILogger<FasterLogSerializationManager> logger, SerializationManager serializationManager) {
            _logger = logger;
            _serializationManager = serializationManager;
        }

        public static FasterLogSerializationManager Create(ILoggerFactory loggerFactory, SerializationManager serializationManager)
            // todo implement
            => new FasterLogSerializationManager(loggerFactory.CreateLogger<FasterLogSerializationManager>(), serializationManager);
    }
}