using System;
using System.Threading.Tasks;
using Cloudsiders.Quickstep.Serialization;

namespace Cloudsiders.Quickstep {
    internal interface IFasterQueueSubscription {
        uint StreamId { get; }

        void Unsubscribed();

        ValueTask Accept(Guid streamId, string streamNamespace, uint sequence, ReadOnlyMemory<byte> buffer, long currentAddress = 0L, int chSize = 0);

        ValueTask Accept(Guid streamId, string streamNamespace, uint sequence, MemorySegment[] parts, int cp, long currentAddress = 0L, int chSize = 0);
    }
}