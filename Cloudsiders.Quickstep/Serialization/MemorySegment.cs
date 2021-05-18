using System;
using System.Buffers;

namespace Cloudsiders.Quickstep.Serialization {
    // todo probably not the smartest approach, though this is allocated once and reused.
    internal sealed class MemorySegment {
        internal IMemoryOwner<byte> MemoryOwner;
        internal ReadOnlyMemory<byte> Memory;
        internal int BytesWritten;
    }
}