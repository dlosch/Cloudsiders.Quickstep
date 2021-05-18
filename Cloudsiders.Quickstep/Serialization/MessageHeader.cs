using System;
using System.Text;

namespace Cloudsiders.Quickstep.Serialization {
    internal static class MessageHeader {
        // todo add a magic number prefix
        // todo what is the adequate utf8 encode/decode?
        internal static int ConstructMessageHeader(in Memory<byte> memoryOwnerMemory, in uint valueStreamId, Guid valueStreamGuid, in string streamNamespace, in uint valueSequence, in uint msgSize) {
            if (memoryOwnerMemory.Length < 32) throw new Exception("Memory size is too small for message header. At least 31 + 1 byte needed.");

            var span = memoryOwnerMemory.Span;
            // todo serialization
            // todo maximum stupid
            span[0] = unchecked((byte)valueStreamId);
            span[1] = unchecked((byte)(valueStreamId >> 8));
            span[2] = unchecked((byte)(valueStreamId >> 16));
            span[3] = unchecked((byte)(valueStreamId >> 24));

            valueStreamGuid.TryWriteBytes(span.Slice(4, 16));

            span[20] = unchecked((byte)valueSequence);
            span[21] = unchecked((byte)(valueSequence >> 8));
            span[22] = unchecked((byte)(valueSequence >> 16));
            span[23] = unchecked((byte)(valueSequence >> 24));

            span[24] = unchecked((byte)msgSize);
            span[25] = unchecked((byte)(msgSize >> 8));
            span[26] = unchecked((byte)(msgSize >> 16));
            span[27] = unchecked((byte)(msgSize >> 24));

            // todo 20200419 use System.Buffers.Text.Utf8formatter/Parser
            // todo 202104 unsure whether sharing a static instance is actually smart. Dont think so ... 
            var bytesUsed = Encoding.UTF8.GetBytes(streamNamespace.AsSpan(), span.Slice(31));

            //Encoding.UTF8.GetEncoder().Convert(streamNamespace.AsSpan(), span[30..] /*.Slice(30)*/, true, out var _, out var bytesUsed, out bool completed);
            //var bytesUsed = 0;
            //lock (encoder) {
            //    encoder.Reset();
            //    encoder.Convert(streamNamespace.AsSpan(), span.Slice(30), true, out var _, out bytesUsed, out bool completed);
            //}
            var snsBytes = (ushort)bytesUsed;
            span[28] = unchecked((byte)snsBytes);
            span[29] = unchecked((byte)(snsBytes >> 8));

            return 31 + bytesUsed;
        }

        internal static int DeconstructMessageHeader(in Memory<byte> memoryOwner, in int cbSize, out uint streamId, out Guid streamGuid, out string streamNamespace, out uint sequence, out uint msgSize, out int msgParts) {
            var span = memoryOwner.Span;
            streamId = BitConverter.ToUInt32(span);
            streamGuid = new Guid(span.Slice(4, 16));
            sequence = BitConverter.ToUInt32(span.Slice(20, 4));
            msgSize = BitConverter.ToUInt32(span.Slice(24, 4));

            //System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian()

            msgParts = span[30];

            var snSize = BitConverter.ToUInt16(span.Slice(28));
            if (snSize == 0) {
                streamNamespace = default;
            }
            else {
                streamNamespace = Encoding.UTF8.GetString(span.Slice(31, snSize));
            }

            return 31 + snSize;
        }

        public static void UpdateMessageSize(Memory<byte> memoryOwnerMemory, int msgSize, int totalMemoryRanges = 1) {
            var span = memoryOwnerMemory.Span;
            span[24] = unchecked((byte)msgSize);
            span[25] = unchecked((byte)(msgSize >> 8));
            span[26] = unchecked((byte)(msgSize >> 16));
            span[27] = unchecked((byte)(msgSize >> 24));
            span[30] = unchecked((byte)totalMemoryRanges);
        }
    }
}