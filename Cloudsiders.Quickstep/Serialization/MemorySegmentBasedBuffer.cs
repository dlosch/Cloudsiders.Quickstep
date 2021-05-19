using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Cloudsiders.Quickstep.Serialization {
    internal sealed class MemorySegmentBasedBuffer : Stream, FASTER.core.IReadOnlySpanBatch {
        private readonly MemoryPool<byte> _memoryPool;

        private const int DefaultAllocationSize = 1024;

        private IMemoryOwner<byte> currentOwner;
        private MemorySegment[] _segments = new MemorySegment[3];
        private int _nextSegmentOffset = 0;
        private Memory<byte> currentBuffer;
        private int currentOffset;

        public Memory<byte> Memory(int sizeHint = 0) {
            if (sizeHint > 0) {
                EnsureContiguous(sizeHint);
                return currentBuffer.Slice(currentOffset, sizeHint);
            }
            else {
                return currentBuffer.Slice(currentOffset);
            }
        }

        public MemorySegmentBasedBuffer(MemoryPool<byte> memoryPool) {
            _memoryPool = memoryPool;

            Init();
        }

        public MemorySegmentBasedBuffer() {
            _memoryPool = MemoryPool<byte>.Shared;
            Init();
        }

        protected override void Dispose(bool disposing) {
            foreach (var item in _segments) {
                if (null == item) continue;
                item.MemoryOwner?.Dispose();
                item.MemoryOwner = null;
            }

            base.Dispose(disposing);
            GC.SuppressFinalize(this);
        }


        ~MemorySegmentBasedBuffer() {
            Dispose(false);
        }


        public int TotalEntries() => _nextSegmentOffset + 1;

        public ReadOnlySpan<byte> Get(int index)
            => _segments.Length <= index || _segments[index] == null
                ? currentBuffer.Slice(0, currentOffset).Span
                : _segments[index].Memory /*.Memory*/.Slice(0, _segments[index].BytesWritten).Span;


        private void Init() {
            currentOwner = _memoryPool.Rent(DefaultAllocationSize);
            currentBuffer = currentOwner.Memory;
            currentOffset = 0;
        }


        internal void EnsureContiguous(int minlength) {
            if (minlength > 0 && currentBuffer.Length - currentOffset >= minlength) return;

            var allocate = Math.Max(DefaultAllocationSize, minlength);
            var buffer = _memoryPool.Rent(allocate);

            if (_nextSegmentOffset + 1 > _segments.Length) {
                var newSegments = new MemorySegment[_segments.Length << 1];
                Array.Copy(_segments, newSegments, _segments.Length);
                _segments = newSegments;
            }

            if (_segments[_nextSegmentOffset] == null) {
                _segments[_nextSegmentOffset] = new MemorySegment { BytesWritten = currentOffset, MemoryOwner = currentOwner, Memory = currentOwner.Memory };
            }

            currentOwner = buffer;
            currentBuffer = buffer.Memory;
            currentOffset = 0;
            _nextSegmentOffset++;
        }


        internal Span<byte> WritableSpan => currentBuffer.Slice(currentOffset).Span;

        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotImplementedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotImplementedException();

        public override void SetLength(long value) {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count) {
            throw new NotImplementedException();
        }

        // todo HIGH virtual cannot be inlined / do not use interface ... ??
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Write(ReadOnlySpan<byte> value) {
            //// Fast path, try copying to the current buffer.
            if (value.Length <= currentBuffer.Length - currentOffset) {
                value.CopyTo(WritableSpan);
                currentOffset += value.Length;
            }
            else {
                WriteMultiSegment(in value);
            }
        }

        private void WriteMultiSegment(in ReadOnlySpan<byte> source) {
            var input = source;
            while (true) {
                // Write as much as possible/necessary into the current segment.
                var writeSize = Math.Min(currentBuffer.Length - currentOffset, input.Length);
                input.Slice(0, writeSize).CopyTo(WritableSpan);
                currentOffset += writeSize;

                EnsureContiguous(source.Length - writeSize);

                input = input.Slice(writeSize);

                if (input.Length == 0) return;

                // The current segment is full but there is more to write.
                //this.Allocate(input.Length);
            }
        }

        internal void Advance(int cbHeader) {
            currentOffset += cbHeader;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => base.WriteAsync(buffer, offset, count, cancellationToken);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = new CancellationToken()) {
            Write(buffer.Span);
            return new ValueTask();
        }

        public override void WriteByte(byte value) => Write(value);

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => currentOffset + _segments.Where(s => s != null).Sum(s => s.BytesWritten);

        public override long Position {
            get => Length;
            set { }
        }
//#if DEBUG
//        // debug only as bad as it gets
//        internal MemorySegment[] AllSegments {
//            get {
//                //var owner = this._memoryPool.Rent(unchecked((int)this.Length));

//                //return new MemorySegment2[] {
//                //                                new MemorySegment2 {
//                //                                                       BytesWritten = unchecked((int)this.Length),
//                //                                                       Memory = owner.Memory,
//                //                                                       MemoryOwner = owner
//                //                                                   }
//                //                            };


//                var segments = new MemorySegment[_segments.Count(s => s?.MemoryOwner != null) + 1];
//                for (int idx = 0; idx < segments.Length - 1; idx++) {
//                    segments[idx] = _segments[idx];
//                }

//                segments[^1] = new MemorySegment {
//                    BytesWritten = this.currentOffset,
//                    MemoryOwner = this.currentOwner,
//                    Memory = this.currentBuffer
//                };
//                return segments;

//            }
//        }
//#endif

#region

        public void Write(byte b) {
            WritableSpan[0] = b;
            currentOffset++;
            //const int width = sizeof(byte);
            //this.EnsureContiguous(width);
            //this.WritableSpan[0] = b;
            //this.currentOffset += width;
        }

        public void Write(sbyte b) {
            const int width = sizeof(sbyte);
            //this.EnsureContiguous(width);
            WritableSpan[0] = (byte)b;
            currentOffset += width;
        }

        public void Write(float i) {
            ReadOnlySpan<float> span = stackalloc float[1] { i };
            Write(MemoryMarshal.Cast<float, byte>(span));
        }

        public void Write(double i) {
            ReadOnlySpan<double> span = stackalloc double[1] { i };
            Write(MemoryMarshal.Cast<double, byte>(span));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(short value) {
            const int width = sizeof(short);
            EnsureContiguous(width);
            BinaryPrimitives.WriteInt16LittleEndian(WritableSpan, value);
            currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(int value) {
            const int width = sizeof(int);
            EnsureContiguous(width);
            BinaryPrimitives.WriteInt32LittleEndian(WritableSpan, value);
            currentOffset += width;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(long value) {
            const int width = sizeof(long);
            EnsureContiguous(width);
            BinaryPrimitives.WriteInt64LittleEndian(WritableSpan, value);
            currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(uint value) {
            const int width = sizeof(uint);
            EnsureContiguous(width);
            BinaryPrimitives.WriteUInt32LittleEndian(WritableSpan, value);
            currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ushort value) {
            const int width = sizeof(ushort);
            EnsureContiguous(width);
            BinaryPrimitives.WriteUInt16LittleEndian(WritableSpan, value);
            currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ulong value) {
            const int width = sizeof(ulong);
            EnsureContiguous(width);
            BinaryPrimitives.WriteUInt64LittleEndian(WritableSpan, value);
            currentOffset += width;
        }

#endregion
    }
}