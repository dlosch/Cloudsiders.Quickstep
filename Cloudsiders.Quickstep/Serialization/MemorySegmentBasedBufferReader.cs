using System;
using System.Buffers.Binary;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Cloudsiders.Quickstep.Serialization {
    // this doesnt handle cases, where n bytes of a single entity are split over multiple buffers - because we dont write that. #revisit
    internal sealed class MemorySegmentBasedBufferReader {
        private readonly MemorySegment[] _segments;

        private int _segmentIndex = 0;
        private int _segmentCurrentPosition = 0;
        private int _finishedSegmentsIndex = 0;

        public MemorySegmentBasedBufferReader(MemorySegment[] segments) => _segments = segments;

        public long Length => _segments.Sum(s => s.BytesWritten);

        public void Skip(long cb) => Skip(unchecked((int)cb));

        public void Skip(int cb) {
            // todo max length check :D

            do {
                if (_segments[_segmentIndex].BytesWritten - _segmentCurrentPosition > cb) {
                    _segmentCurrentPosition += cb;
                    return;
                }

                cb -= _segments[_segmentIndex].BytesWritten - _segmentCurrentPosition;
                _segmentCurrentPosition = _segments[_segmentIndex].BytesWritten;
                NextSegment();
            } while (true);
        }

        public int Position => _finishedSegmentsIndex + _segmentCurrentPosition;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte() {
            EnsureAvailable(1);
            var retVal = _segments[_segmentIndex]. /*Memory.*/Memory[_segmentCurrentPosition++..].Span[0];
            if (_segmentCurrentPosition /*+ 1*/ >= _segments[_segmentIndex].BytesWritten) {
                NextSegment();
            }

            return retVal;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte PeekByte() {
            EnsureAvailable(1);
            return _segments[_segmentIndex]. /*Memory.*/Memory[_segmentCurrentPosition ..].Span[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ReadUInt16() {
            EnsureAvailable(2);
            var val = BinaryPrimitives.ReadUInt16LittleEndian(_segments[_segmentIndex]. /*Memory.*/Memory.Slice(_segmentCurrentPosition, 2).Span);
            _segmentCurrentPosition += 2;
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt32() {
            EnsureAvailable(4);
            var val = BinaryPrimitives.ReadUInt32LittleEndian(_segments[_segmentIndex]. /*Memory.*/Memory.Slice(_segmentCurrentPosition, 4).Span);
            _segmentCurrentPosition += 2;
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadUInt64() {
            const int len = 8;
            EnsureAvailable(len);
            var val = BinaryPrimitives.ReadUInt64LittleEndian(_segments[_segmentIndex]. /*Memory.*/Memory.Slice(_segmentCurrentPosition, len).Span);
            _segmentCurrentPosition += len;
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadInt16() {
            const int len = 2;
            EnsureAvailable(len);
            var val = BinaryPrimitives.ReadInt16LittleEndian(_segments[_segmentIndex]. /*Memory.*/Memory.Slice(_segmentCurrentPosition, len).Span);
            _segmentCurrentPosition += len;
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt32() {
            const int len = 4;
            EnsureAvailable(len);
            var val = BinaryPrimitives.ReadInt32LittleEndian(_segments[_segmentIndex]. /*Memory.*/Memory.Slice(_segmentCurrentPosition, len).Span);
            _segmentCurrentPosition += len;
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadInt64() {
            const int len = 8;
            EnsureAvailable(len);
            var val = BinaryPrimitives.ReadInt64LittleEndian(_segments[_segmentIndex]. /*Memory.*/Memory.Slice(_segmentCurrentPosition, len).Span);
            _segmentCurrentPosition += len;
            return val;
        }

        public byte[] ReadBytes(uint count) {
            if (count == 0) {
                return Array.Empty<byte>();
            }

            var bytes = new byte[count];
            // todo is there a ... difference between AsSpan and new?
            var destination = new Span<byte>(bytes);
            ReadBytes(in destination);
            return bytes;
        }

        public void ReadBytes(in Span<byte> destination) {
            //if (_segments[_segmentIndex].BytesWritten - _segmentCurrentPosition >= destination.Length) {
            //    _segments[_segmentIndex]./*Memory.*/Memory[_segmentCurrentPosition .. (_segmentCurrentPosition + destination.Length)].Span.CopyTo(destination);
            //    _segmentCurrentPosition += destination.Length;

            //    NextSegmentIfNeeded();
            //    return;
            //}

            var offset = 0;
            do {
                var cb = Math.Min(destination.Length - offset, _segments[_segmentIndex].BytesWritten - _segmentCurrentPosition);
                if (cb == 0) break;

                _segments[_segmentIndex]. /*Memory.*/Memory[_segmentCurrentPosition..(_segmentCurrentPosition + cb)].Span.CopyTo(destination[offset ..]);
                _segmentCurrentPosition += cb;
                offset += cb;

                NextSegmentIfNeeded();
            } while (offset < destination.Length);
        }

        public string ReadString() {
            var n = ReadInt32();
            if (n <= 0) {
                if (n == 0) return string.Empty;

                // a length of -1 indicates that the string is null.
                if (n == -1) return null;
            }

            if (_segments[_segmentIndex].BytesWritten - _segmentCurrentPosition >= n) {
                var s = Encoding.UTF8.GetString(_segments[_segmentIndex]. /*Memory.*/Memory[_segmentCurrentPosition .. (_segmentCurrentPosition + n)].Span);
                _segmentCurrentPosition += n;
                NextSegmentIfNeeded();
                return s;
            }
            else if (n <= 256) {
                Span<byte> bytes = stackalloc byte[n];
                ReadBytes(in bytes);
                return Encoding.UTF8.GetString(bytes);
            }
            else {
                var bytes = ReadBytes((uint)n);
                return Encoding.UTF8.GetString(bytes);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void NextSegmentIfNeeded() {
            if (_segments[_segmentIndex].BytesWritten == _segmentCurrentPosition) NextSegment();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureAvailable(int bytes) {
            // if it is a single byte the _segmentCurrentPosition is already pointing there, we only need bytes -1 additional
            if (_segments[_segmentIndex].BytesWritten > bytes + _segmentCurrentPosition - 1) return;

            // off by one, I am officially a clown :D
            if (_segments[_segmentIndex].BytesWritten - _segmentCurrentPosition > 1) throw new ApplicationException("????");

            NextSegment();

            if (_segments[_segmentIndex].BytesWritten < bytes + _segmentCurrentPosition - 1) throw new ApplicationException("!!!!!!");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void NextSegment() {
            _finishedSegmentsIndex += _segmentCurrentPosition;
            _segmentIndex++;
            _segmentCurrentPosition = 0;
        }
    }
}