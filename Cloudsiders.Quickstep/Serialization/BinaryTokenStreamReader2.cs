using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Orleans.Runtime;
using Orleans.Serialization;

// borrowed from original Orleans serialization code, unfortunately internal
// The MIT License (MIT)

// Copyright (c) .NET Foundation

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

namespace Cloudsiders.Quickstep.Serialization {
    // used when deserializing a single, contiguous buffer
    internal sealed class BinaryTokenStreamReader2 : IBinaryTokenStreamReader {
        // ReSharper disable FieldCanBeMadeReadOnly.Local
        private ReadOnlySequence<byte> _input;
        // ReSharper restore FieldCanBeMadeReadOnly.Local

        private ReadOnlyMemory<byte> _currentSpan;
        private SequencePosition _nextSequencePosition;
        private int _bufferPos;
        private int _bufferSize;
        private long _previousBuffersSize;

        public BinaryTokenStreamReader2() { }

        public BinaryTokenStreamReader2(ReadOnlySequence<byte> input) {
            PartialReset(input);
        }

        public long Length => _input.Length;

        public long Position {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _previousBuffersSize + _bufferPos;
        }

        public int CurrentPosition {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)_previousBuffersSize + _bufferPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PartialReset(ReadOnlySequence<byte> input) {
            _input = input;
            _nextSequencePosition = input.Start;
            _currentSpan = input.First;
            _bufferPos = 0;
            _bufferSize = _currentSpan.Length;
            _previousBuffersSize = 0;
        }

        public void Skip(long count) {
            var end = Position + count;
            while (Position < end) {
                if (Position + _bufferSize >= end) {
                    _bufferPos = (int)(end - _previousBuffersSize);
                }
                else {
                    MoveNext();
                }
            }
        }

        /// <summary>
        /// Creates a new reader beginning at the specified position.
        /// </summary>
        public BinaryTokenStreamReader2 ForkFrom(long position) {
            var result = new BinaryTokenStreamReader2();
            var sliced = _input.Slice(position);
            result.PartialReset(sliced);
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MoveNext() {
            _previousBuffersSize += _bufferSize;

            // If this is the first call to MoveNext then nextSequencePosition is invalid and must be moved to the second position.
            if (_nextSequencePosition.Equals(_input.Start)) _input.TryGet(ref _nextSequencePosition, out _);

            if (!_input.TryGet(ref _nextSequencePosition, out var memory)) {
                _currentSpan = memory;
                ThrowInsufficientData();
            }

            _currentSpan = memory;
            _bufferPos = 0;
            _bufferSize = _currentSpan.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte() {
            if (_bufferPos == _bufferSize) MoveNext();
            return _currentSpan.Span[_bufferPos++];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte PeekByte() {
            if (_bufferPos == _bufferSize) MoveNext();
            return _currentSpan.Span[_bufferPos];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadInt16() => unchecked((short)ReadUInt16());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ReadUInt16() {
            const int width = 2;
            if (_bufferPos + width > _bufferSize) return ReadSlower();

            var result = BinaryPrimitives.ReadUInt16LittleEndian(_currentSpan.Span.Slice(_bufferPos, width));
            _bufferPos += width;
            return result;

            ushort ReadSlower() {
                ushort b1 = ReadByte();
                ushort b2 = ReadByte();

                return (ushort)(b1 | (b2 << 8));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt32() => (int)ReadUInt32();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt32() {
            const int width = 4;
            if (_bufferPos + width > _bufferSize) return ReadSlower();

            var result = BinaryPrimitives.ReadUInt32LittleEndian(_currentSpan.Span.Slice(_bufferPos, width));
            _bufferPos += width;
            return result;

            uint ReadSlower() {
                uint b1 = ReadByte();
                uint b2 = ReadByte();
                uint b3 = ReadByte();
                uint b4 = ReadByte();

                return b1 | (b2 << 8) | (b3 << 16) | (b4 << 24);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadInt64() => (long)ReadUInt64();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadUInt64() {
            const int width = 8;
            if (_bufferPos + width > _bufferSize) return ReadSlower();

            var result = BinaryPrimitives.ReadUInt64LittleEndian(_currentSpan.Slice(_bufferPos, width).Span);
            _bufferPos += width;
            return result;

            ulong ReadSlower() {
                ulong b1 = ReadByte();
                ulong b2 = ReadByte();
                ulong b3 = ReadByte();
                ulong b4 = ReadByte();
                ulong b5 = ReadByte();
                ulong b6 = ReadByte();
                ulong b7 = ReadByte();
                ulong b8 = ReadByte();

                return b1 | (b2 << 8) | (b3 << 16) | (b4 << 24)
                       | (b5 << 32) | (b6 << 40) | (b7 << 48) | (b8 << 56);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowInsufficientData() => throw new InvalidOperationException("Insufficient data present in buffer.");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float ReadFloat() => BitConverter.Int32BitsToSingle(ReadInt32());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double ReadDouble() => BitConverter.Int64BitsToDouble(ReadInt64());

        public decimal ReadDecimal() {
            var parts = new[] { ReadInt32(), ReadInt32(), ReadInt32(), ReadInt32() };
            return new decimal(parts);
        }

        public byte[] ReadBytes(uint count) {
            if (count == 0) return Array.Empty<byte>();

            var bytes = new byte[count];
            var destination = new Span<byte>(bytes);
            ReadBytes(in destination);
            return bytes;
        }

        public void ReadBytes(in Span<byte> destination) {
            if (_bufferPos + destination.Length <= _bufferSize) {
                _currentSpan.Slice(_bufferPos, destination.Length).Span.CopyTo(destination);
                _bufferPos += destination.Length;
                return;
            }

            CopySlower(in destination);

            void CopySlower(in Span<byte> d) {
                var dest = d;
                while (true) {
                    var writeSize = Math.Min(dest.Length, _currentSpan.Length - _bufferPos);
                    _currentSpan.Slice(_bufferPos, writeSize).Span.CopyTo(dest);
                    _bufferPos += writeSize;
                    dest = dest.Slice(writeSize);

                    if (dest.Length == 0) break;

                    MoveNext();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadBytes(int length, out ReadOnlySpan<byte> bytes) {
            if (_bufferPos + length <= _bufferSize) {
                bytes = _currentSpan.Slice(_bufferPos, length).Span;
                _bufferPos += length;
                return true;
            }

            bytes = default;
            return false;
        }

        /// <summary> Read a <c>bool</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public bool ReadBoolean() => ReadToken() == SerializationTokenType.True;

        public DateTime ReadDateTime() {
            var n = ReadInt64();
            return n == 0 ? default(DateTime) : DateTime.FromBinary(n);
        }

        /// <summary> Read an <c>string</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public string ReadString() {
            var n = ReadInt32();
            if (n <= 0) {
                if (n == 0) return string.Empty;

                // a length of -1 indicates that the string is null.
                if (n == -1) return null;
            }

            if (_bufferSize - _bufferPos >= n) {
                var s = Encoding.UTF8.GetString(_currentSpan.Slice(_bufferPos, n).Span);
                _bufferPos += n;
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

        /// <summary> Read the next bytes from the stream. </summary>
        /// <param name="destination">Output array to store the returned data in.</param>
        /// <param name="offset">Offset into the destination array to write to.</param>
        /// <param name="count">Number of bytes to read.</param>
        public void ReadByteArray(byte[] destination, int offset, int count) {
            if (offset + count > destination.Length) throw new ArgumentOutOfRangeException("count", "Reading into an array that is too small");

            if (count > 0) {
                var destSpan = new Span<byte>(destination, offset, count);
                ReadBytes(in destSpan);
            }
        }

        /// <summary> Read an <c>char</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public char ReadChar() => Convert.ToChar(ReadInt16());

        /// <summary> Read an <c>sbyte</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public sbyte ReadSByte() => unchecked((sbyte)ReadByte());

        /// <summary> Read an <c>IPAddress</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public IPAddress ReadIPAddress() {
            Span<byte> buff = stackalloc byte[16];
            ReadBytes(buff);
            var v4 = true;
            for (var i = 0; i < 12; i++) {
                if (buff[i] != 0) {
                    v4 = false;
                    break;
                }
            }

            if (v4) {
                return new IPAddress(buff.Slice(12));
            }
            else {
                return new IPAddress(buff);
            }
        }

        public Guid ReadGuid() {
            Span<byte> bytes = stackalloc byte[16];
            ReadBytes(in bytes);
            return new Guid(bytes);
        }

        /// <summary> Read an <c>IPEndPoint</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public IPEndPoint ReadIPEndPoint() {
            var addr = ReadIPAddress();
            var port = ReadInt32();
            return new IPEndPoint(addr, port);
        }

        /// <summary> Read an <c>SiloAddress</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public SiloAddress ReadSiloAddress() {
            var ep = ReadIPEndPoint();
            var gen = ReadInt32();
            return SiloAddress.New(ep, gen);
        }

        public TimeSpan ReadTimeSpan() => new TimeSpan(ReadInt64());

        /// <summary>
        /// Read a block of data into the specified output <c>Array</c>.
        /// </summary>
        /// <param name="array">Array to output the data to.</param>
        /// <param name="n">Number of bytes to read.</param>
        public void ReadBlockInto(Array array, int n) {
            Buffer.BlockCopy(ReadBytes((uint)n), 0, array, 0, n);
        }

        /// <summary> Read a <c>SerializationTokenType</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        internal SerializationTokenType ReadToken() => (SerializationTokenType)ReadByte();

        public IBinaryTokenStreamReader Copy() {
            var result = new BinaryTokenStreamReader2();
            result.PartialReset(_input);
            return result;
        }

        public int ReadInt() => ReadInt32();

        public uint ReadUInt() => ReadUInt32();

        public short ReadShort() => ReadInt16();

        public ushort ReadUShort() => ReadUInt16();

        public long ReadLong() => ReadInt64();

        public ulong ReadULong() => ReadUInt64();

        public byte[] ReadBytes(int count) => ReadBytes((uint)count);
    }
}