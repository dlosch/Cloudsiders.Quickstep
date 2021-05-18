using System;
using System.Net;
using System.Runtime.CompilerServices;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Cloudsiders.Quickstep.Serialization {
    // borrowed from original Orleans serialization code, unfortunately internal
    internal sealed class BinaryTokenStreamReader3 : IBinaryTokenStreamReader {
        private readonly MemorySegmentBasedBufferReader _reader;

        public BinaryTokenStreamReader3(MemorySegmentBasedBufferReader reader) => _reader = reader;

        public long Length => _reader.Length;

        public long Position {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _reader.Position;
        }

        public int CurrentPosition {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _reader.Position;
        }

        public void Skip(long count) => _reader.Skip(count);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte() => _reader.ReadByte();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte PeekByte() => _reader.PeekByte();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadInt16() => unchecked((short)_reader.ReadUInt16());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ReadUInt16() => _reader.ReadUInt16();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt32() => (int)_reader.ReadUInt32();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt32() => _reader.ReadUInt32();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadInt64() => (long)_reader.ReadUInt64();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadUInt64() => _reader.ReadUInt64();

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowInsufficientData() => throw new InvalidOperationException("Insufficient data present in buffer.");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float ReadFloat() => BitConverter.Int32BitsToSingle(_reader.ReadInt32());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double ReadDouble() => BitConverter.Int64BitsToDouble(_reader.ReadInt64());

        public decimal ReadDecimal() {
            var parts = new[] { _reader.ReadInt32(), _reader.ReadInt32(), _reader.ReadInt32(), _reader.ReadInt32() };
            return new decimal(parts);
        }

        public byte[] ReadBytes(uint count) => _reader.ReadBytes(count);

        public void ReadBytes(in Span<byte> destination) => _reader.ReadBytes(destination);

        /// <summary> Read a <c>bool</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public bool ReadBoolean() => ReadToken() == SerializationTokenType.True;

        public DateTime ReadDateTime() {
            var n = _reader.ReadInt64();
            return n == 0 ? default(DateTime) : DateTime.FromBinary(n);
        }

        /// <summary> Read an <c>string</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public string ReadString() => _reader.ReadString();

        /// <summary> Read the next bytes from the stream. </summary>
        /// <param name="destination">Output array to store the returned data in.</param>
        /// <param name="offset">Offset into the destination array to write to.</param>
        /// <param name="count">Number of bytes to read.</param>
        public void ReadByteArray(byte[] destination, int offset, int count) {
            if (offset + count > destination.Length) {
                throw new ArgumentOutOfRangeException("count", "Reading into an array that is too small");
            }

            if (count > 0) {
                var destSpan = new Span<byte>(destination, offset, count);
                _reader.ReadBytes(in destSpan);
            }
        }

        /// <summary> Read an <c>char</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public char ReadChar() => Convert.ToChar(_reader.ReadInt16());

        /// <summary> Read an <c>sbyte</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public sbyte ReadSByte() => unchecked((sbyte)_reader.ReadByte());

        /// <summary> Read an <c>IPAddress</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public IPAddress ReadIPAddress() {
            Span<byte> buff = stackalloc byte[16];
            _reader.ReadBytes(buff);
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
            _reader.ReadBytes(in bytes);
            return new Guid(bytes);
        }

        /// <summary> Read an <c>IPEndPoint</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public IPEndPoint ReadIPEndPoint() {
            var addr = ReadIPAddress();
            var port = _reader.ReadInt32();
            return new IPEndPoint(addr, port);
        }

        /// <summary> Read an <c>SiloAddress</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public SiloAddress ReadSiloAddress() {
            var ep = ReadIPEndPoint();
            var gen = _reader.ReadInt32();
            return SiloAddress.New(ep, gen);
        }

        public TimeSpan ReadTimeSpan() => new TimeSpan(_reader.ReadInt64());

        /// <summary>
        /// Read a block of data into the specified output <c>Array</c>.
        /// </summary>
        /// <param name="array">Array to output the data to.</param>
        /// <param name="n">Number of bytes to read.</param>
        public void ReadBlockInto(Array array, int n) {
            Buffer.BlockCopy(_reader.ReadBytes((uint)n), 0, array, 0, n);
        }

        /// <summary> Read a <c>SerializationTokenType</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        internal SerializationTokenType ReadToken() => (SerializationTokenType)_reader.ReadByte();

        public IBinaryTokenStreamReader Copy() => throw new NotImplementedException("IBinaryTokenStreamReader COPY");

        //var result = new BinaryTokenStreamReader3();
        //result.PartialReset(this.input);
        //return result;
        public int ReadInt() => _reader.ReadInt32();

        public uint ReadUInt() => _reader.ReadUInt32();

        public short ReadShort() => _reader.ReadInt16();

        public ushort ReadUShort() => _reader.ReadUInt16();

        public long ReadLong() => _reader.ReadInt64();

        public ulong ReadULong() => _reader.ReadUInt64();

        public byte[] ReadBytes(int count) => _reader.ReadBytes((uint)count);
    }
}