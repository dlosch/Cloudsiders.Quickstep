using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Orleans.CodeGeneration;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Cloudsiders.Quickstep.Serialization {
    /// <summary>
    /// Writer for Orleans binary token streams ... borrowed from ... you guessed it :) Why is this stuff all internal?
    /// </summary>
    internal sealed class BinaryTokenStreamWriter3 : IBinaryTokenStreamWriter //where TBufferWriter : Stream
    {
        private static readonly Dictionary<Type, SerializationTokenType> typeTokens;
        private static readonly Dictionary<Type, Action<BinaryTokenStreamWriter3, object>> writers;
        private static readonly Encoding Utf8Encoding = new UTF8Encoding(false, false);

        private readonly Encoder _utf8Encoder = Utf8Encoding.GetEncoder();
        private readonly MemorySegmentBasedBuffer _output;

        static BinaryTokenStreamWriter3() {
            typeTokens = new Dictionary<Type, SerializationTokenType>();
            typeTokens[typeof(bool)] = SerializationTokenType.Boolean;
            typeTokens[typeof(int)] = SerializationTokenType.Int;
            typeTokens[typeof(uint)] = SerializationTokenType.Uint;
            typeTokens[typeof(short)] = SerializationTokenType.Short;
            typeTokens[typeof(ushort)] = SerializationTokenType.Ushort;
            typeTokens[typeof(long)] = SerializationTokenType.Long;
            typeTokens[typeof(ulong)] = SerializationTokenType.Ulong;
            typeTokens[typeof(byte)] = SerializationTokenType.Byte;
            typeTokens[typeof(sbyte)] = SerializationTokenType.Sbyte;
            typeTokens[typeof(float)] = SerializationTokenType.Float;
            typeTokens[typeof(double)] = SerializationTokenType.Double;
            typeTokens[typeof(decimal)] = SerializationTokenType.Decimal;
            typeTokens[typeof(string)] = SerializationTokenType.String;
            typeTokens[typeof(char)] = SerializationTokenType.Character;
            typeTokens[typeof(Guid)] = SerializationTokenType.Guid;
            typeTokens[typeof(DateTime)] = SerializationTokenType.Date;
            typeTokens[typeof(TimeSpan)] = SerializationTokenType.TimeSpan;
            //typeTokens[typeof(GrainId)] = SerializationTokenType.GrainId;
            //typeTokens[typeof(ActivationId)] = SerializationTokenType.ActivationId;
            typeTokens[typeof(SiloAddress)] = SerializationTokenType.SiloAddress;
            //typeTokens[typeof(ActivationAddress)] = SerializationTokenType.ActivationAddress;
            typeTokens[typeof(IPAddress)] = SerializationTokenType.IpAddress;
            typeTokens[typeof(IPEndPoint)] = SerializationTokenType.IpEndPoint;
            //typeTokens[typeof(CorrelationId)] = SerializationTokenType.CorrelationId;
            typeTokens[typeof(InvokeMethodRequest)] = SerializationTokenType.Request;
            //typeTokens[typeof(Response)] = SerializationTokenType.Response;
            typeTokens[typeof(Dictionary<string, object>)] = SerializationTokenType.StringObjDict;
            typeTokens[typeof(object)] = SerializationTokenType.Object;
            typeTokens[typeof(List<>)] = SerializationTokenType.List;
            typeTokens[typeof(SortedList<,>)] = SerializationTokenType.SortedList;
            typeTokens[typeof(Dictionary<,>)] = SerializationTokenType.Dictionary;
            typeTokens[typeof(HashSet<>)] = SerializationTokenType.Set;
            typeTokens[typeof(SortedSet<>)] = SerializationTokenType.SortedSet;
            typeTokens[typeof(KeyValuePair<,>)] = SerializationTokenType.KeyValuePair;
            typeTokens[typeof(LinkedList<>)] = SerializationTokenType.LinkedList;
            typeTokens[typeof(Stack<>)] = SerializationTokenType.Stack;
            typeTokens[typeof(Queue<>)] = SerializationTokenType.Queue;
            typeTokens[typeof(Tuple<>)] = SerializationTokenType.Tuple + 1;
            typeTokens[typeof(Tuple<,>)] = SerializationTokenType.Tuple + 2;
            typeTokens[typeof(Tuple<,,>)] = SerializationTokenType.Tuple + 3;
            typeTokens[typeof(Tuple<,,,>)] = SerializationTokenType.Tuple + 4;
            typeTokens[typeof(Tuple<,,,,>)] = SerializationTokenType.Tuple + 5;
            typeTokens[typeof(Tuple<,,,,,>)] = SerializationTokenType.Tuple + 6;
            typeTokens[typeof(Tuple<,,,,,,>)] = SerializationTokenType.Tuple + 7;

            writers = new Dictionary<Type, Action<BinaryTokenStreamWriter3, object>>();
            writers[typeof(bool)] = (stream, obj) => stream.Write((bool)obj);
            writers[typeof(int)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Int);
                stream.Write((int)obj);
            };
            writers[typeof(uint)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Uint);
                stream.Write((uint)obj);
            };
            writers[typeof(short)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Short);
                stream.Write((short)obj);
            };
            writers[typeof(ushort)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Ushort);
                stream.Write((ushort)obj);
            };
            writers[typeof(long)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Long);
                stream.Write((long)obj);
            };
            writers[typeof(ulong)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Ulong);
                stream.Write((ulong)obj);
            };
            writers[typeof(byte)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Byte);
                stream.Write((byte)obj);
            };
            writers[typeof(sbyte)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Sbyte);
                stream.Write((sbyte)obj);
            };
            writers[typeof(float)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Float);
                stream.Write((float)obj);
            };
            writers[typeof(double)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Double);
                stream.Write((double)obj);
            };
            writers[typeof(decimal)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Decimal);
                stream.Write((decimal)obj);
            };
            writers[typeof(string)] = (stream, obj) => {
                stream.Write(SerializationTokenType.String);
                stream.Write((string)obj);
            };
            writers[typeof(char)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Character);
                stream.Write((char)obj);
            };
            writers[typeof(Guid)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Guid);
                stream.Write((Guid)obj);
            };
            writers[typeof(DateTime)] = (stream, obj) => {
                stream.Write(SerializationTokenType.Date);
                stream.Write((DateTime)obj);
            };
            writers[typeof(TimeSpan)] = (stream, obj) => {
                stream.Write(SerializationTokenType.TimeSpan);
                stream.Write((TimeSpan)obj);
            };
            //writers[typeof(GrainId)] = (stream, obj) => { stream.Write(SerializationTokenType.GrainId); stream.Write((GrainId)obj); };
            //writers[typeof(ActivationId)] = (stream, obj) => { stream.Write(SerializationTokenType.ActivationId); stream.Write((ActivationId)obj); };
            writers[typeof(SiloAddress)] = (stream, obj) => {
                stream.Write(SerializationTokenType.SiloAddress);
                stream.Write((SiloAddress)obj);
            };
            //writers[typeof(ActivationAddress)] = (stream, obj) => { stream.Write(SerializationTokenType.ActivationAddress); stream.Write((ActivationAddress)obj); };
            writers[typeof(IPAddress)] = (stream, obj) => {
                stream.Write(SerializationTokenType.IpAddress);
                stream.Write((IPAddress)obj);
            };
            writers[typeof(IPEndPoint)] = (stream, obj) => {
                stream.Write(SerializationTokenType.IpEndPoint);
                stream.Write((IPEndPoint)obj);
            };
            //writers[typeof(CorrelationId)] = (stream, obj) => { stream.Write(SerializationTokenType.CorrelationId); stream.Write((CorrelationId)obj); };
        }

        public BinaryTokenStreamWriter3(MemorySegmentBasedBuffer buffer) => _output = buffer;

        /// <summary> Current write position in the stream. </summary>
        public int CurrentOffset => Length;

        public void Write(decimal d) => Write(decimal.GetBits(d));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(string s) {
            if (s is null) {
                Write(-1);
            }
            else {
                var enc = _utf8Encoder;
                enc.Reset();

                var writableSpan = _output.WritableSpan;
                var completed = false;
                var bytesUsed = 0;
                enc.Convert(s, writableSpan.Slice(4), true, out _, out bytesUsed, out completed);

                if (!completed) {
                    var bytes = Encoding.UTF8.GetByteCount(s);
                    _output.EnsureContiguous(4 + bytes);
                    writableSpan = _output.WritableSpan;
                    enc.Convert(s, writableSpan.Slice(4), true, out _, out bytesUsed, out completed);
                }

                if (completed) {
                    BinaryPrimitives.WriteInt32LittleEndian(writableSpan, bytesUsed);
                    _output.Advance(4 + bytesUsed);
                    return;
                }
            }
        }

        public void Write(char c) {
            Write(Convert.ToInt16(c));
        }

        public void Write(bool b) {
            Write((byte)(b ? SerializationTokenType.True : SerializationTokenType.False));
        }

        public void WriteNull() {
            Write((byte)SerializationTokenType.Null);
        }

        public void WriteTypeHeader(Type t, Type expected = null) {
            if (t == expected) {
                Write((byte)SerializationTokenType.ExpectedType);
                return;
            }

            Write((byte)SerializationTokenType.SpecifiedType);

            if (t.IsArray) {
                Write((byte)(SerializationTokenType.Array + (byte)t.GetArrayRank()));
                WriteTypeHeader(t.GetElementType());
                return;
            }

            SerializationTokenType token;
            if (typeTokens.TryGetValue(t, out token)) {
                Write((byte)token);
                return;
            }

            if (t.GetTypeInfo().IsGenericType) {
                if (typeTokens.TryGetValue(t.GetGenericTypeDefinition(), out token)) {
                    Write((byte)token);
                    foreach (var tp in t.GetGenericArguments()) {
                        WriteTypeHeader(tp);
                    }

                    return;
                }
            }

            Write((byte)SerializationTokenType.NamedType);
            var typeKey = t.OrleansTypeKey();
            Write(typeKey.Length);
            Write(typeKey);
        }

        public void Write(byte[] b, int offset, int count) {
            if (count <= 0) {
                return;
            }

            if (offset == 0 && count == b.Length) {
                Write(b);
            }
            else {
                var temp = new byte[count];
                Buffer.BlockCopy(b, offset, temp, 0, count);
                Write(temp);
            }
        }

        public void Write(IPEndPoint ep) {
            Write(ep.Address);
            Write(ep.Port);
        }

        public void Write(IPAddress ip) {
            if (ip.AddressFamily == AddressFamily.InterNetwork) {
                for (var i = 0; i < 12; i++) {
                    Write((byte)0);
                }

                Write(ip.GetAddressBytes()); // IPv4 -- 4 bytes
            }
            else {
                Write(ip.GetAddressBytes()); // IPv6 -- 16 bytes
            }
        }

        public void Write(SiloAddress addr) {
            Write(addr.Endpoint);
            Write(addr.Generation);
        }

        public void Write(TimeSpan ts) {
            Write(ts.Ticks);
        }

        public void Write(DateTime dt) {
            Write(dt.ToBinary());
        }

        public void Write(Guid id) {
            Write(id.ToByteArray());
        }

        /// <summary>
        /// Try to write a simple type (non-array) value to the stream.
        /// </summary>
        /// <param name="obj">Input object to be written to the output stream.</param>
        /// <returns>Returns <c>true</c> if the value was successfully written to the output stream.</returns>
        public bool TryWriteSimpleObject(object obj) {
            if (obj == null) {
                WriteNull();
                return true;
            }

            Action<BinaryTokenStreamWriter3, object> writer;
            if (writers.TryGetValue(obj.GetType(), out writer)) {
                writer(this, obj);
                return true;
            }

            return false;
        }

        public int Length => unchecked((int)_output.Length); //currentOffset + completedLength;

        public void Write(byte[] array) {
            _output.Write(array);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ReadOnlySpan<byte> value) {
            _output.Write(value);
        }

        private void WriteMultiSegment(in ReadOnlySpan<byte> source) {
            _output.Write(source);
        }

        public void Write(List<ArraySegment<byte>> b) {
            foreach (var segment in b) {
                Write(segment);
            }
        }

        public void Write(short[] array) {
            Write(MemoryMarshal.Cast<short, byte>(array));
        }

        public void Write(int[] array) {
            Write(MemoryMarshal.Cast<int, byte>(array));
        }

        public void Write(long[] array) {
            Write(MemoryMarshal.Cast<long, byte>(array));
        }

        public void Write(ushort[] array) {
            Write(MemoryMarshal.Cast<ushort, byte>(array));
        }

        public void Write(uint[] array) {
            Write(MemoryMarshal.Cast<uint, byte>(array));
        }

        public void Write(ulong[] array) {
            Write(MemoryMarshal.Cast<ulong, byte>(array));
        }

        public void Write(sbyte[] array) {
            Write(MemoryMarshal.Cast<sbyte, byte>(array));
        }

        public void Write(char[] array) {
            Write(MemoryMarshal.Cast<char, byte>(array));
        }

        public void Write(bool[] array) {
            Write(MemoryMarshal.Cast<bool, byte>(array));
        }

        public void Write(float[] array) {
            Write(MemoryMarshal.Cast<float, byte>(array));
        }

        public void Write(double[] array) {
            Write(MemoryMarshal.Cast<double, byte>(array));
        }

        public void Write(SerializationTokenType b) => Write((byte)b);

        public void Write(byte b) {
            _output.Write(b);
        }

        public void Write(sbyte b) => _output.Write(b);

        public void Write(float i) => _output.Write(i);

        public void Write(double i) => _output.Write(i);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(short value) => _output.Write(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(int value) => _output.Write(value);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(long value) => _output.Write(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(uint value) => _output.Write(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ushort value) => _output.Write(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ulong value) => _output.Write(value);
    }
}