using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Cloudsiders.Quickstep.Serialization {
    // small Stream interface wrapper over existing Memory<byte>
    internal sealed class MemoryPoolStream : Stream {
        private readonly Memory<byte> _memory;
        private int _position = 0;

        public MemoryPoolStream(Memory<byte> memory) => _memory = memory;

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;


        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = new CancellationToken()) {
            if (buffer.Length + _position > _memory.Length) throw new InvalidOperationException();
            if (!buffer.TryCopyTo(_memory.Slice(_position))) throw new ApplicationException();
            _position += buffer.Length;
            return new ValueTask();
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = new CancellationToken()) {
            if (_position + 1 == _memory.Length) return new ValueTask<int>(-1);
            var cbAvailable = Math.Min(buffer.Length, _memory.Length - _position);
            if (!_memory.Slice(_position, cbAvailable).TryCopyTo(buffer)) throw new ApplicationException();
            return new ValueTask<int>(cbAvailable);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => throw new NotSupportedException();

        public override void WriteByte(byte value) {
            if (_position >= _memory.Length) throw new InvalidOperationException();
            _memory.Slice(_position).Span[0] = value;
            _position++;
        }

        public override void Flush() { }

        public override int Read(Span<byte> buffer) {
            var count = Math.Min(_memory.Length - _position, buffer.Length);
            if (count <= 0) return -1;

            _memory.Slice(_position, count).Span.TryCopyTo(buffer);
            _position += count;
            return count;
        }

        public override int Read(byte[] buffer, int offset, int count) {
            count = Math.Min(_memory.Length - _position, count);
            if (count <= 0) return -1;

            var cbRead = _memory.Slice(_position, count)
                                .TryCopyTo(new Memory<byte>(buffer).Slice(offset))
                ? count
                : 0;
            _position += cbRead;
            return cbRead;
        }

        public override long Seek(long offset, SeekOrigin origin) {
            var iOffset = unchecked((int)offset);
            if (iOffset < 0) throw new InvalidOperationException();
            if (iOffset > _memory.Length) throw new InvalidOperationException();
            if (origin == SeekOrigin.Begin) {
                _position = iOffset;
            }
            else if (origin == SeekOrigin.Current) {
                if (_position + iOffset > _memory.Length) throw new InvalidOperationException();
                _position += iOffset;
            }

            if (origin == SeekOrigin.End) throw new NotSupportedException();
            return _position;
        }

        public override void SetLength(long value) {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count) {
            if (_position + count > _memory.Length) throw new InvalidOperationException();
            var memSpan = _memory.Span.Slice(_position);
            buffer.AsSpan(offset, count).CopyTo(memSpan);
            _position += count;
        }

        public override void Write(ReadOnlySpan<byte> buffer) {
            if (_position + buffer.Length > _memory.Length) throw new InvalidOperationException();
            var memSpan = _memory.Span.Slice(_position);
            buffer.CopyTo(memSpan);
            _position += buffer.Length;
        }

        public override bool CanRead => _position + 1 < _memory.Length;
        public override bool CanSeek => CanRead;
        public override bool CanWrite => CanRead;

        public override long Length => _memory.Length;

        public override long Position {
            get => _position;
            set {
                var iPos = unchecked((int)value);
                if (iPos >= _memory.Length || iPos < 0) throw new InvalidOperationException();
                _position = iPos;
            }
        }

        public int BytesWritten => unchecked((int)_position);
    }
}