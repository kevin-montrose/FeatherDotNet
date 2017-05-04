using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    class BufferedStream : Stream
    {
        public override bool CanRead => false;

        public override bool CanSeek => true;

        public override bool CanWrite => true;

        public override long Length
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override long Position { get; set; }

        MultiStreamProvider Outer;

        public BufferedStream(MultiStreamProvider outer, long startingPosition)
        {
            Outer = outer;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPosition;

            switch (origin)
            {
                case SeekOrigin.Begin: newPosition = offset; break;
                case SeekOrigin.Current: newPosition = Position + offset; break;
                case SeekOrigin.End: throw new NotImplementedException();
                default: throw new Exception("Unexpected SeekOrigin: " + origin);
            }

            if (newPosition < Position) throw new InvalidOperationException($"Cannot seek backwards in BufferedStream");

            Position = newPosition;

            return newPosition;
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Outer.Push(Position, buffer, offset, count);
            Position += count;
        }

        protected override void Dispose(bool disposing)
        {
            Outer.RemoveChild(this);

            base.Dispose(disposing);
        }

        public override void Flush()
        {
            Outer.RequestFlush(this);
        }
    }

    class MultiStreamProvider
    {
        const int BUFFER_COUNT = 8;
        const int INITIAL_BUFFER_SIZE = 32;

        struct PendingEntry : IEquatable<PendingEntry>
        {
            public long WriteAtPosition { get; private set; }
            public byte[] Data { get; private set; }
            public int Count { get; private set; }

            public PendingEntry(long writeAt, byte[] data, int count)
            {
                WriteAtPosition = writeAt;
                Data = data;
                Count = count;
            }

            public bool Equals(PendingEntry other)
            {
                return
                    WriteAtPosition == this.WriteAtPosition &&
                    Data == this.Data &&
                    Count == this.Count;
            }

            public override bool Equals(object obj)
            {
                if (!(obj is PendingEntry)) return false;

                return Equals((PendingEntry)obj);
            }

            public override int GetHashCode()
            {
                var ret = 0;
                ret += WriteAtPosition.GetHashCode();
                ret *= 17;
                ret += Data?.GetHashCode() ?? 0;
                ret *= 17;
                ret += Count.GetHashCode();

                return ret;
            }
        }

        byte[][] Buffers;

        Stream TrueStream;
        List<PendingEntry> Pending;
        List<BufferedStream> Children;
        List<bool> WantsFlush;
        public MultiStreamProvider(Stream trueStream)
        {
            TrueStream = trueStream;
            Pending = new List<PendingEntry>();
            Children = new List<BufferedStream>();
            WantsFlush = new List<bool>();
            Buffers = Enumerable.Range(0, BUFFER_COUNT).Select(b => new byte[INITIAL_BUFFER_SIZE]).ToArray();
        }

        public BufferedStream CreateChildStream()
        {
            var nearestPoint = Children.Count == 0 ? 0 : Children.Min(c => c.Position);

            var ret = new BufferedStream(this, nearestPoint);
            Children.Add(ret);
            WantsFlush.Add(false);

            return ret;
        }

        public void Push(long position, byte[] data, int offset, int count)
        {
            var inBuffer = PlaceInBuffer(data, offset, count);
            Array.Copy(data, offset, inBuffer, 0, count);

            Pending.Add(new PendingEntry(position, inBuffer, count));
        }

        public void RemoveChild(BufferedStream child)
        {
            if (!Children.Remove(child))
            {
                throw new InvalidOperationException("Removed same child twice, probably a double disposal");
            }

            if (Children.Count == 0)
            {
                // clear everything in the queue, now that we've torn it down
                WriteToStream();
            }
        }

        public void RequestFlush(BufferedStream child)
        {
            var ix = Children.IndexOf(child);
            WantsFlush[ix] = true;

            if (WantsFlush.All(_ => _))
            {
                WriteToStream();
                for (var i = 0; i < Children.Count; i++)
                {
                    WantsFlush[i] = false;
                }
            }
        }

        void AdvanceTo(long toPosition)
        {
            if (TrueStream.Position < toPosition)
            {
                if (TrueStream.CanSeek)
                {
                    TrueStream.Seek(toPosition, SeekOrigin.Begin);
                }
                else
                {
                    if (TrueStream.CanRead)
                    {
                        while (TrueStream.Position < toPosition)
                        {
                            TrueStream.ReadByte();
                        }
                    }
                    else
                    {
                        if (TrueStream.CanWrite)
                        {
                            while (TrueStream.Position < toPosition)
                            {
                                TrueStream.WriteByte(0);
                            }
                        }
                    }
                }
            }
        }

        void WriteToStream()
        {
            foreach (var write in Pending.OrderBy(p => p.WriteAtPosition))
            {
                var atPosition = write.WriteAtPosition;
                AdvanceTo(atPosition);

                TrueStream.Write(write.Data, 0, write.Count);
            }

            TrueStream.Flush();

            Pending.Clear();
        }

        byte[] PlaceInBuffer(byte[] data, int offset, int count)
        {
            byte[] placeIn;

            for(var i = 0; i < Buffers.Length; i++)
            {
                var candidateBuffer = Buffers[i];
                if(candidateBuffer != null && candidateBuffer.Length >= count)
                {
                    var exRes = Interlocked.CompareExchange(ref Buffers[i], null, candidateBuffer);
                    if(object.ReferenceEquals(candidateBuffer, exRes))
                    {
                        placeIn = exRes;
                        goto copy;
                    }
                }
            }

            placeIn = new byte[Math.Max(count, INITIAL_BUFFER_SIZE)];

            copy:
            Array.Copy(data, offset, placeIn, 0, count);

            return placeIn;
        }

        void ReturnBuffer(byte[] buffer)
        {
            for(var i = 0; i < Buffers.Length; i++)
            {
                var inBuffers = Buffers[i];
                if (inBuffers == null || inBuffers.Length < buffer.Length)
                {
                    var exRes = Interlocked.CompareExchange(ref Buffers[i], buffer, inBuffers);
                    if(object.ReferenceEquals(exRes, inBuffers))
                    {
                        if(exRes != null)
                        {
                            // try saving this buffer
                            ReturnBuffer(exRes);
                        }

                        return;
                    }
                }
            }
        }
    }
}