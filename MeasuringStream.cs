using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Buffers;
using System.Threading;
using System.Diagnostics;

namespace PakPatcher
{
    class MeasuringStream : Stream
    {
        public struct BwStat
        {
            public Int64 count;
            public Int64 time;

            public TimeSpan TS => new TimeSpan(time);
            public double Speed => TS.TotalSeconds != 0 ? (count / TS.TotalSeconds) : double.PositiveInfinity;
            //public double Speed => count / TS.TotalSeconds;
        }

        BwStat m_readStat = new BwStat();
        BwStat m_writeStat = new BwStat();

        public BwStat StatRead => m_readStat;
        public BwStat StatWrite => m_writeStat;

        public string Name { get; private set; }
        public Stream UnderlyingStream { get; private set; }

        public MeasuringStream(Stream stream, string name)
        {
            UnderlyingStream = stream;
        }
        public MeasuringStream(FileStream stream)
        {
            UnderlyingStream = stream;
            Name = stream.Name;
        }

        public override bool CanRead => UnderlyingStream.CanRead;
        public override bool CanSeek => UnderlyingStream.CanSeek;
        public override bool CanWrite => UnderlyingStream.CanWrite;

        public override long Length => UnderlyingStream.Length;

        public override long Position { get => UnderlyingStream.Position; set => UnderlyingStream.Position = value; }

        public override bool CanTimeout => UnderlyingStream.CanTimeout;

        public override int ReadTimeout { get => UnderlyingStream.ReadTimeout; set => UnderlyingStream.ReadTimeout = value; }
        public override int WriteTimeout { get => UnderlyingStream.WriteTimeout; set => UnderlyingStream.WriteTimeout = value; }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            throw new NotImplementedException();
            //return UnderlyingStream.BeginRead(buffer, offset, count, callback, state);
        }

        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            throw new NotImplementedException();
            //return UnderlyingStream.BeginWrite(buffer, offset, count, callback, state);
        }

        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
            //return UnderlyingStream.CopyToAsync(destination, bufferSize, cancellationToken);
        }

        public override int EndRead(IAsyncResult asyncResult)
        {
            throw new NotImplementedException();
            //return UnderlyingStream.EndRead(asyncResult);
        }

        public override void EndWrite(IAsyncResult asyncResult)
        {
            throw new NotImplementedException();
            //UnderlyingStream.EndWrite(asyncResult);
        }

        public override void Flush()
        {
            UnderlyingStream.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return UnderlyingStream.FlushAsync(cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var t = Stopwatch.StartNew();
            var res = UnderlyingStream.Read(buffer, offset, count);
            StopAddRead(res, t);
            return res;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
            //return UnderlyingStream.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override int ReadByte()
        {
            var t = Stopwatch.StartNew();
            var res = UnderlyingStream.ReadByte();
            StopAddRead(1, t);
            return res;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return UnderlyingStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            UnderlyingStream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var t = Stopwatch.StartNew();
            UnderlyingStream.Write(buffer, offset, count);
            StopAddWrite(count, t);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
            //return UnderlyingStream.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override void WriteByte(byte value)
        {
            var t = Stopwatch.StartNew();
            UnderlyingStream.WriteByte(value);
            StopAddWrite(1, t);
        }

        #region Dispose impl
        protected override void Dispose(bool disposing)
        {
            if (UnderlyingStream == null) return;
            if (disposing)
            {
                UnderlyingStream.Dispose();
                UnderlyingStream = null;
            }
        }
        #endregion


        private void StopAddRead(Int64 count, Stopwatch t)
        {
            t.Stop();
            m_readStat.count += count;
            m_readStat.time += t.ElapsedTicks;
        }

        private void StopAddWrite(Int64 count, Stopwatch t)
        {
            t.Stop();
            m_writeStat.count += count;
            m_writeStat.time += t.ElapsedTicks;
        }
    }
}
