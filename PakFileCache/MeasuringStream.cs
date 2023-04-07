using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace PakFileCache
{
    public enum StreamPurpose
    { 
        Source,
        Target,
        Cache,
        CacheMeta
    }

    public class MeasuringStream : Stream
    {
        [DebuggerDisplay("BwStat({count}, {TS}, {Speed})")]
        public struct BwStat
        {
            public Int64 count;
            public Int64 time;
            public Int64 opCount;

            public TimeSpan TS => new TimeSpan(time);
            public double Speed => TS.TotalSeconds != 0 ? (count / TS.TotalSeconds) : double.PositiveInfinity;
            //public double Speed => count / TS.TotalSeconds;

            public static BwStat operator +(BwStat a, BwStat b) => new BwStat() 
            { 
                count = a.count + b.count, 
                time = a.time + b.time,
                opCount = a.opCount + b.opCount,
            };
        }

        BwStat m_readStat = new BwStat();
        BwStat m_writeStat = new BwStat();
        bool m_registeredReport = false;

        public BwStat StatRead => m_readStat;
        public BwStat StatWrite => m_writeStat;

        public string Name { get; private set; }
        public StreamPurpose Purpose { get; private set; }
        public Stream UnderlyingStream { get; private set; }

        public MeasuringStream(Stream stream, string name, StreamPurpose purpose)
        {
            UnderlyingStream = stream;
            Name = name;
            Purpose = purpose;
        }
        public MeasuringStream(FileStream stream, StreamPurpose purpose)
            : this(stream, stream.Name, purpose)
        {
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

                RegisterReport();
            }
        }
        #endregion

        void RegisterReport(bool discard = false)
        {
            if (!m_registeredReport)
            {
                m_registeredReport = true;
                if (!discard)
                {
                    StreamStatsMgr.Instance.AddStream(this);
                }
            }
        }



        private void StopAddRead(Int64 count, Stopwatch t)
        {
            t.Stop();
            m_readStat.count += count;
            m_readStat.time += t.ElapsedTicks;
            m_readStat.opCount += 1;
        }

        private void StopAddWrite(Int64 count, Stopwatch t)
        {
            t.Stop();
            m_writeStat.count += count;
            m_writeStat.time += t.ElapsedTicks;
            m_writeStat.opCount += 1;
        }
    }

    public class StreamStatsMgr
    {
        private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public static StreamStatsMgr Instance { get; } = new StreamStatsMgr();

        [DebuggerDisplay("Key({Name}, {DriveType})")]
        class DiskReportKey : IEquatable<DiskReportKey>
        {
            public string Name { get; set; }
            public DriveType DriveType { get; set; }

            #region Equality (generated)
            public override bool Equals(object obj)
            {
                return Equals(obj as DiskReportKey);
            }

            public bool Equals(DiskReportKey other)
            {
                return other != null &&
                       Name == other.Name &&
                       DriveType == other.DriveType;
            }

            public override int GetHashCode()
            {
                int hashCode = 1461668965;
                hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Name);
                hashCode = hashCode * -1521134295 + DriveType.GetHashCode();
                return hashCode;
            }
            #endregion
        }

        class StreamReportReq
        {
            public string FilePath { get; }
            public MeasuringStream.BwStat Read { get; }
            public MeasuringStream.BwStat Write { get; }
            public StreamPurpose Purpose { get; }

            public StreamReportReq(MeasuringStream stream)
            {
                FilePath = stream.Name;
                Read = stream.StatRead;
                Write = stream.StatWrite;
                Purpose = stream.Purpose;
            }
        }

        class DiskReport
        {
            public MeasuringStream.BwStat Read { get; set; } = new MeasuringStream.BwStat();
            public MeasuringStream.BwStat Write { get; set; } = new MeasuringStream.BwStat();

            public int StreamCount { get; set; } = 0;

            public void AddToReport(StreamReportReq report)
            {
                Read += report.Read;
                Write += report.Write;
                StreamCount += 1;
            }
        }

        Dictionary<DiskReportKey, DiskReport> ReportsByDisk { get; } = new Dictionary<DiskReportKey, DiskReport>();
        Dictionary<StreamPurpose, DiskReport> ReportsByPurpose { get; } = new Dictionary<StreamPurpose, DiskReport>();


        private ActionBlock<StreamReportReq> m_reportBlock;

        StreamStatsMgr()
        {
            m_reportBlock = new ActionBlock<StreamReportReq>(AddStreamImpl);
        }

        private DiskReport GetOrAddReport<D, K>(D dict, K key) where D : IDictionary<K, DiskReport>
        {
            DiskReport value;
            if (!dict.TryGetValue(key, out value))
            {
                dict[key] = value = new DiskReport();
            }
            return value;
        }

        private void AddStreamImpl(StreamReportReq req)
        {
            lock (this)
            {
                string filePath = req.FilePath;

                var rootPath = Path.GetPathRoot(filePath);
                DiskReportKey k;
                if (rootPath.StartsWith("\\\\"))
                {
                    k = new DiskReportKey() { Name = rootPath, DriveType = DriveType.Network };
                }
                else
                {
                    var di = new DriveInfo(rootPath);
                    k = new DiskReportKey() { Name = di.Name, DriveType = di.DriveType };
                }

                GetOrAddReport(ReportsByDisk, k).AddToReport(req);
                GetOrAddReport(ReportsByPurpose, req.Purpose).AddToReport(req);
            }
        }

        public void AddStream(MeasuringStream stream)
        {
            StreamReportReq report = new StreamReportReq(stream);
            m_reportBlock.Post(report);
        }

        public void LogReports()
        {
            m_reportBlock.Complete();
            m_reportBlock.Completion.Wait();
            m_reportBlock = new ActionBlock<StreamReportReq>(AddStreamImpl);

            lock (this)
            {
                logger.Info(() =>
                {
                    StringBuilder info = new StringBuilder();
                    info.AppendLine("Report by disk");

                    Action<MeasuringStream.BwStat> appendStat = (MeasuringStream.BwStat s) => { info.Append($"{GetBytesReadable(s.count)}, {s.TS}, {GetBytesReadable(s.Speed, "/s")}, {s.opCount} ops; "); };

                    foreach (var p in ReportsByDisk)
                    {
                        var r = p.Value.Read;
                        var w = p.Value.Write;
                        info.Append($"{p.Key.Name} ({p.Key.DriveType}): {p.Value.StreamCount} streams: ");
                        info.Append("r => ");
                        appendStat(r);
                        info.Append("w => ");
                        appendStat(w);
                        info.AppendLine();
                    }
                    info.AppendLine("Report by purpose");
                    foreach (var p in ReportsByPurpose)
                    {
                        var r = p.Value.Read;
                        var w = p.Value.Write;
                        info.Append($"{p.Key}: {p.Value.StreamCount} streams: ");
                        info.Append("r => ");
                        appendStat(r);
                        info.Append("w => ");
                        appendStat(w);
                        info.AppendLine();
                    }

                    return info.ToString();
                });
            }
        }

        public void Reset()
        {
            lock (this)
            {
                ReportsByDisk.Clear();
                ReportsByPurpose.Clear();
            }
        }


        // from https://stackoverflow.com/a/11124118
        private static string GetBytesReadable(long i)
        {
            // Get absolute value
            long absolute_i = (i < 0 ? -i : i);
            // Determine the suffix and readable value
            string suffix;
            double readable;
            if (absolute_i >= 0x40000000) // Gigabyte
            {
                suffix = "GB";
                readable = (i >> 20);
            }
            else if (absolute_i >= 0x100000) // Megabyte
            {
                suffix = "MB";
                readable = (i >> 10);
            }
            else if (absolute_i >= 0x400) // Kilobyte
            {
                suffix = "KB";
                readable = i;
            }
            else
            {
                return i.ToString("0 B"); // Byte
            }
            // Divide by 1024 to get fractional value
            readable = (readable / 1024);
            // Return formatted number with suffix
            return readable.ToString("0.### ", CultureInfo.InvariantCulture) + suffix;
        }

        private static string GetBytesReadable(double i, string suffixExtra)
        {
            if (double.IsInfinity(i))
                return i.ToString(CultureInfo.InvariantCulture);

            // Get absolute value
            double absolute_i = (i < 0 ? -i : i);
            // Determine the suffix and readable value
            string suffix;
            double readable;
            if (absolute_i >= 0x40000000) // Gigabyte
            {
                suffix = "GB";
                readable = i / (1 << 20);
            }
            else if (absolute_i >= 0x100000) // Megabyte
            {
                suffix = "MB";
                readable = i / (1 << 10);
            }
            else if (absolute_i >= 0x400) // Kilobyte
            {
                suffix = "KB";
                readable = i;
            }
            else
            {
                return i.ToString("0 B"); // Byte
            }
            // Divide by 1024 to get fractional value
            readable = (readable / 1024);
            // Return formatted number with suffix
            return readable.ToString("0.### ", CultureInfo.InvariantCulture) + suffix + suffixExtra;
        }
    }

}
