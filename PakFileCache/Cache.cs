using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Text.Json;

namespace PakFileCache
{
    public class FileStats
    {
        public DateTime MTime { get; set; }
        public Int64 Size { get; set; }
    }

    public class CacheId
    { 
        public byte[] Id { get; private set; }

        private string m_str = null;
        public string Str
        {
            get
            {
                if (m_str == null)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (byte b in Id)
                        sb.Append(b.ToString("x2"));
                    m_str = sb.ToString();
                }
                return m_str;
            }
        }

        public CacheId(byte[] id)
        {
            Id = id;
        }

        public override string ToString() 
        {
            return $"CacheId({Str})";
        }
    }

    static class FileCacheUtil
    {

        public static CacheId MakeFileId(Stream f, string filepath, out FileStats stats)
        {
            stats = new FileStats()
            {
                MTime = File.GetLastWriteTime(filepath),
                Size = f.Length
            };

            using (var h = IncrementalHash.CreateHash(HashAlgorithmName.SHA1))
            {
                h.AppendData(BitConverter.GetBytes(stats.Size));

                f.Seek(0, SeekOrigin.Begin);                    

                long bufferSize = 4096;
                byte[] buffer = new byte[bufferSize];
                int read;
                long n = stats.Size;
                while (n > 0 &&
                       (read = f.Read(buffer, 0, (int)Math.Min(bufferSize, n))) > 0)
                {
                    h.AppendData(buffer);
                    n -= read;
                }

                return new CacheId(h.GetHashAndReset());
            }
        }
    }


    public class CacheObject
    {
        private class Meta
        {
            public string name { get; set; }
            public Int64 size { get; set; }
            public DateTime mtime { get; set; }
        }


        public CacheId Id { get; private set; }
        public string PathRoot { get; private set; }

        private string PathObj => Path.Combine(PathRoot, "obj");
        private string PathMeta => Path.Combine(PathRoot, "meta");

        public CacheObject(CacheId id, string root)
        {
            Id = id;
            string sid = id.Str;
            PathRoot = Path.Combine(root, string.Concat(sid.Take(2)), string.Concat(sid.Skip(2)));
        }

        public bool IsPathValid()
        {
            return Directory.Exists(PathRoot)
                && File.Exists(PathObj)
                && File.Exists(PathMeta);
        }

        public void UpdateMtime(FileStats stats)
        {
            Meta meta = LoadMeta();
            if (meta.mtime != stats.MTime)
            {
                meta.mtime = stats.MTime;
                SaveMeta(meta);
            }
        }

        public void PlaceStreamFile(Stream f, string filepath, FileStats stats)
        {
            PlaceStream(f, Path.GetFileName(filepath), stats);
        }

        public void PlaceStream(Stream f, string name, FileStats stats)
        {
            Directory.CreateDirectory(PathRoot);
            Meta meta = new Meta()
            {
                name = name,
                size = stats.Size,
                mtime = stats.MTime
            };
            SaveMeta(meta);

            using (MeasuringStream dst = new MeasuringStream(new FileStream(PathObj, FileMode.OpenOrCreate, FileAccess.Write), StreamPurpose.Cache))
            {
                StreamUtil.CopyNTo(f, dst, stats.Size);
            }
            File.SetLastWriteTime(PathObj, stats.MTime);
        }

        public void CopyToFile(string dst)
        {
            Meta meta = LoadMeta();
            using (MeasuringStream fdst = new MeasuringStream(new FileStream(dst, FileMode.OpenOrCreate, FileAccess.Write), StreamPurpose.Target))
            {
                CopyTo(fdst, meta.size);
            }
            File.SetLastWriteTime(dst, meta.mtime);
        }

        public void CopyToStream(Stream dst, long maxSize)
        {
            Meta meta = LoadMeta();
            if (meta.size != maxSize)
            {
                throw new InvalidDataException();
            }
            CopyTo(dst, maxSize);
        }

        private void CopyTo(Stream dst, long size)
        {
            using (MeasuringStream src = new MeasuringStream(new FileStream(PathObj, FileMode.Open, FileAccess.Read), StreamPurpose.Cache))
            { 
                if (src.Length != size)
                {
                    throw new InvalidDataException();
                }
                StreamUtil.CopyNTo(src, dst, size);
            }
        }

        private Meta LoadMeta()
        {
            using (Stream s = new MeasuringStream(new FileStream(PathMeta, FileMode.Open, FileAccess.Read), StreamPurpose.CacheMeta))
            using (BinaryReader br = new BinaryReader(s))
            {
                var utf8Reader = new Utf8JsonReader(br.ReadBytes((int)s.Length));
                var meta = JsonSerializer.Deserialize<Meta>(ref utf8Reader);
                return meta;
            }
            
        }

        private void SaveMeta(Meta meta)
        {
            using (Stream s = new MeasuringStream(new FileStream(PathMeta, FileMode.OpenOrCreate, FileAccess.Write), StreamPurpose.CacheMeta))
            {
                byte[] jsonUtf8Bytes = JsonSerializer.SerializeToUtf8Bytes(meta);
                s.Write(jsonUtf8Bytes, 0, jsonUtf8Bytes.Length);
            }
        }
    }


    public class FileCache
    {
        private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public string Root { get; set; }


        public CacheObject Add(string filepath)
        {
            using (MeasuringStream s = new MeasuringStream(new FileStream(filepath, FileMode.Open), StreamPurpose.Source))
            {
                return Add(s, filepath);
            }
        }

        public CacheObject Add(Stream f, string filepath)
        {
            FileStats stats;
            CacheId id = FileCacheUtil.MakeFileId(f, filepath, out stats);

            CacheObject co = new CacheObject(id, Root);
            if (co.IsPathValid())
            {
                logger.Info("Already exists {0}", id);
                co.UpdateMtime(stats);
            }
            else
            {
                logger.Info("Placing {0}", id);
                co.PlaceStreamFile(f, filepath, stats);
            }

            return co;
        }

        public CacheObject AddFromStream(CacheId id, string name, FileStats stats, Stream s)
        {
            CacheObject co = new CacheObject(id, Root);
            if (co.IsPathValid())
            {
                //logger.Info("Already exists {0}", id);
                co.UpdateMtime(stats);
            }
            else
            {
                //logger.Info("Placing {0}", id);
                co.PlaceStream(s, name, stats);
            }

            return co;
        }

    }
}
