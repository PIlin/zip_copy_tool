using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Windows;

namespace PakFileCache
{
    public static class ZipReplicate
    {
        private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        public const bool HackIgnoreLFHVersionNeeded = true;
        public const bool HackIgnoreInconsistentFilenameSeparator = true;

		public static void ReplicateZipFileWithCache(string src, string dst, FileCache fc)
		{
			logger.Info("Processing {0}", src);
			Stopwatch startTime = Stopwatch.StartNew();
			DateTime zipMtime;
			using (MeasuringStream ms = new MeasuringStream(new FileStream(src, FileMode.Open), StreamPurpose.Source))
			using (MeasuringStream fdst = new MeasuringStream(new FileStream(dst, FileMode.Create), StreamPurpose.Target))
			{
				using (BufferedStream fsrc = new BufferedStream(ms))
				{
					zipMtime = File.GetLastWriteTime(src);
					ZipReadFile z = new ZipReadFile(fsrc);
					logger.Info("Replicating {0}", src);
					Stopwatch startRepTime = Stopwatch.StartNew();
					Replicate(z, fc, fdst, zipMtime);
					startRepTime.Stop();
					logger.Info("Replication {0} done in {1}", src, startRepTime.Elapsed);
				}
			}
			File.SetLastWriteTime(dst, zipMtime);

			startTime.Stop();
			logger.Info("Processing {0} done in {1}", src, startTime.Elapsed);
		}

        public static void ReplicateUpdate(string srcV1, string srcV2, string dst)
        {
			string dstWork = dst;
			if (dst == srcV1 || dst == srcV2)
			{
				dstWork = dst + ".new";
                logger.Info("Processing {0} and {1} to produce {2} (writing to temporary {3})", srcV1, srcV2, dst, dstWork);
            }
			else
			{
                logger.Info("Processing {0} and {1} to produce {2}", srcV1, srcV2, dst);
            }
            
            Stopwatch startTime = Stopwatch.StartNew();
            DateTime zipMtime;
            using (MeasuringStream fv1 = new MeasuringStream(new FileStream(srcV1, FileMode.Open), StreamPurpose.Source))
            using (MeasuringStream fv2 = new MeasuringStream(new FileStream(srcV2, FileMode.Open), StreamPurpose.Source))
            using (MeasuringStream fdst = new MeasuringStream(new FileStream(dstWork, FileMode.Create), StreamPurpose.Target))
            {
                using (BufferedStream fbv1 = new BufferedStream(fv1))
                using (BufferedStream fbv2 = new BufferedStream(fv2))
                {
                    zipMtime = File.GetLastWriteTime(srcV2);
                    ZipReadFile z1 = new ZipReadFile(fbv1);
                    ZipReadFile z2 = new ZipReadFile(fbv2);
                    Stopwatch startRepTime = Stopwatch.StartNew();
                    Replicate(z1, z2, fdst);
                    startRepTime.Stop();
                    logger.Info("Replication done in {0}", startRepTime.Elapsed);
                }
            }

			if (dst != dstWork)
			{
				File.Replace(dstWork, dst, null);
			}

            File.SetLastWriteTime(dst, zipMtime);

            startTime.Stop();
            logger.Info("Processing {0}", startTime.Elapsed);
        }

		/// <summary>
		/// Update in-place with fuzzy zip structure
		/// </summary>
		public static void ReplicateUpdateFuzzy(string src, string dst)
		{
            logger.Info("Processing {0} to update from {1}", dst, src);
            Stopwatch startTime = Stopwatch.StartNew();
            DateTime zipMtime;
            using (MeasuringStream fsrc = new MeasuringStream(new FileStream(src, FileMode.Open, FileAccess.Read), StreamPurpose.Source))
            using (MeasuringStream fdst = new MeasuringStream(new FileStream(dst, FileMode.Open, FileAccess.ReadWrite), StreamPurpose.Target))
            {
                using (BufferedStream fbsrc = new BufferedStream(fsrc))
                using (BufferedStream fbdst = new BufferedStream(fdst))
                {
                    zipMtime = File.GetLastWriteTime(src);
                    ZipReadFile zsrc = new ZipReadFile(fbsrc);
                    ZipReadFile zdst = new ZipReadFile(fbdst);
                    logger.Info("Replicating {0}", src);
                    Stopwatch startRepTime = Stopwatch.StartNew();
                    ReplicateFuzzy(zsrc, zdst, fbdst);
                    startRepTime.Stop();
                    logger.Info("Replication {0} done in {1}", dst, startRepTime.Elapsed);
                }
            }
            File.SetLastWriteTime(dst, zipMtime);

            startTime.Stop();
            logger.Info("Processing {0} done in {1}", dst, startTime.Elapsed);
        }


        /// <summary>
        /// Replicate using FileCache
        /// </summary>
        public static void Replicate(ZipReadFile z, FileCache fc, Stream fsOut, DateTime zipMtime)
		{
			var zEntries = z.CDR.Entries;
			zEntries.Sort((a, b) => a.lLocalHeaderOffset.CompareTo(b.lLocalHeaderOffset));

			byte[] localHeaderBuf = new byte[LocalFileHeader.SIZE];
			byte[] fileNameBuf = new byte[256];

			long expectedPos = 0;
			foreach (var rec in zEntries)
			{
				if (rec.lLocalHeaderOffset != expectedPos)
					throw new FileFormatException($"Unable to replicate zip - data stream has holes. File {rec.FileName}, expected offset {expectedPos:x}, actual offset {rec.lLocalHeaderOffset:x}");

				z.Stream.Seek(rec.lLocalHeaderOffset, SeekOrigin.Begin);
				StreamUtil.FillBuffer(z.Stream, localHeaderBuf, localHeaderBuf.Length);

				LocalFileHeader lfh = new LocalFileHeader(localHeaderBuf);
				if (!lfh.Check(rec, HackIgnoreLFHVersionNeeded))
					throw new FileFormatException($"LocalFileHeader failed check. File {rec.FileName}");

				fileNameBuf = StreamUtil.ReadBuffer(z.Stream, fileNameBuf, lfh.nFileNameLength);
				if (!CompareFilenameBuffer(fileNameBuf, lfh.nFileNameLength, rec))
					throw new FileFormatException($"LocalFileHeader filename differs from filename in CDR. File {rec.FileName}");

				fsOut.Write(localHeaderBuf, 0, localHeaderBuf.Length);
				fsOut.Write(fileNameBuf, 0, lfh.nFileNameLength);
				StreamUtil.CopyNTo(z.Stream, fsOut, lfh.nExtraFieldLength);

				long dataPos = z.Stream.Position;
				long curExpectedPos = expectedPos + LocalFileHeader.SIZE + lfh.nFileNameLength + lfh.nExtraFieldLength;
				if (curExpectedPos != dataPos)
					throw new FileFormatException($"Unable to replicate zip - data stream has holes. data expected offset {curExpectedPos:x}, actual offset {dataPos:x}");

				long size = lfh.desc.lSizeCompressed;

				if (size >= fc.SmallFileSize)
				{
					FileStats fs = new FileStats() { Size = size, MTime = zipMtime };
					CacheId id = ComputeZipLocalFileCacheId(lfh);
					CacheObject co = fc.AddFromStream(id, Path.GetFileName(rec.FileName), fs, z.Stream);
					co.CopyToStream(fsOut, size);
				}
				else
				{
					StreamUtil.CopyNTo(z.Stream, fsOut, size);
				}

				expectedPos += LocalFileHeader.SIZE + lfh.nFileNameLength + lfh.nExtraFieldLength + size;
			}

			if (z.CDROffset != expectedPos)
				throw new FileFormatException($"Unable to replicate zip - data stream has holes. CDR expected offset {expectedPos:x}, actual offset {z.CDROffset:x}");

			{
				// Copy CDR + CDREnd + comment
				Stream srcStream = z.Stream;
				srcStream.Seek(z.CDROffset, SeekOrigin.Begin);
				srcStream.CopyTo(fsOut);
			}
		}

		/// <summary>
		/// Replicate using local file version
		/// </summary>
		public static void Replicate(ZipReadFile z1, ZipReadFile z2, Stream fsOut)
		{
			var z2Entries = z2.CDR.Entries;
			z2Entries.Sort((a, b) => a.lLocalHeaderOffset.CompareTo(b.lLocalHeaderOffset));

			byte[] localHeaderBuf = new byte[LocalFileHeader.SIZE];
			byte[] fileNameBuf = new byte[256];

			long expectedPos = 0;
			foreach (var rec in z2Entries)
			{
				if (rec.lLocalHeaderOffset != expectedPos)
					throw new FileFormatException($"Unable to replicate zip - data stream has holes. File {rec.FileName}, expected offset {expectedPos:x}, actual offset {rec.lLocalHeaderOffset:x}");

				string debugReason;
				var srcRec = FindSameFile(rec, z1.CDR, out debugReason);
				Stream srcStream;
				if (srcRec != null)
				{
					srcStream = z1.Stream;
					logger.Info("Copying {0} from z1", srcRec.FileName);
				}
				else
				{
					srcRec = rec;
					srcStream = z2.Stream;
					logger.Info("Copying {0} from z2 because {1}", srcRec.FileName, debugReason);
				}

				srcStream.Seek(srcRec.lLocalHeaderOffset, SeekOrigin.Begin);
				StreamUtil.FillBuffer(srcStream, localHeaderBuf, localHeaderBuf.Length);

				LocalFileHeader lfh = new LocalFileHeader(localHeaderBuf);
				if (!lfh.Check(srcRec, HackIgnoreLFHVersionNeeded))
					throw new FileFormatException($"LocalFileHeader failed check. File {srcRec.FileName}");

				fileNameBuf = StreamUtil.ReadBuffer(srcStream, fileNameBuf, lfh.nFileNameLength);
				if (!CompareFilenameBuffer(fileNameBuf, lfh.nFileNameLength, rec))
					throw new FileFormatException($"LocalFileHeader filename differs from filename in CDR. File {srcRec.FileName}");

				fsOut.Write(localHeaderBuf, 0, localHeaderBuf.Length);
				fsOut.Write(fileNameBuf, 0, lfh.nFileNameLength);
				long size = lfh.nExtraFieldLength + lfh.desc.lSizeCompressed;
				StreamUtil.CopyNTo(srcStream, fsOut, size);

				expectedPos += LocalFileHeader.SIZE + lfh.nFileNameLength + size;
			}
			if (z2.CDROffset != expectedPos)
				throw new FileFormatException($"Unable to replicate zip - data stream has holes. CDR expected offset {expectedPos:x}, actual offset {z2.CDROffset:x}");

			{
				// Copy CDR + CDREnd + comment
				Stream srcStream = z2.Stream;
				srcStream.Seek(z2.CDROffset, SeekOrigin.Begin);
				srcStream.CopyTo(fsOut);
			}
		}

		readonly struct Segment
		{
            public Segment(string n, uint s, uint e) { name = n; start = s; end = e; }
            public Segment(uint s, uint e) { name = null; start = s; end = e; }

            public string name { get; }
			public uint start { get; }
			public uint end { get; }

			public uint Size => end - start;
			public bool IsHole => name == null;

			public Segment MakeHole() { return new Segment(null, start, end); }
			public Segment WithEnd(uint e) { return new Segment(name, start, e); }
			public Segment WithStart(uint s) { return new Segment(name, s, end); }

            //bool Equals(Segment other)
            //{
            //	return name == other.name && start == other.start && end == other.end;
            //}

            public override string ToString()
            {
                return $"{{{start}:{end}}}; {name}";
            }
        }

		class SegmentsList
		{
            private List<Segment> segments = new List<Segment>
            {
                new Segment(0, 0)
            };
			public List<Segment> Segments => segments;
			public int Count => segments.Count;
            public Segment this[int index] 
			{
                get => segments[index];
                set => segments[index] = value;
            }

			public void Add(Segment segment) => segments.Add(segment);

            public void MergeHoles()
            {
                Debug.Assert(segments.Count > 0);
                var merged = new List<Segment>(segments.Count) { segments[0] };
                for (int i = 1; i < segments.Count; i++)
                {
                    var prev = merged[merged.Count - 1];
                    var seg = segments[i];
                    Debug.Assert(prev.end == seg.start);
                    if (prev.IsHole && seg.IsHole)
                    {
                        merged[merged.Count - 1] = prev.WithEnd(seg.end);
                    }
                    else
                    {
                        merged.Add(seg);
                    }
                }
				segments = merged;
            }

			public Segment MakeExpandedHole(int iseg)
			{
                Segment hole = segments[iseg].MakeHole();
                if (iseg > 0 && segments[iseg - 1].IsHole)
                {
                    Debug.Assert(segments[iseg - 1].end == hole.start);
                    hole = hole.WithStart(segments[iseg - 1].start);
					segments.RemoveAt(iseg - 1);
					iseg--;
                }
                if (iseg + 1 < segments.Count && segments[iseg + 1].IsHole)
                {
                    Debug.Assert(segments[iseg + 1].start == hole.end);
                    hole = hole.WithEnd(segments[iseg + 1].end);
                }
				segments[iseg] = hole;
				return hole;
            }

			public int FindHole(uint size)
			{
				for (int i = 0; i < segments.Count; i++)
				{
					Segment seg = segments[i];
					if (seg.IsHole && seg.Size >= size)
					{
						return i;
					}
				}
				return -1;
			}

			public void FillHole(int ihole, Segment hole, Segment seg)
			{
				Debug.Assert(hole.IsHole);
				Debug.Assert(segments[ihole].Equals(hole));
				Debug.Assert(hole.start == seg.start);
				Debug.Assert(seg.end <= hole.end);
				if (seg.end < hole.end)
				{
					segments.Insert(ihole, seg);
					Debug.Assert(segments[ihole + 1].Equals(hole));
					segments[ihole + 1] = hole.WithStart(seg.end);
                }
				else
				{
					segments[ihole] = seg;
				}
			}

			public Segment ReserveEnd(uint size)
			{
				var last = segments.Last();

                if (last.IsHole)
				{
					if (last.Size < size)
					{
						last = segments[segments.Count - 1] = last.WithEnd(last.end + (size - last.Size));
					}
				}
				else
				{
					segments.Add(new Segment(null, last.end, last.end + size));
                    last = segments.Last();
                }

				Debug.Assert(last.IsHole && last.Size >= size);
				return last;
			}

			public Segment PushSegment(Segment seg)
			{
                var prev = segments[segments.Count - 1];
				Debug.Assert(prev.end <= seg.start);

				if (prev.end < seg.start)
				{
					if (prev.IsHole)
					{
						prev = segments[segments.Count - 1] = prev.WithEnd(seg.start);
					}
					else
					{
                        segments.Add(new Segment(prev.end, seg.start));
                        prev = segments[segments.Count - 1];
                    }
				}

                Debug.Assert(prev.end == seg.start);
				Add(seg);
				return seg;
            }
        }

		public static void ReplicateFuzzy(ZipReadFile zsrc, ZipReadFile zdst, Stream fsOut)
        {
			var zsrcEntries = zsrc.CDR.Entries;
            zsrcEntries.Sort((a, b) => a.lLocalHeaderOffset.CompareTo(b.lLocalHeaderOffset));

            var zdstEntries = zdst.CDR.Entries;
            zdstEntries.Sort((a, b) => a.lLocalHeaderOffset.CompareTo(b.lLocalHeaderOffset));

			var segments = new SegmentsList();
            for (int i = 0; i < zdstEntries.Count; ++i)
			{
				var entry = zdstEntries[i];
                segments.PushSegment(new Segment(entry.FileName, entry.lLocalHeaderOffset, entry.lLocalHeaderOffset + entry.FullRecordSize));
            }
            
            List<CDRFileHeader> toKeepInDstCdr = new List<CDRFileHeader>(zdstEntries.Count);
            List<CDRFileHeader> toUpdateFromSrcCdr = new List<CDRFileHeader>(zsrcEntries.Count);
            foreach (var rec in zdstEntries)
            {
				var similarFiles = zsrc.CDR.Files[rec.FileName].ToArray();
				if (similarFiles.Length > 1)
				{
					throw new FileFormatException($"Source archive has multiple files with the same name: {rec.FileName}");
				}

				if (similarFiles.Length > 0)
				{
					string debugReason;
					var srcRec = similarFiles[0];

					if (IsSameFile(rec, srcRec, out debugReason))
					{
						toKeepInDstCdr.Add(rec);
						continue;
					}
					else
					{
						toUpdateFromSrcCdr.Add(srcRec);
						// fall through
					}
				}
				else
				{
					// to remove, fall through
				}
				
				int idx = segments.Segments.FindIndex(s => s.name == rec.FileName);
				Debug.Assert(idx >= 0);
                var seg = segments[idx];
				segments[idx] = seg.MakeHole();
			}
			foreach (var rec in zsrcEntries)
			{
                if (zdst.CDR.Files[rec.FileName].Count() == 0)
				{
                    toUpdateFromSrcCdr.Add(rec);
				}
            }
            toUpdateFromSrcCdr.Sort((a, b) => a.lLocalHeaderOffset.CompareTo(b.lLocalHeaderOffset));


            // merge holes
            segments.MergeHoles();


            byte[] localHeaderBuf = new byte[LocalFileHeader.SIZE];
            byte[] fileNameBuf = new byte[256];

            List<CDRFileHeader> newCdr = new List<CDRFileHeader>(zsrcEntries.Count);

			var srcStream = zsrc.Stream;
			foreach (var srcRec in toUpdateFromSrcCdr)
			{
				int ihole = segments.FindHole(srcRec.FullRecordSize);
				Segment hole;
				if (ihole >= 0)
				{
					hole = segments[ihole];
				}
				else
				{
					hole = segments.ReserveEnd(srcRec.FullRecordSize);
					ihole = segments.Count - 1;
				}
				Debug.Assert(srcRec.FullRecordSize <= hole.Size);
				Segment seg = new Segment(srcRec.FileName, hole.start, hole.start + srcRec.FullRecordSize);
				segments.FillHole(ihole, hole, seg);

				srcStream.Seek(srcRec.lLocalHeaderOffset, SeekOrigin.Begin);
				StreamUtil.FillBuffer(srcStream, localHeaderBuf, localHeaderBuf.Length);

				LocalFileHeader lfh = new LocalFileHeader(localHeaderBuf);
				if (!lfh.Check(srcRec, HackIgnoreLFHVersionNeeded))
					throw new FileFormatException($"LocalFileHeader failed check. File {srcRec.FileName}");

				fileNameBuf = StreamUtil.ReadBuffer(srcStream, fileNameBuf, lfh.nFileNameLength);
				if (!CompareFilenameBuffer(fileNameBuf, lfh.nFileNameLength, srcRec))
					throw new FileFormatException($"LocalFileHeader filename differs from filename in CDR. File {srcRec.FileName}");

				fsOut.Seek(seg.start, SeekOrigin.Begin);
				fsOut.Write(localHeaderBuf, 0, localHeaderBuf.Length);
				fsOut.Write(fileNameBuf, 0, lfh.nFileNameLength);
				long size = lfh.nExtraFieldLength + lfh.desc.lSizeCompressed;
				StreamUtil.CopyNTo(srcStream, fsOut, size);

				var dstRec = new CDRFileHeader(srcRec, seg.start);
				newCdr.Add(dstRec);
            }
			newCdr.AddRange(toKeepInDstCdr);
            newCdr.Sort((a, b) => a.lLocalHeaderOffset.CompareTo(b.lLocalHeaderOffset));

			Debug.Assert(newCdr.Count == zsrc.CDR.Entries.Count);

			uint cdrStart = 0;
			for (int i = segments.Count - 1; i >= 0; i--)
			{
				if (segments[i].IsHole)
				{
					continue;
				}
                cdrStart = segments[i].end;
				break;
			}

			fsOut.Seek(cdrStart, SeekOrigin.Begin);
			using (BinaryWriter bw = new BinaryWriter(fsOut, Encoding.ASCII, true))
			{
				Debug.Assert(fsOut.Position == cdrStart);
				foreach (var entry in newCdr)
				{
					entry.Write(bw);
				}
				uint cdrEnd = (uint)fsOut.Position;

				Debug.Assert(newCdr.Count <= ushort.MaxValue);
				CDREnd end = new CDREnd(cdrStart, cdrEnd - cdrStart, (ushort)newCdr.Count);
				end.Write(bw);
            }

			fsOut.SetLength(fsOut.Position);
        }


        static CDRFileHeader FindSameFile(CDRFileHeader query, CDR cdr, out string debugReason)
		{
			debugReason = "not found";
			foreach (CDRFileHeader src in cdr.Files[query.FileName])
			{
				if (!IsSameFile(src, query, out debugReason))
					continue;
				return src;
			}

			return null;
		}

		static bool IsSameFile(CDRFileHeader src, CDRFileHeader query, out string debugReason)
		{
            if (!src.desc.Equals(query.desc)) { debugReason = "size or crc"; return false; }
            if (!(src.nLastModTime == query.nLastModTime)) { debugReason = "nLastModTime"; return false; }
            if (!(src.nLastModDate == query.nLastModDate)) { debugReason = "nLastModDate"; return false; }
            if (!(src.nVersionMadeBy == query.nVersionMadeBy)) { debugReason = "nVersionMadeBy"; return false; }
            if (!(src.nVersionNeeded == query.nVersionNeeded)) { debugReason = "nVersionNeeded"; return false; }
            if (!(src.nFlags == query.nFlags)) { debugReason = "nFlags"; return false; }
            if (!(src.nMethod == query.nMethod)) { debugReason = "nMethod"; return false; }
            if (!(src.nFileNameLength == query.nFileNameLength)) { debugReason = "nFileNameLength"; return false; }
            if (!(src.nExtraFieldLength == query.nExtraFieldLength)) { debugReason = "nExtraFieldLength"; return false; }
            if (!(src.nFileCommentLength == query.nFileCommentLength)) { debugReason = "nFileCommentLength"; return false; }
            if (!(src.nDiskNumberStart == query.nDiskNumberStart)) { debugReason = "nDiskNumberStart"; return false; }
            if (!(src.nAttrInternal == query.nAttrInternal)) { debugReason = "nAttrInternal"; return false; }
            if (!(src.lAttrExternal == query.lAttrExternal)) { debugReason = "lAttrExternal"; return false; }
            if (!src.filenameBytes.SequenceEqual(query.filenameBytes)) { debugReason = "filenameBytes"; return false; }
            if (!src.extraField.SequenceEqual(query.extraField)) { debugReason = "extraField"; return false; }
            if (!src.comment.SequenceEqual(query.comment)) { debugReason = "comment"; return false; }
			debugReason = "same";
            return true;
        }

		static bool CompareFilenameBuffer(byte[] fileNameBuf, int fileNameLength, CDRFileHeader rec)
		{
			if (!fileNameBuf.Take(fileNameLength).SequenceEqual(rec.filenameBytes))
			{
#pragma warning disable CS0162 // Unreachable code detected
				if (HackIgnoreInconsistentFilenameSeparator)
				{
					string headerFileName = Encoding.UTF8.GetString(fileNameBuf, 0, fileNameLength);
					// sometimes slashes are inconsistent between CDR and LFH.
					return (rec.FileName.Replace('\\', '/') == headerFileName.Replace('\\', '/'));
				}
				return false;
#pragma warning restore CS0162 // Unreachable code detected
			}
			return true;
		}

		static CacheId ComputeZipLocalFileCacheId(LocalFileHeader rec)
		{
			using (var h = IncrementalHash.CreateHash(HashAlgorithmName.SHA1))
			{
				if (!BitConverter.IsLittleEndian)
				{
					throw new NotImplementedException("BigEndian support is not implmeneted");
				}
				if (rec.nExtraFieldLength != 0)
				{
					throw new NotImplementedException($"Support for nExtraFieldLength = {rec.nExtraFieldLength} > 0 is not implemented");
				}

				if (!HackIgnoreLFHVersionNeeded)
				{
#pragma warning disable CS0162 // Unreachable code detected
					h.AppendData(BitConverter.GetBytes((Int16)rec.nVersionNeeded));
#pragma warning restore CS0162 // Unreachable code detected
				}
				h.AppendData(BitConverter.GetBytes((Int16)rec.nFlags));
				// TODO: does modtime matter? Maybe not
				//h.AppendData(BitConverter.GetBytes(rec.nLastModTime));
				//h.AppendData(BitConverter.GetBytes(rec.nLastModDate));

				h.AppendData(BitConverter.GetBytes(rec.desc.lCRC32));
				h.AppendData(BitConverter.GetBytes(rec.desc.lSizeCompressed));
				h.AppendData(BitConverter.GetBytes(rec.desc.lSizeUncompressed));

				return new CacheId(h.GetHashAndReset());
			}
		}

	}
}
