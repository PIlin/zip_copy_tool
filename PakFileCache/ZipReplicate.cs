using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

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

		

		static CDRFileHeader FindSameFile(CDRFileHeader query, CDR cdr, out string debugReason)
		{
			debugReason = "not found";
			foreach (CDRFileHeader src in cdr.Files[query.FileName])
			{
				if (!src.desc.Equals(query.desc)) { debugReason = "size or crc"; continue; }
				if (!(src.nLastModTime == query.nLastModTime)) { debugReason = "nLastModTime"; continue; }
				if (!(src.nLastModDate == query.nLastModDate)) { debugReason = "nLastModDate"; continue; }
				if (!(src.nVersionMadeBy == query.nVersionMadeBy)) { debugReason = "nVersionMadeBy"; continue; }
				if (!(src.nVersionNeeded == query.nVersionNeeded)) { debugReason = "nVersionNeeded"; continue; }
				if (!(src.nFlags == query.nFlags)) { debugReason = "nFlags"; continue; }
				if (!(src.nMethod == query.nMethod)) { debugReason = "nMethod"; continue; }
				if (!(src.nFileNameLength == query.nFileNameLength)) { debugReason = "nFileNameLength"; continue; }
				if (!(src.nExtraFieldLength == query.nExtraFieldLength)) { debugReason = "nExtraFieldLength"; continue; }
				if (!(src.nFileCommentLength == query.nFileCommentLength)) { debugReason = "nFileCommentLength"; continue; }
				if (!(src.nDiskNumberStart == query.nDiskNumberStart)) { debugReason = "nDiskNumberStart"; continue; }
				if (!(src.nAttrInternal == query.nAttrInternal)) { debugReason = "nAttrInternal"; continue; }
				if (!(src.lAttrExternal == query.lAttrExternal)) { debugReason = "lAttrExternal"; continue; }
				if (!src.filenameBytes.SequenceEqual(query.filenameBytes)) { debugReason = "filenameBytes"; continue; }
				if (!src.extraField.SequenceEqual(query.extraField)) { debugReason = "extraField"; continue; }
				if (!src.comment.SequenceEqual(query.comment)) { debugReason = "comment"; continue; }

				return src;
			}

			return null;
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
