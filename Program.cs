using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace PakPatcher
{
	class StreamUtil
	{
		public static void FillBuffer(Stream stream, byte[] buffer, int numBytes)
		{
			int read = 0;
			do
			{
				int n = stream.Read(buffer, read, numBytes - read);
				if (n == 0)
				{
					throw new EndOfStreamException();
				}
				read += n;
			} while (read < numBytes);
		}

		public static byte[] ReadBuffer(Stream stream, byte[] buffer, int numBytes)
		{
			if (numBytes == 0) return buffer;
			if (buffer?.Length < numBytes)
			{
				buffer = new byte[numBytes];
			}
			FillBuffer(stream, buffer, numBytes);
			return buffer;
		}

		public static void CopyNTo(Stream src, Stream dst, long n)
		{
			long bufferSize = 4096;
			byte[] buffer = new byte[bufferSize];
			int read;
			while (n > 0 &&
				   (read = src.Read(buffer, 0, (int)Math.Min(bufferSize, n))) > 0)
			{
				dst.Write(buffer, 0, read);
				n -= read;
			}
		}

	}

	class CDREnd
	{
		public const uint SIGNATURE = 0x06054b50;
		public const long SIZE = 4 + (4 * 2) + (2 * 4) + 2;

		public uint lSignature;       // end of central dir signature    4 bytes  (0x06054b50)
		public ushort nDisk;            // number of this disk             2 bytes
		public ushort nCDRStartDisk;    // number of the disk with the start of the central directory  2 bytes
		public ushort numEntriesOnDisk; // total number of entries in the central directory on this disk  2 bytes
		public ushort numEntriesTotal;  // total number of entries in the central directory           2 bytes
		public uint lCDRSize;         // size of the central directory   4 bytes
		public uint lCDROffset;       // offset of start of central directory with respect to the starting disk number        4 bytes
		public ushort nCommentLength;   // .ZIP file comment length        2 bytes
										// .ZIP file comment (variable size, can be empty) follows


		public CDREnd(BinaryReader br)
		{
			lSignature = br.ReadUInt32();
			if (lSignature != SIGNATURE)
				throw new FileFormatException("Unexpected signature");
			nDisk = br.ReadUInt16();
			nCDRStartDisk = br.ReadUInt16();
			numEntriesOnDisk = br.ReadUInt16();
			numEntriesTotal = br.ReadUInt16();
			lCDRSize = br.ReadUInt32();
			lCDROffset = br.ReadUInt32();
			nCommentLength = br.ReadUInt16();
		}
	}

	[Flags]
	enum EGPFlags : ushort
	{
		GPF_ENCRYPTED = 1 << 0, // If set, indicates that the file is encrypted.
		GPF_DATA_DESCRIPTOR = 1 << 3, // if set, the CRC32 and sizes aren't set in the file header, but only in the data descriptor following compressed data
		GPF_RESERVED_8_ENHANCED_DEFLATING = 1 << 4, // Reserved for use with method 8, for enhanced deflating.
		GPF_COMPRESSED_PATCHED = 1 << 5, // the file is compressed patched data
	};

	enum ECompressionMethon : ushort
	{
		METHOD_STORE = 0, // The file is stored (no compression)
		METHOD_SHRINK = 1, // The file is Shrunk
		METHOD_REDUCE_1 = 2, // The file is Reduced with compression factor 1
		METHOD_REDUCE_2 = 3, // The file is Reduced with compression factor 2
		METHOD_REDUCE_3 = 4, // The file is Reduced with compression factor 3
		METHOD_REDUCE_4 = 5, // The file is Reduced with compression factor 4
		METHOD_IMPLODE = 6, // The file is Imploded
		METHOD_TOKENIZE = 7, // Reserved for Tokenizing compression algorithm
		METHOD_DEFLATE = 8, // The file is Deflated
		METHOD_DEFLATE64 = 9, // Enhanced Deflating using Deflate64(tm)
		METHOD_IMPLODE_PKWARE = 10, // PKWARE Date Compression Library Imploding
		METHOD_DEFLATE_AND_ENCRYPT = 11, // Deflate + Custom encryption (TEA)
		METHOD_DEFLATE_AND_STREAMCIPHER = 12, // Deflate + stream cipher encryption on a per file basis
		METHOD_STORE_AND_STREAMCIPHER_KEYTABLE = 13, // Store + Timur's encryption technique on a per file basis
		METHOD_DEFLATE_AND_STREAMCIPHER_KEYTABLE = 14, // Deflate + Timur's encryption technique on a per file basis
	};

	enum EVersion : ushort
	{
		VERSION_DEFAULT = 10, // Default value

		VERSION_TYPE_VOLUMELABEL = 11, // File is a volume label
		VERSION_TYPE_FOLDER = 20, // File is a folder (directory)
		VERSION_TYPE_PATCHDATASET = 27, // File is a patch data set 
		VERSION_TYPE_ZIP64 = 45, // File uses ZIP64 format extensions

		VERSION_COMPRESSION_DEFLATE = 20, // File is compressed using Deflate compression
		VERSION_COMPRESSION_DEFLATE64 = 21, // File is compressed using Deflate64(tm)
		VERSION_COMPRESSION_DCLIMPLODE = 25, // File is compressed using PKWARE DCL Implode 
		VERSION_COMPRESSION_BZIP2 = 46, // File is compressed using BZIP2 compression*
		VERSION_COMPRESSION_LZMA = 63, // File is compressed using LZMA
		VERSION_COMPRESSION_PPMD = 63, // File is compressed using PPMd+

		VERSION_ENCRYPTION_PKWARE = 20, // File is encrypted using traditional PKWARE encryption
		VERSION_ENCRYPTION_DES = 50, // File is encrypted using DES
		VERSION_ENCRYPTION_3DES = 50, // File is encrypted using 3DES
		VERSION_ENCRYPTION_RC2 = 50, // File is encrypted using original RC2 encryption
		VERSION_ENCRYPTION_RC4 = 50, // File is encrypted using RC4 encryption
		VERSION_ENCRYPTION_AES = 51, // File is encrypted using AES encryption
		VERSION_ENCRYPTION_RC2C = 51, // File is encrypted using corrected RC2 encryption**
		VERSION_ENCRYPTION_RC4C = 52, // File is encrypted using corrected RC2-64 encryption**
		VERSION_ENCRYPTION_NOOAEP = 61, // File is encrypted using non-OAEP key wrapping***
		VERSION_ENCRYPTION_CDR = 62, // Central directory encryption
		VERSION_ENCRYPTION_BLOWFISH = 63, // File is encrypted using Blowfish
		VERSION_ENCRYPTION_TWOFISH = 63, // File is encrypted using Twofish
	};

	enum EVersionCreator : ushort
	{
		CREATOR_MSDOS = 0, // MS-DOS and OS/2 (FAT / VFAT / FAT32 file systems)
		CREATOR_AMIGA = 1, // Amiga                     
		CREATOR_OpenVMS = 2, // OpenVMS
		CREATOR_UNIX = 3, // UNIX                      
		CREATOR_VM = 4, // VM/CMS
		CREATOR_ATARI = 5, // Atari ST                  
		CREATOR_OS2 = 6, // OS/2 H.P.F.S.
		CREATOR_MACINTOSH = 7, // Macintosh                 
		CREATOR_ZSYSTEM = 8, // Z-System
		CREATOR_CPM = 9, // CP/M                     
		CREATOR_WINDOWS = 10, // Windows NTFS
		CREATOR_MVS = 11, // MVS (OS/390 - Z/OS)      
		CREATOR_VSE = 12, // VSE
		CREATOR_ACORN = 13, // Acorn Risc               
		CREATOR_VFAT = 14, // VFAT
		CREATOR_AMVS = 15, // alternate MVS            
		CREATOR_BEOS = 16, // BeOS
		CREATOR_TANDEM = 17, // Tandem                   
		CREATOR_OS400 = 18, // OS/400
		CREATOR_OSX = 19, // OS X (Darwin)        

		CREATOR_UNUSED = 20, // 20 thru 255 - unused    
	};

	// This descriptor exists only if bit 3 of the general
	// purpose bit flag is set (see below).  It is byte aligned
	// and immediately follows the last byte of compressed data.
	// This descriptor is used only when it was not possible to
	// seek in the output .ZIP file, e.g., when the output .ZIP file
	// was standard output or a non seekable device.  For Zip64 format
	// archives, the compressed and uncompressed sizes are 8 bytes each.
	class DataDescriptor : IEquatable<DataDescriptor>
	{
		public const long SIZE = 3*4;

		public readonly uint lCRC32 = 0;             // crc-32                          4 bytes
		public readonly uint lSizeCompressed = 0;    // compressed size                 4 bytes
		public readonly uint lSizeUncompressed = 0;  // uncompressed size               4 bytes

		public DataDescriptor(BinaryReader br)
		{
			lCRC32 = br.ReadUInt32();
			lSizeCompressed = br.ReadUInt32();
			lSizeUncompressed = br.ReadUInt32();
		}

		public bool Equals(DataDescriptor other)
		{
			return lCRC32 == other.lCRC32 && lSizeCompressed == other.lSizeCompressed && lSizeUncompressed == other.lSizeUncompressed;
		}

		//	bool IsZIP64(const DataDescriptor& d) const
		//	{
		//		return lSizeCompressed == (ulong) ZIP64_SEE_EXTENSION || lSizeUncompressed == (ulong) ZIP64_SEE_EXTENSION;
		//	}

	}


	class CDRFileHeader
	{
		public const uint SIGNATURE = 0x02014b50;
		public const long SIZE = 4 + 11*2 + 2*4 + DataDescriptor.SIZE;
		// This is the offset from the start of the first disk on
		// which this file appears, to where the local header should
		// be found.  If an archive is in zip64 format and the value
		// in this field is 0xFFFFFFFF, the size will be in the
		// corresponding 8 byte zip64 extended information extra field.
		public const uint ZIP64_LOCAL_HEADER_OFFSET = 0xFFFFFFFF;

		public readonly uint lSignature;         // central file header signature   4 bytes  (0x02014b50)
		public readonly EVersionCreator nVersionMadeBy;     // version made by                 2 bytes
		public readonly EVersion nVersionNeeded;     // version needed to extract       2 bytes
		public readonly EGPFlags nFlags;             // general purpose bit flag        2 bytes
		public readonly ECompressionMethon nMethod;            // compression method              2 bytes
		public readonly ushort nLastModTime;       // last mod file time              2 bytes
		public readonly ushort nLastModDate;       // last mod file date              2 bytes
		public readonly DataDescriptor desc;
		public readonly ushort nFileNameLength;    // file name length                2 bytes
		public readonly ushort nExtraFieldLength;  // extra field length              2 bytes
		public readonly ushort nFileCommentLength; // file comment length             2 bytes
		public readonly ushort nDiskNumberStart;   // disk number start               2 bytes
		public readonly ushort nAttrInternal;      // internal file attributes        2 bytes
		public readonly uint lAttrExternal;      // external file attributes        4 bytes
		public readonly uint lLocalHeaderOffset; // relative offset of local header 4 bytes

		public readonly byte[] filenameBytes;
		public readonly byte[] extraField;
		public readonly byte[] comment;

		public string FileName { get; }


		public long CdrRecordSize => CDRFileHeader.SIZE + nFileNameLength + nExtraFieldLength + nFileCommentLength;

		public CDRFileHeader(BinaryReader br)
		{
			lSignature = br.ReadUInt32();
			if (lSignature != SIGNATURE)
				throw new FileFormatException("Unexpected signature");
			nVersionMadeBy = (EVersionCreator)br.ReadUInt16();
			nVersionNeeded = (EVersion)br.ReadUInt16();
			nFlags = (EGPFlags)br.ReadUInt16();
			nMethod = (ECompressionMethon)br.ReadUInt16();
			nLastModTime = br.ReadUInt16();
			nLastModDate = br.ReadUInt16();
			desc = new DataDescriptor(br);
			nFileNameLength = br.ReadUInt16();
			nExtraFieldLength = br.ReadUInt16();
			nFileCommentLength = br.ReadUInt16();
			nDiskNumberStart = br.ReadUInt16();
			nAttrInternal = br.ReadUInt16();
			lAttrExternal = br.ReadUInt32();
			lLocalHeaderOffset = br.ReadUInt32();

			filenameBytes = br.ReadBytes(nFileNameLength);
			extraField = br.ReadBytes(nExtraFieldLength);
			comment = br.ReadBytes(nFileCommentLength);

			FileName = Encoding.UTF8.GetString(filenameBytes);
		}

		public string FormatLastModTime()
		{
			ushort t = nLastModTime;
			int hour = t >> 11;
			int min = (t >> 5) & 0x3F;
			int sec = (t & 0x1F) * 2;
			return string.Format("{0:D2}:{1:D2}:{2:D2}", hour, min, sec);
		}

		public string FormatLastModDate()
		{
			ushort d = nLastModDate;
			int year = (d >> 9) + 1980;
			int mon = (d >> 5) & 0xF;
			int day = d & 0x1F;
			return string.Format("{0:D4}:{1:D2}:{2:D2}", year, mon, day);
		}
	}

	class LocalFileHeader
	{
		public const uint SIGNATURE = 0x04034b50;
		public const long SIZE = 4 + 7 * 2 + DataDescriptor.SIZE;

		public uint lSignature;        // local file header signature     4 bytes  (0x04034b50)
		public EVersion nVersionNeeded;    // version needed to extract       2 bytes
		public EGPFlags nFlags;            // general purpose bit flag        2 bytes
		public ECompressionMethon nMethod;           // compression method              2 bytes
		public ushort nLastModTime;      // last mod file time              2 bytes
		public ushort nLastModDate;      // last mod file date              2 bytes
		public DataDescriptor desc;
		public ushort nFileNameLength;   // file name length                2 bytes
		public ushort nExtraFieldLength; // extra field length              2 bytes

		public LocalFileHeader(BinaryReader br)
		{
			Parse(br);
		}

		public LocalFileHeader(byte[] buf)
		{
			using (MemoryStream ms = new MemoryStream(buf, false))
			using (BinaryReader br = new BinaryReader(ms))
			{
				Parse(br);
			}
		}

		public bool Check(CDRFileHeader cfh)
		{
			return nVersionNeeded == cfh.nVersionNeeded
				&& nFlags == cfh.nFlags
				&& nMethod == cfh.nMethod
				&& nLastModTime == cfh.nLastModTime
				&& nLastModDate == cfh.nLastModDate
				&& desc.Equals(cfh.desc)
				&& nFileNameLength == cfh.nFileNameLength;
		}

		private void Parse(BinaryReader br)
		{
			lSignature = br.ReadUInt32();
			if (lSignature != SIGNATURE)
				throw new FileFormatException("Unexpected signature");
			nVersionNeeded = (EVersion)br.ReadUInt16();
			nFlags = (EGPFlags)br.ReadUInt16();
			nMethod = (ECompressionMethon)br.ReadUInt16();
			nLastModTime = br.ReadUInt16();
			nLastModDate = br.ReadUInt16();
			desc = new DataDescriptor(br);
			nFileNameLength = br.ReadUInt16();
			nExtraFieldLength = br.ReadUInt16();
		}
	}

	class CDR
	{
		public List<CDRFileHeader> Entries { get; private set; }
		public ILookup<string, CDRFileHeader> Files => Entries.ToLookup(x => x.FileName);
		public CDR(List<CDRFileHeader> entries)
		{
			Entries = entries;
		}
	}

	class ZipReadFile
	{
		private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

		public CDR CDR { get; }
		public long CDROffset { get; }
		public Stream Stream { get; }
		

		public ZipReadFile(Stream stream)
		{
			Stream = stream;
			long cdrOffset;
			CDR = LoadCDR(stream, out cdrOffset);
			CDROffset = cdrOffset;
		}

		static CDR LoadCDR(Stream bs, out long outCdrOffset)
		{
			bool debugLog = false;


			long fileSize = bs.Length;
			if (debugLog) logger.Info($"File size {fileSize}");

			long commentStart;
			CDREnd fileCDREnd = FindCDREnd(bs, out commentStart);
			if (debugLog)
			{
				logger.Info("CDREnd {0:x} - {1:x} ({2:x})", commentStart - CDREnd.SIZE, commentStart, CDREnd.SIZE);
				logger.Info("  .lSignature        {0:x}", fileCDREnd.lSignature);
				logger.Info("  .nDisk             {0:x}", fileCDREnd.nDisk);
				logger.Info("  .nCDRStartDisk     {0:x}", fileCDREnd.nCDRStartDisk);
				logger.Info("  .numEntriesOnDisk  {0:x}", fileCDREnd.numEntriesOnDisk);
				logger.Info("  .numEntriesTotal   {0:x}", fileCDREnd.numEntriesTotal);
				logger.Info("  .lCDRSize          {0:x}", fileCDREnd.lCDRSize);
				logger.Info("  .lCDROffset        {0:x}", fileCDREnd.lCDROffset);
				logger.Info("  .nCommentLength    {0:x}", fileCDREnd.nCommentLength);
			}

			List<CDRFileHeader> cdr = new List<CDRFileHeader>(fileCDREnd.numEntriesTotal);

			// TODO: Test for crypak encryption
			if (fileCDREnd.lCDRSize > 0)
			{
				bs.Seek(fileCDREnd.lCDROffset, SeekOrigin.Begin);
				long cdrPos = bs.Position;
				long cdrPosEnd = bs.Position + fileCDREnd.lCDRSize;

				if (debugLog) logger.Info("CDR {0:x} - {1:x} ({2:x})", cdrPos, cdrPosEnd, cdrPosEnd - cdrPos);

				using (BinaryReader br = new BinaryReader(bs, Encoding.ASCII, true))
				{
					for (ushort fileIndex = 0; fileIndex < fileCDREnd.numEntriesTotal; fileIndex++)
					{
						long currentCDRPos = bs.Position;
						CDRFileHeader pfileCDRRecord = new CDRFileHeader(br);

						cdr.Add(pfileCDRRecord);

						if (debugLog)
						{
							logger.Info("  CDRFileHeader[{0:X4}] {1:x} - {2:x} ({3:x})", fileIndex, currentCDRPos, currentCDRPos + pfileCDRRecord.CdrRecordSize, pfileCDRRecord.CdrRecordSize);
							logger.Info("   .lSignature = {0:x}", pfileCDRRecord.lSignature);
							logger.Info("   .nVersionMadeBy = {0}", pfileCDRRecord.nVersionMadeBy);
							logger.Info("   .nVersionNeeded = {0}", pfileCDRRecord.nVersionNeeded);
							logger.Info("   .nFlags = {0}", pfileCDRRecord.nFlags);
							logger.Info("   .nMethod = {0}", pfileCDRRecord.nMethod);
							logger.Info("   .nLastModTime = {0:x}   {1}", pfileCDRRecord.nLastModTime, pfileCDRRecord.FormatLastModTime());
							logger.Info("   .nLastModDate = {0:x}   {1}", pfileCDRRecord.nLastModDate, pfileCDRRecord.FormatLastModDate());
							logger.Info("   .desc.lCRC32 = {0:x}", pfileCDRRecord.desc.lCRC32);
							logger.Info("   .desc.lSizeCompressed = {0:x}", pfileCDRRecord.desc.lSizeCompressed);
							logger.Info("   .desc.lSizeUncompressed = {0:x}", pfileCDRRecord.desc.lSizeUncompressed);
							logger.Info("   .nFileNameLength = {0:x}", pfileCDRRecord.nFileNameLength);
							logger.Info("   .nExtraFieldLength = {0:x}", pfileCDRRecord.nExtraFieldLength);
							logger.Info("   .nFileCommentLength = {0:x}", pfileCDRRecord.nFileCommentLength);
							logger.Info("   .nDiskNumberStart = {0:x}", pfileCDRRecord.nDiskNumberStart);
							logger.Info("   .nAttrInternal = {0:x}", pfileCDRRecord.nAttrInternal);
							logger.Info("   .lAttrExternal = {0:x}", pfileCDRRecord.lAttrExternal);
							logger.Info("   .lLocalHeaderOffset = {0:x}", pfileCDRRecord.lLocalHeaderOffset);
							logger.Info("   .fileName: {0}", pfileCDRRecord.FileName);
						}
					}
				}
			}

			outCdrOffset = fileCDREnd.lCDROffset;
			return new CDR(cdr);
		}

		static CDREnd FindCDREnd(Stream pArchiveFile, out long commentStart)
		{
			pArchiveFile.Seek(0, SeekOrigin.End);
			long fLength = pArchiveFile.Position;
			if (fLength < CDREnd.SIZE)
			{
				throw new FileFormatException("File isn't big enough to contain a CDREnd structure");
			}

			//Search backwards through the file for the CDREnd structure
			long nOldBufPos = fLength;
			// start scanning well before the end of the file to avoid reading beyond the end
			long nScanPos = nOldBufPos - CDREnd.SIZE;

			using (BinaryReader br = new BinaryReader(pArchiveFile, Encoding.ASCII, true))
			{
				//Scan the file, but don't scan far beyond the 64k limit of the comment size
				while (nScanPos >= 0 && nScanPos > (fLength - CDREnd.SIZE - 0xFFFF))
				{
					pArchiveFile.Seek(nScanPos, SeekOrigin.Begin);
					uint signature = br.ReadUInt32();
					if (signature == CDREnd.SIGNATURE)
					{
						//Found the CDREnd signature. Extract the CDREnd and test it.
						pArchiveFile.Seek(nScanPos, SeekOrigin.Begin);

						CDREnd pCDREnd = new CDREnd(br);
						//Test the CDREnd by examining the length of the comment
						long commentLength = fLength - pArchiveFile.Position;
						if (pCDREnd.nCommentLength == commentLength)
						{
							//Got it.
							commentStart = pArchiveFile.Position;
							//printf("Found a CDREnd structure in the file\n");
							return pCDREnd;
						}
						//False positive! Keep going.
					}
					//Didn't find the signature. Keep going
					nScanPos -= 1;
				}
			}

			throw new FileFormatException("Couldn't find a CDREnd structure");
		}
	}


	class Program
	{
		private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();



		static void InitLog()
		{
			var config = new NLog.Config.LoggingConfiguration();
			var logfile = new NLog.Targets.FileTarget("logfile") { FileName = "log.txt" };
			var logconsole = new NLog.Targets.ConsoleTarget("logconsole");
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logconsole);
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logfile);
			NLog.LogManager.Configuration = config;
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



		static void Replicate(ZipReadFile z1, ZipReadFile z2, Stream fsOut)
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
				if (!lfh.Check(srcRec))
					throw new FileFormatException($"LocalFileHeader failed check. File {srcRec.FileName}");

				fileNameBuf = StreamUtil.ReadBuffer(srcStream, fileNameBuf, lfh.nFileNameLength);
				if (!fileNameBuf.Take(lfh.nFileNameLength).SequenceEqual(srcRec.filenameBytes))
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

				h.AppendData(BitConverter.GetBytes((Int16)rec.nVersionNeeded));
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


		static void Replicate(ZipReadFile z, FileCache fc, Stream fsOut, DateTime zipMtime)
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
				if (!lfh.Check(rec))
					throw new FileFormatException($"LocalFileHeader failed check. File {rec.FileName}");

				fileNameBuf = StreamUtil.ReadBuffer(z.Stream, fileNameBuf, lfh.nFileNameLength);
				if (!fileNameBuf.Take(lfh.nFileNameLength).SequenceEqual(rec.filenameBytes))
					throw new FileFormatException($"LocalFileHeader filename differs from filename in CDR. File {rec.FileName}");

				fsOut.Write(localHeaderBuf, 0, localHeaderBuf.Length);
				fsOut.Write(fileNameBuf, 0, lfh.nFileNameLength);
				StreamUtil.CopyNTo(z.Stream, fsOut, lfh.nExtraFieldLength);

				long dataPos = z.Stream.Position;
				long curExpectedPos = expectedPos + LocalFileHeader.SIZE + lfh.nFileNameLength + lfh.nExtraFieldLength;
				if (curExpectedPos != dataPos)
					throw new FileFormatException($"Unable to replicate zip - data stream has holes. data expected offset {curExpectedPos:x}, actual offset {dataPos:x}");

				long size = lfh.desc.lSizeCompressed;

				FileStats fs = new FileStats() { Size = size, MTime = zipMtime };
				CacheId id = ComputeZipLocalFileCacheId(lfh);
				CacheObject co = fc.AddFromStream(id, Path.GetFileName(rec.FileName), fs, z.Stream);
				co.CopyToStream(fsOut, size);

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



		static void TestZipReplicate()
		{
			string inputFileV1 = "d:\\code\\PakPatcher\\test\\v1.zip";
			string inputFileV2 = "d:\\code\\PakPatcher\\test\\v2.zip";
			string outFileV2 = "d:\\code\\PakPatcher\\test\\v2_out.zip";

			using (BufferedStream fsv1 = new BufferedStream(new FileStream(inputFileV1, FileMode.Open)))
			using (BufferedStream fsv2 = new BufferedStream(new FileStream(inputFileV2, FileMode.Open)))
			{
				logger.Info("Loading {0}", inputFileV1);
				ZipReadFile z1 = new ZipReadFile(fsv1);
				logger.Info("Loading {0}", inputFileV2);
				ZipReadFile z2 = new ZipReadFile(fsv2);

				using (FileStream fsOut = new FileStream(outFileV2, FileMode.Create))
				{
					Replicate(z1, z2, fsOut);
				}
			}
		}

		static void TestCacheCopy()
		{
			var fc = new FileCache() { Root = @"f:\testcache" };

			fc.Add(@"e:\photo\2020_Ma\IMG-8436bb0cf2bceb815b2065ee9ea4beb5-V.jpeg.jpg").CopyToFile(@"f:\test_target\test.jpg");
		}

		static void TestZipCacheReplicate(string src, string dst, FileCache fc)
		{
			using (BufferedStream fsrc = new BufferedStream(new FileStream(src, FileMode.Open)))
			{
				DateTime zipMtime = File.GetLastWriteTime(src);
				logger.Info("Loading {0}", src);
				ZipReadFile z = new ZipReadFile(fsrc);

				using (FileStream fdst = new FileStream(dst, FileMode.Create))
				{
					Replicate(z, fc, fdst, zipMtime);
				}
			}
		}

		static void TestZipCacheReplicate()
		{
			var fc = new FileCache() { Root = @"f:\testcache" };
			TestZipCacheReplicate(@"d:\code\PakPatcher\test\v1.zip", @"d:\code\PakPatcher\test\v1_out.zip", fc);
			TestZipCacheReplicate(@"d:\code\PakPatcher\test\v2.zip", @"d:\code\PakPatcher\test\v2_out.zip", fc);
		}


		static void Main(string[] args)
        {
			InitLog();

			//TestZipReplicate();

			//TestCacheCopy();
			TestZipCacheReplicate();
		}
    }
}
