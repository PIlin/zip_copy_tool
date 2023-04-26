using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PakFileCache
{

	class CDREnd
	{
		public const uint SIGNATURE = 0x06054b50;
		public const long SIZE = 4 + (4 * 2) + (2 * 4) + 2;

		public static readonly byte[] SignatureBytes = new byte[] { 0x50, 0x4b, 0x05, 0x06 };

		public uint signature;
		public ushort diskNumber;
		public ushort diskStart;
		public ushort numEntriesThisDisk;
		public ushort numEntriesTotal;
		public uint cdrSize;
		public uint cdrOffset;
		public ushort commentSize;

		public CDREnd(BinaryReader br)
		{
			signature = br.ReadUInt32();
			if (signature != SIGNATURE)
				throw new FileFormatException("Unexpected signature");
			diskNumber = br.ReadUInt16();
			diskStart = br.ReadUInt16();
			numEntriesThisDisk = br.ReadUInt16();
			numEntriesTotal = br.ReadUInt16();
			cdrSize = br.ReadUInt32();
			cdrOffset = br.ReadUInt32();
			commentSize = br.ReadUInt16();
		}
		public CDREnd(BinaryReader br, uint sig)
		{
			signature = sig;
			if (signature != SIGNATURE)
				throw new FileFormatException("Unexpected signature");
			diskNumber = br.ReadUInt16();
			diskStart = br.ReadUInt16();
			numEntriesThisDisk = br.ReadUInt16();
			numEntriesTotal = br.ReadUInt16();
			cdrSize = br.ReadUInt32();
			cdrOffset = br.ReadUInt32();
			commentSize = br.ReadUInt16();
		}


		public CDREnd(uint offset, uint size, ushort numEntries, byte[] comment)
		{
			signature = SIGNATURE;
			diskNumber = 0;
			diskStart = 0;
			numEntriesThisDisk = numEntries;
			numEntriesTotal = numEntries;
			cdrSize = size;
			cdrOffset = offset;
			commentSize = (ushort)(comment != null ? comment.Length : 0);
		}

		public void Write(BinaryWriter bw)
		{
			bw.Write((UInt32)signature);
			bw.Write((UInt16)diskNumber);
			bw.Write((UInt16)diskStart);
			bw.Write((UInt16)numEntriesThisDisk);
			bw.Write((UInt16)numEntriesTotal);
			bw.Write((UInt32)cdrSize);
			bw.Write((UInt32)cdrOffset);
			bw.Write((UInt16)commentSize);
		}
	}

	[Flags]
	public enum EGPFlags : ushort
	{
		Encrypted = 1 << 0,
		CompressOption1 = 1 << 1,
		CompressOption2 = 1 << 2,
		UseDataDescryptor = 1 << 3,
		CompressedPatch = 1 << 5,
		StrongEncryption = 1 << 6,
		Utf8Filename = 1 << 11
	};

	public enum ECompressionMethon : ushort
	{
		Store = 0, // The file is stored (no compression)
		Shrink = 1, // The file is Shrunk
		Reduce1 = 2, // The file is Reduced with compression factor 1
		Reduce2 = 3, // The file is Reduced with compression factor 2
		Reduce3 = 4, // The file is Reduced with compression factor 3
		Reduce4 = 5, // The file is Reduced with compression factor 4
		Implode = 6, // The file is Imploded
		Tokenize = 7, // Reserved for Tokenizing compression algorithm
		Deflate = 8, // The file is Deflated
		Deflate64 = 9, // Enhanced Deflating using Deflate64(tm)
		ImplodePKWARE = 10, // PKWARE Data Compression Library Imploding (old IBM TERSE)
		BZIP2 = 12, // File is compressed using BZIP2 algorithm
		LZMA = 14, // LZMA
		IBM_zOS = 16, // IBM z/OS CMPSC Compression
		IBM_Terse = 18, // File is compressed using IBM TERSE (new)
		IBM_LZ77 = 19, // IBM LZ77 z Architecture
		Zstd = 93, // Zstandard (zstd) Compression 
		MP3 = 94, // MP3 Compression 
		XZ = 95, // XZ Compression
		JPEG = 96, // JPEG variant
		WawPack = 97, // WavPack compressed data
		PPMd = 98, // PPMd version I, Rev 1
		AEx = 99, // AE-x encryption marker (see APPENDIX E)
	};

	public enum EVersion : ushort
	{
		Default = 10, // Default value
		VolumeLabel = 11, // File is a volume label
		Folder = 20, // File is a folder (directory)
		CompressedDeflate = 20, // File is compressed using Deflate compression
		EncryptedPKWARE = 20, // File is encrypted using traditional PKWARE encryption
		CompressedDeflate64 = 21, // File is compressed using Deflate64(tm)
		CompressedDCLImplode = 25, // File is compressed using PKWARE DCL Implode 
		PatchDataSet = 27, // File is a patch data set 
		Zip64Extensions = 45, // File uses ZIP64 format extensions
		CompressedBZIP2 = 46, // File is compressed using BZIP2 compression*
		EncryptedDES = 50, // File is encrypted using DES
		Encrypted3DES = 50, // File is encrypted using 3DES
		EncryptedRC2 = 50, // File is encrypted using original RC2 encryption
		EncryptedRC4 = 50, // File is encrypted using RC4 encryption
		EncryptedAES = 51, // File is encrypted using AES encryption
		EncryptedRC2Corrected = 51, // File is encrypted using corrected RC2 encryption**
		EncryptedRC2_64 = 52, // File is encrypted using corrected RC2-64 encryption**
		EncryptedNonOAEP = 61, // File is encrypted using non-OAEP key wrapping***
		EncryptedCDR = 62, // Central directory encryption
		CompressedLZMA = 63, // File is compressed using LZMA
		CompressedPPMD = 63, // File is compressed using PPMd+
		EncryptedBlowfish = 63, // File is encrypted using Blowfish
		EncryptedTwofish = 63, // File is encrypted using Twofish
	};

	public enum EVersionCreator : ushort
	{
		MSDOS = 0,
		Amiga = 1,
		OpenVMS = 2,
		UNIX = 3,
		VM = 4,
		Atari = 5,
		OS2 = 6,
		Macintosh = 7,
		ZSystem = 8,
		CPM = 9,
		Windows = 10,
		MVS = 11,
		VSE = 12,
		Acorn = 13,
		VFAT = 14,
		AMVS = 15,
		BEOS = 16,
		Tandem = 17,
		OS400 = 18,
		OSX = 19,

		Unused = 20,
	};

	public class DataDescriptor : IEquatable<DataDescriptor>
	{
		public const long SIZE = 3 * 4;

		public readonly uint crc32 = 0;
		public readonly uint sizeCompressed = 0;
		public readonly uint sizeUncompressed = 0;

		public DataDescriptor(BinaryReader br)
		{
			crc32 = br.ReadUInt32();
			sizeCompressed = br.ReadUInt32();
			sizeUncompressed = br.ReadUInt32();
		}

		public bool Equals(DataDescriptor other)
		{
			return crc32 == other.crc32 && sizeCompressed == other.sizeCompressed && sizeUncompressed == other.sizeUncompressed;
		}

		public void Write(BinaryWriter bw)
		{
			bw.Write((UInt32)crc32);
			bw.Write((UInt32)sizeCompressed);
			bw.Write((UInt32)sizeUncompressed);
		}
	}


	public class CDRFileHeader
	{
		public const uint SIGNATURE = 0x02014b50;
		public const long SIZE = 4 + 11 * 2 + 2 * 4 + DataDescriptor.SIZE;

		public readonly uint signature;
		public readonly EVersionCreator createVersion;
		public readonly EVersion extractVersion;
		public readonly EGPFlags flags;
		public readonly ECompressionMethon method;
		public readonly ushort modTime;
		public readonly ushort modDate;
		public readonly DataDescriptor desc;
		public readonly ushort fileNameSize;
		public readonly ushort extraFieldSize;
		public readonly ushort commentLength;
		public readonly ushort diskNumberStart;
		public readonly ushort internalFileAttributes;
		public readonly uint externalFileAttributes;
		public readonly uint localHeaderOffset;

		public readonly byte[] filenameBytes;
		public readonly byte[] extraField;
		public readonly byte[] comment;

		public string FileName { get; }


		public long CdrRecordSize => CDRFileHeader.SIZE + fileNameSize + extraFieldSize + commentLength;

		public uint FullRecordSize => (uint)LocalFileHeader.SIZE + fileNameSize + extraFieldSize + desc.sizeCompressed;


		public CDRFileHeader(BinaryReader br)
		{
			signature = br.ReadUInt32();
			if (signature != SIGNATURE)
				throw new FileFormatException("Unexpected signature");
			createVersion = (EVersionCreator)br.ReadUInt16();
			extractVersion = (EVersion)br.ReadUInt16();
			flags = (EGPFlags)br.ReadUInt16();
			method = (ECompressionMethon)br.ReadUInt16();
			modTime = br.ReadUInt16();
			modDate = br.ReadUInt16();
			desc = new DataDescriptor(br);
			fileNameSize = br.ReadUInt16();
			extraFieldSize = br.ReadUInt16();
			commentLength = br.ReadUInt16();
			diskNumberStart = br.ReadUInt16();
			internalFileAttributes = br.ReadUInt16();
			externalFileAttributes = br.ReadUInt32();
			localHeaderOffset = br.ReadUInt32();

			filenameBytes = br.ReadBytes(fileNameSize);
			extraField = br.ReadBytes(extraFieldSize);
			comment = br.ReadBytes(commentLength);

			FileName = Encoding.UTF8.GetString(filenameBytes);
		}

		public CDRFileHeader(CDRFileHeader h)
		{
			signature = h.signature;
			createVersion = h.createVersion;
			extractVersion = h.extractVersion;
			flags = h.flags;
			method = h.method;
			modTime = h.modTime;
			modDate = h.modDate;
			desc = h.desc;
			fileNameSize = h.fileNameSize;
			extraFieldSize = h.extraFieldSize;
			commentLength = h.commentLength;
			diskNumberStart = h.diskNumberStart;
			internalFileAttributes = h.internalFileAttributes;
			externalFileAttributes = h.externalFileAttributes;
			localHeaderOffset = h.localHeaderOffset;
			filenameBytes = h.filenameBytes;
			extraField = h.extraField;
			comment = h.comment;
			FileName = h.FileName;
		}

		public CDRFileHeader(CDRFileHeader h, uint newLocalHeaderOffset)
		{
			signature = h.signature;
			createVersion = h.createVersion;
			extractVersion = h.extractVersion;
			flags = h.flags;
			method = h.method;
			modTime = h.modTime;
			modDate = h.modDate;
			desc = h.desc;
			fileNameSize = h.fileNameSize;
			extraFieldSize = h.extraFieldSize;
			commentLength = h.commentLength;
			diskNumberStart = h.diskNumberStart;
			internalFileAttributes = h.internalFileAttributes;
			externalFileAttributes = h.externalFileAttributes;
			localHeaderOffset = newLocalHeaderOffset;
			filenameBytes = h.filenameBytes;
			extraField = h.extraField;
			comment = h.comment;
			FileName = h.FileName;
		}


		public string FormatLastModTime()
		{
			ushort t = modTime;
			int hour = t >> 11;
			int min = (t >> 5) & 0x3F;
			int sec = (t & 0x1F) * 2;
			return string.Format("{0:D2}:{1:D2}:{2:D2}", hour, min, sec);
		}

		public string FormatLastModDate()
		{
			ushort d = modDate;
			int year = (d >> 9) + 1980;
			int mon = (d >> 5) & 0xF;
			int day = d & 0x1F;
			return string.Format("{0:D4}:{1:D2}:{2:D2}", year, mon, day);
		}

		public void Write(BinaryWriter bw)
		{
			bw.Write(signature);
			bw.Write((UInt16)createVersion);
			bw.Write((UInt16)extractVersion);
			bw.Write((UInt16)flags);
			bw.Write((UInt16)method);
			bw.Write((UInt16)modTime);
			bw.Write((UInt16)modDate);
			desc.Write(bw);
			bw.Write((UInt16)fileNameSize);
			bw.Write((UInt16)extraFieldSize);
			bw.Write((UInt16)commentLength);
			bw.Write((UInt16)diskNumberStart);
			bw.Write((UInt16)internalFileAttributes);
			bw.Write((UInt32)externalFileAttributes);
			bw.Write((UInt32)localHeaderOffset);

			bw.Write(filenameBytes);
			bw.Write(extraField);
			bw.Write(comment);
		}
		public override string ToString()
		{
			return $"{localHeaderOffset}: {FileName}";
		}
	}

	public class LocalFileHeader
	{
		public const uint SIGNATURE = 0x04034b50;
		public const long SIZE = 4 + 7 * 2 + DataDescriptor.SIZE;

		public uint signature;
		public EVersion extractVersion;
		public EGPFlags flags;
		public ECompressionMethon method;
		public ushort modTime;
		public ushort modDate;
		public DataDescriptor desc;
		public ushort fileNameSize;
		public ushort extraFieldSize;

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

		public bool Check(CDRFileHeader cfh, bool hackIgnoreVersionNeeded)
		{
			return (extractVersion == cfh.extractVersion || hackIgnoreVersionNeeded)
				&& flags == cfh.flags
				&& method == cfh.method
				&& modTime == cfh.modTime
				&& modDate == cfh.modDate
				&& desc.Equals(cfh.desc)
				&& fileNameSize == cfh.fileNameSize;
		}

		private void Parse(BinaryReader br)
		{
			signature = br.ReadUInt32();
			if (signature != SIGNATURE)
				throw new FileFormatException("Unexpected signature");
			extractVersion = (EVersion)br.ReadUInt16();
			flags = (EGPFlags)br.ReadUInt16();
			method = (ECompressionMethon)br.ReadUInt16();
			modTime = br.ReadUInt16();
			modDate = br.ReadUInt16();
			desc = new DataDescriptor(br);
			fileNameSize = br.ReadUInt16();
			extraFieldSize = br.ReadUInt16();
		}
	}

	public class CDR
	{
		public List<CDRFileHeader> Entries { get; private set; }
		public Dictionary<string, CDRFileHeader> Files { get; private set; }
		public byte[] Comment { get; }
		public CDR(List<CDRFileHeader> entries, byte[] comment)
		{
			Entries = entries;
			Comment = comment;

			try
			{
				Files = entries.ToDictionary(x => x.FileName);
			}
			catch (ArgumentException ex)
			{
				throw new FileFormatException($"Source archive has multiple files with the same name", ex);
			}
		}
	}

	public class ZipReadFile
	{
		private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

		public CDR CDR { get; }
		public long CDROffset { get; }
		public Stream Stream { get; }

		public static async Task<ZipReadFile> OpenAsync(Stream stream)
		{
			CDRLoadResult cdr = await LoadCDR(stream);
			return new ZipReadFile(stream, cdr.cdr, cdr.cdrOffset);
		}

		public ZipReadFile(Stream stream)
		{
			Stream = stream;
			CDRLoadResult cdr = LoadCDR(stream).Result;
			CDR = cdr.cdr;
			CDROffset = cdr.cdrOffset;
		}

		private ZipReadFile(Stream stream, CDR cdr, long cdrOffset)
		{
			Stream = stream;
			CDR = cdr;
			CDROffset = cdrOffset;
		}

		class CDRLoadResult
		{
			public CDR cdr;
			public long cdrOffset;
		}
		static async Task<CDRLoadResult> LoadCDR(Stream bs)
		{
			bool debugLog = false;


			long fileSize = bs.Length;
			if (debugLog) logger.Info($"File size {fileSize}");

			CDREndResult cdrEndResult = await FindCDREnd(bs);
			CDREnd fileCDREnd = cdrEndResult.cdrEnd;
			long commentStart = cdrEndResult.commentStart;
			byte[] comment = cdrEndResult.comment;
			if (debugLog)
			{
				logger.Info("CDREnd {0:x} - {1:x} ({2:x})", commentStart - CDREnd.SIZE, commentStart, CDREnd.SIZE);
				logger.Info("  .signature          {0:x}", fileCDREnd.signature);
				logger.Info("  .diskNumber         {0:x}", fileCDREnd.diskNumber);
				logger.Info("  .diskStart          {0:x}", fileCDREnd.diskStart);
				logger.Info("  .numEntriesThisDisk {0:x}", fileCDREnd.numEntriesThisDisk);
				logger.Info("  .numEntriesTotal    {0:x}", fileCDREnd.numEntriesTotal);
				logger.Info("  .cdrSize            {0:x}", fileCDREnd.cdrSize);
				logger.Info("  .cdrOffset          {0:x}", fileCDREnd.cdrOffset);
				logger.Info("  .commentSize        {0:x}", fileCDREnd.commentSize);
			}

			List<CDRFileHeader> cdr = new List<CDRFileHeader>(fileCDREnd.numEntriesTotal);

			if (fileCDREnd.cdrSize > 0)
			{
				bs.Seek(fileCDREnd.cdrOffset, SeekOrigin.Begin);
				long cdrPos = bs.Position;
				long cdrPosEnd = bs.Position + fileCDREnd.cdrSize;

				if (debugLog) logger.Info("CDR {0:x} - {1:x} ({2:x})", cdrPos, cdrPosEnd, cdrPosEnd - cdrPos);

				byte[] buffer = await StreamUtil.ReadBufferAsync(bs, null, (int)fileCDREnd.cdrSize);
				using (MemoryStream ms = new MemoryStream(buffer))
				using (BinaryReader br = new BinaryReader(ms, Encoding.ASCII, true))
				{
					for (ushort fileIndex = 0; fileIndex < fileCDREnd.numEntriesTotal; fileIndex++)
					{
						long currentCDRPos = bs.Position;
						CDRFileHeader pfileCDRRecord = new CDRFileHeader(br);

						cdr.Add(pfileCDRRecord);

						if (debugLog)
						{
							logger.Info("  CDRFileHeader[{0:X4}] {1:x} - {2:x} ({3:x})", fileIndex, currentCDRPos, currentCDRPos + pfileCDRRecord.CdrRecordSize, pfileCDRRecord.CdrRecordSize);
							logger.Info("   .signature =              {0:x}", pfileCDRRecord.signature);
							logger.Info("   .createVersion =          {0}", pfileCDRRecord.createVersion);
							logger.Info("   .extractVersion =         {0}", pfileCDRRecord.extractVersion);
							logger.Info("   .flags =                  {0}", pfileCDRRecord.flags);
							logger.Info("   .method =                 {0}", pfileCDRRecord.method);
							logger.Info("   .modTime =                {0:x}   {1}", pfileCDRRecord.modTime, pfileCDRRecord.FormatLastModTime());
							logger.Info("   .modDate =                {0:x}   {1}", pfileCDRRecord.modDate, pfileCDRRecord.FormatLastModDate());
							logger.Info("   .desc.crc32 =             {0:x}", pfileCDRRecord.desc.crc32);
							logger.Info("   .desc.sizeCompressed =    {0:x}", pfileCDRRecord.desc.sizeCompressed);
							logger.Info("   .desc.sizeUncompressed =  {0:x}", pfileCDRRecord.desc.sizeUncompressed);
							logger.Info("   .fileNameSize =           {0:x}", pfileCDRRecord.fileNameSize);
							logger.Info("   .extraFieldSize =         {0:x}", pfileCDRRecord.extraFieldSize);
							logger.Info("   .commentLength =          {0:x}", pfileCDRRecord.commentLength);
							logger.Info("   .diskNumberStart =        {0:x}", pfileCDRRecord.diskNumberStart);
							logger.Info("   .internalFileAttributes = {0:x}", pfileCDRRecord.internalFileAttributes);
							logger.Info("   .externalFileAttributes = {0:x}", pfileCDRRecord.externalFileAttributes);
							logger.Info("   .localHeaderOffset =      {0:x}", pfileCDRRecord.localHeaderOffset);
							logger.Info("   .fileName: {0}", pfileCDRRecord.FileName);
						}
					}
				}
			}


			return new CDRLoadResult()
			{
				cdr = new CDR(cdr, comment),
				cdrOffset = fileCDREnd.cdrOffset
			};
		}

		class CDREndResult
		{
			public CDREnd cdrEnd;
			public long commentStart;
			public byte[] comment;
		}
		static async Task<CDREndResult> FindCDREnd(Stream stream/*, out long commentStart, out byte[] comment*/)
		{
			stream.Seek(0, SeekOrigin.End);
			long fileSize = stream.Position;
			if (fileSize < CDREnd.SIZE)
			{
				throw new FileFormatException("File isn't big enough to contain a CDREnd structure");
			}

			CDREndResult result = new CDREndResult();
			//CDREnd cdrEnd = null;

			// Check to see if this is ZIP file with no archive comment (the
			// "end of central directory" structure should be the last item in the
			// file if this is the case).
			stream.Seek(-CDREnd.SIZE, SeekOrigin.End);
			using (BinaryReader br = new BinaryReader(stream, Encoding.ASCII, true))
			{
				result.cdrEnd = CheckCDREnd(br, 0, out result.commentStart);
				if (result.cdrEnd != null)
				{
					result.comment = null;
					return result;
				}
			}

			// Either this is not a ZIP file, or it is a ZIP file with an archive
			// comment.  Search the end of the file for the "end of central directory"
			// record signature. The comment is the last item in the ZIP file and may be
			// up to 64K long.  It is assumed that the "end of central directory" magic
			// number does not appear in the comment.

			long maxCdrEndOffset = Math.Max(fileSize - (1 << 16) - CDREnd.SIZE, 0);
			stream.Seek(maxCdrEndOffset, SeekOrigin.Begin);

			//         byte[] buffer = new byte[fileSize - maxCdrEndOffset];

			//if (await stream.ReadAsync(buffer, 0, buffer.Length) != buffer.Length)
			//{
			//	throw new FileFormatException("Unable to read possible CDREnd buffer with comment");
			//}

			byte[] buffer = await StreamUtil.ReadBufferAsync(stream, null, (int)(fileSize - maxCdrEndOffset));

			return SearchBuffer(buffer, result, maxCdrEndOffset);
			static CDREndResult SearchBuffer(byte[] buffer, CDREndResult result, long maxCdrEndOffset)
			{
				// Since comment size has to be > 0, search span can be reduced
				Span<byte> bufSpan = buffer.AsSpan().Slice(0, buffer.Length - (int)CDREnd.SIZE);
				if (bufSpan.Length > 0)
				{
					int pos = bufSpan.LastIndexOf(CDREnd.SignatureBytes);
					if (pos != -1)
					{
						int sizeLeft = buffer.Length - pos;

						using (MemoryStream ms = new MemoryStream(buffer, pos, sizeLeft))
						using (BinaryReader br = new BinaryReader(ms))
						{
							int commentSize = sizeLeft - (int)CDREnd.SIZE;
							Debug.Assert(commentSize > 0);

							ushort expectedCommentSize = (ushort)commentSize;
							result.cdrEnd = CheckCDREnd(br, expectedCommentSize, out result.commentStart);
							if (result.cdrEnd != null)
							{
								result.commentStart += pos;
								result.comment = buffer.AsSpan((int)result.commentStart).ToArray();
								result.commentStart += maxCdrEndOffset;
								return result;
							}
						}
					}
				}
				throw new FileFormatException("Couldn't find a CDREnd structure");
			}
		}

		static CDREnd CheckCDREnd(BinaryReader br, ushort expectedCommentSize, out long commentStart)
		{
			commentStart = 0;
			uint signature = br.ReadUInt32();
			if (signature == CDREnd.SIGNATURE)
			{
				CDREnd cdrEnd = new CDREnd(br, signature);
				if (cdrEnd.commentSize == expectedCommentSize)
				{
					commentStart = br.BaseStream.Position;
					return cdrEnd;
				}
			}
			return null;
		}
	}
}
